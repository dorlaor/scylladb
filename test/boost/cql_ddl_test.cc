/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include <boost/test/unit_test.hpp>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include "test/lib/cql_test_env.hh"
#include "sstables/sstables.hh"
#include "test/lib/cql_assertions.hh"

BOOST_AUTO_TEST_SUITE(cql_ddl_test)

/// Check that writes interact with caching = {'enabled': X} as expected:
/// * enable: true -> data is merged into cache on memtable flush [1]
/// * enable: false -> data is not merged into cache on memtable flush
///
/// [1] Important: only partitions which are either already in the cache,
///     or are not present in underlying (disk) are merged.
future<> writes_with_caching_toggle(bool enabled) {
    return do_with_cql_env_thread([enabled] (cql_test_env& e) {
        e.execute_cql(format("CREATE TABLE ks.tbl (pk int PRIMARY KEY, v text) WITH CACHING = {{'enabled': '{}'}}", enabled)).get();

        const auto& table = e.local_db().find_column_family("ks", "tbl");
        const auto table_id = table.schema()->id();

        auto write_rows = [&, first_pk = 0] () mutable {
            sstring value(128, 'v');
            const auto cql3_value = cql3::raw_value::make_value(serialized(value));

            auto id = e.prepare("INSERT INTO ks.tbl (pk, v) VALUES (?, ?);").get();
            for (int flushes = 0; flushes < 5; flushes++) {
                for (int32_t pk = first_pk; pk < first_pk + 10; ++pk) {
                    const auto cql3_pk = cql3::raw_value::make_value(serialized(pk));
                    e.execute_prepared(id, {cql3_pk, cql3_value}).get();
                }
                replica::database::flush_table_on_all_shards(e.db(), table_id).get();
            }
            first_pk += 10;
        };

        auto check_expected_cache_content = [&] (bool cache_enabled) {
            const auto get_cache_shards_with_content = e.db().map_reduce0([] (const replica::database& db) {
                auto& t = db.find_column_family("ks", "tbl");
                return uint64_t(!t.get_row_cache().empty());
            }, uint64_t(0), std::plus<uint64_t>()).get();

            if (cache_enabled) {
                BOOST_REQUIRE_GT(get_cache_shards_with_content, 0);
            } else {
                BOOST_REQUIRE_EQUAL(get_cache_shards_with_content, 0);
            }
        };

        write_rows();
        check_expected_cache_content(enabled);

        replica::database::drop_cache_for_table_on_all_shards(e.db(), table_id).get();
        e.execute_cql(format("ALTER TABLE ks.tbl WITH CACHING = {{'enabled': '{}'}}", !enabled)).get();

        write_rows();
        check_expected_cache_content(!enabled);
    });
}

SEASTAR_TEST_CASE(test_writes_with_caching_disabled) {
    return writes_with_caching_toggle(false);
}

SEASTAR_TEST_CASE(test_writes_with_caching_enabled) {
    return writes_with_caching_toggle(true);
}

// The `parquet = {...}` table property, end to end through CQL.
//
// parquet_parameters has its own unit test for parsing and validation. What that cannot
// cover is the part that actually broke first: whether the property survives being
// *stored*. to_map() originally emitted the internal "L0" while the parser accepted
// "verbatim", so writing the property and reading it back failed its own validation --
// invisible to any test that only parses. This drives CREATE, ALTER and a schema reload.
SEASTAR_TEST_CASE(test_parquet_table_property) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.pqt (pk int PRIMARY KEY, v int) "
                      "WITH parquet = {'row_group_rows': '5000'}").get();
        {
            auto s = e.local_db().find_schema("ks", "pqt");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("row_group_rows"), "5000");
        }

        // ALTER replaces the map, and a folding level exercises the round trip that broke.
        e.execute_cql("ALTER TABLE ks.pqt WITH parquet = "
                      "{'row_group_rows': '20000', 'metadata_folding': 'verbatim'}").get();
        {
            auto s = e.local_db().find_schema("ks", "pqt");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("row_group_rows"), "20000");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("metadata_folding"), "verbatim");
        }

        // Bad values must be configuration errors at DDL time, not surprises at write
        // time. Each of these is rejected for a different reason: unknown sub-option,
        // below the row-group floor where per-row-group metadata dominates, a codec the
        // writer cannot emit, and the export-only folding level that would discard write
        // times and TTLs.
        for (const char* bad : {
                "{'row_groop_rows': '5000'}",
                "{'row_group_rows': '100'}",
                "{'compression': 'gzip'}",
                "{'metadata_folding': 'logical'}"}) {
            BOOST_REQUIRE_THROW(
                    e.execute_cql(seastar::format(
                            "ALTER TABLE ks.pqt WITH parquet = {}", bad)).get(),
                    exceptions::configuration_exception);
        }

        // The rejected ALTERs must not have changed anything.
        auto s = e.local_db().find_schema("ks", "pqt");
        BOOST_REQUIRE_EQUAL(s->parquet_options().at("row_group_rows"), "20000");
    });
}

// The per-column `encoding.<col>` sub-option, end to end through CQL.
//
// The interesting cases are the rejections. An encoding that does not apply to a column's type
// cannot be honoured, and there is no good way to fail later: silently ignoring it is a setting
// that lies, and failing at write time takes the table down long after the DDL was accepted. So
// both a wrong type and a misspelled column name have to be configuration errors here.
//
// `auto` is checked too, because it is the only way to *undo* an override -- an ALTER replaces
// the whole map, but an operator scripting a change wants to name the column and cancel it.
SEASTAR_TEST_CASE(test_parquet_per_column_encoding_property) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.pqe (pk int, ck text, v text, w double, "
                      "PRIMARY KEY ((pk), ck)) WITH parquet = "
                      "{'encoding.v': 'delta_byte_array', 'encoding.w': 'byte_stream_split'}")
                .get();
        {
            auto s = e.local_db().find_schema("ks", "pqe");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("encoding.v"), "delta_byte_array");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("encoding.w"), "byte_stream_split");
        }

        // Every rejection is for a different reason: an encoding that is not a member of the
        // enum, two that do not apply to the column's type, a column the table does not have,
        // and a missing column name.
        for (const char* bad : {
                "{'encoding.v': 'delta_magic'}",
                "{'encoding.v': 'delta_binary_packed'}",
                "{'encoding.v': 'byte_stream_split'}",
                "{'encoding.nosuch': 'plain'}",
                "{'encoding.': 'plain'}"}) {
            BOOST_REQUIRE_THROW(
                    e.execute_cql(seastar::format(
                            "ALTER TABLE ks.pqe WITH parquet = {}", bad)).get(),
                    exceptions::configuration_exception);
        }

        // The rejected ALTERs must have changed nothing.
        {
            auto s = e.local_db().find_schema("ks", "pqe");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("encoding.v"), "delta_byte_array");
        }

        // A clustering key is a legitimate target, and delta_binary_packed applies to an int.
        e.execute_cql("ALTER TABLE ks.pqe WITH parquet = {'encoding.ck': 'delta_byte_array'}")
                .get();
        {
            auto s = e.local_db().find_schema("ks", "pqe");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("encoding.ck"), "delta_byte_array");
            // The replaced map must not have kept the old entries.
            BOOST_REQUIRE(!s->parquet_options().contains("encoding.v"));
        }

        // `auto` is accepted and stored, so DESCRIBE keeps showing an explicit cancellation
        // rather than the setting vanishing from the schema.
        e.execute_cql("ALTER TABLE ks.pqe WITH parquet = {'encoding.ck': 'auto'}").get();
        {
            auto s = e.local_db().find_schema("ks", "pqe");
            BOOST_REQUIRE_EQUAL(s->parquet_options().at("encoding.ck"), "auto");
        }
    });
}

// storage_format actually converts on compaction, in both directions.
//
// The property has been parsed, validated and persisted for a while, but nothing acted on
// it: compaction was format-preserving, so `ALTER TABLE ... WITH storage_format =
// 'parquet'` recorded an intent that never happened. This drives the round trip that
// matters -- native to Parquet and back -- because converting *back* is the direction
// nobody thinks to check, and a table that cannot be un-converted is a trap.
SEASTAR_TEST_CASE(test_storage_format_converts_on_compaction) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.conv (pk int PRIMARY KEY, v int)").get();
        auto& db = e.local_db();
        auto insert_and_flush = [&] (int base) {
            for (int i = 0; i < 20; ++i) {
                e.execute_cql(seastar::format(
                        "INSERT INTO ks.conv (pk, v) VALUES ({}, {})", base + i, i)).get();
            }
            e.db().invoke_on_all([] (replica::database& d) {
                return d.flush_all_memtables();
            }).get();
        };
        auto versions = [&] {
            std::set<sstables::sstable_version_types> out;
            auto& t = db.find_column_family("ks", "conv");
            for (auto&& sst : *t.get_sstables()) { out.insert(sst->get_version()); }
            return out;
        };

        insert_and_flush(0);
        // Flushes are never Parquet: the creator above only affects compaction outputs.
        BOOST_REQUIRE(!versions().contains(sstables::sstable_version_types::pq));

        e.execute_cql("ALTER TABLE ks.conv WITH storage_format = 'parquet'").get();
        insert_and_flush(100);
        db.find_column_family("ks", "conv").compact_all_sstables(tasks::task_info{}).get();
        BOOST_REQUIRE(versions() == std::set<sstables::sstable_version_types>{
                sstables::sstable_version_types::pq});

        // The data has to survive the conversion, not merely change format: read every
        // key back rather than trusting the format switch.
        for (int i = 0; i < 20; ++i) {
            assert_that(e.execute_cql(seastar::format(
                    "SELECT v FROM ks.conv WHERE pk = {}", 100 + i)).get())
                    .is_rows().with_size(1);
        }

        // And back again.
        e.execute_cql("ALTER TABLE ks.conv WITH storage_format = 'sstable'").get();
        insert_and_flush(200);
        db.find_column_family("ks", "conv").compact_all_sstables(tasks::task_info{}).get();
        BOOST_REQUIRE(!versions().contains(sstables::sstable_version_types::pq));
    });
}

// Every write path that creates an sstable *without* going through compaction has to honour
// storage_format, and four of them did not. Streaming, reshape, reshard and split all defaulted
// to the node's preferred native version, so a table declared 'parquet' silently accumulated
// native sstables. All four were found by grepping for creator assignments; none was caught by a
// test, which is what this case is for.
//
// The streaming creator is the one reachable from a single-node cql_test_env, and it is also the
// one an operator hits most often -- repair, bootstrap and `nodetool refresh` in load-and-stream
// mode all go through it.
SEASTAR_TEST_CASE(test_storage_format_honoured_by_streaming_writes) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("CREATE TABLE ks.strm (pk int PRIMARY KEY, v int) "
                      "WITH storage_format = 'parquet'").get();
        auto& t = e.local_db().find_column_family("ks", "strm");

        // The creator the streaming path uses, asked directly. Going through an actual repair
        // needs a second node; the contract being asserted is the creator's, and calling it is
        // the honest unit of that.
        auto sst = t.make_streaming_sstable_for_write();
        BOOST_REQUIRE(sst->get_version() == sstables::sstable_version_types::pq);

        auto staging = t.make_streaming_staging_sstable();
        BOOST_REQUIRE(staging->get_version() == sstables::sstable_version_types::pq);
        // Staging is about view building, and must not be lost to the format change.
        BOOST_REQUIRE(staging->state() == sstables::sstable_state::staging);

        // A table that has not opted in must be untouched: the fix honours an explicit
        // 'parquet' only, and 'hybrid' deliberately streams native because streamed data has
        // just arrived and is not bottom-tier.
        e.execute_cql("CREATE TABLE ks.strm_plain (pk int PRIMARY KEY, v int)").get();
        auto& p = e.local_db().find_column_family("ks", "strm_plain");
        BOOST_REQUIRE(p.make_streaming_sstable_for_write()->get_version()
                      != sstables::sstable_version_types::pq);

        e.execute_cql("CREATE TABLE ks.strm_hybrid (pk int PRIMARY KEY, v int) "
                      "WITH storage_format = 'hybrid'").get();
        auto& h = e.local_db().find_column_family("ks", "strm_hybrid");
        BOOST_REQUIRE(h.make_streaming_sstable_for_write()->get_version()
                      != sstables::sstable_version_types::pq);
    });
}

BOOST_AUTO_TEST_SUITE_END()
