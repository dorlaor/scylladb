/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// End-to-end tests for the `lc` sstable format: mutations in through the
// normal sstable_writer, a real Lance 2.1 file on disk, and the same
// mutations back out through sstable::make_reader.
//
// The standalone suites under sstables/lance/ cover the format codec
// (including bidirectional conformance against pylance); this file covers the
// sstable-layer dispatch, the components, and the round trip through the real
// read path. Scope note: lc v1 rejects non-frozen collections and counters at
// DDL time (docs/dev/lance-storage-format.md 4), so unlike the pq twin of
// this file there are no collection cases -- there is instead a case pinning
// the writer-side backstop for them.

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/reader_concurrency_semaphore.hh"

#include "schema/schema_builder.hh"
#include "readers/from_mutations.hh"
#include "sstables/sstables.hh"
#include "partition_slice_builder.hh"
#include "mutation/mutation.hh"
#include "types/map.hh"
#include "mutation/collection_mutation.hh"
#include "utils/UUID_gen.hh"

#include <cstdio>
#include <cstdlib>
#include <filesystem>

using namespace sstables;

namespace {

schema_ptr lc_schema() {
    return schema_builder(1, "ks", "lc_tbl")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("v_int", int32_type)
        .with_column("v_big", long_type)
        .with_column("v_dbl", double_type)
        .with_column("v_txt", utf8_type)
        .build();
}

utils::chunked_vector<mutation> make_muts(schema_ptr s, int n_part, int n_rows) {
    utils::chunked_vector<mutation> muts;
    for (int p = 0; p < n_part; ++p) {
        auto pk = partition_key::from_single_value(
                *s, utf8_type->decompose(sstring(format("key{:04d}", p))));
        mutation m(s, pk);
        for (int r = 0; r < n_rows; ++r) {
            auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
            const api::timestamp_type row_ts = 1000 + p;
            auto put = [&] (const char* name, bytes val, api::timestamp_type ts) {
                m.set_clustered_cell(ck, *s->get_column_definition(to_bytes(name)),
                                     atomic_cell::make_live(*s->get_column_definition(
                                             to_bytes(name))->type, ts, val));
            };
            put("v_int", int32_type->decompose(r * 7), row_ts);
            if (r % 3) { put("v_big", long_type->decompose(int64_t(r) * 1'000'003), row_ts); }
            put("v_dbl", double_type->decompose(double(r) * 1.5), row_ts);
            if (r % 5 == 0) {
                put("v_txt", utf8_type->decompose(sstring(format("v{}", r))), row_ts + 1);
            } else {
                put("v_txt", utf8_type->decompose(sstring(format("v{}", r))), row_ts);
            }
        }
        muts.push_back(std::move(m));
    }
    std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
        return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
    });
    return muts;
}

utils::chunked_vector<mutation> read_all(shared_sstable sst, schema_ptr s,
                                         reader_permit permit) {
    auto rd = sst->make_reader(s, permit, query::full_partition_range,
                               s->full_slice());
    auto close = deferred_close(rd);
    utils::chunked_vector<mutation> out;
    while (auto m = read_mutation_from_mutation_reader(rd).get()) {
        out.push_back(std::move(*m));
    }
    return out;
}

} // namespace

SEASTAR_THREAD_TEST_CASE(test_lc_sstable_is_written_and_read_back) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = lc_schema();
        auto muts = make_muts(s, 12, 40);
        auto expected = muts;

        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::lc), std::move(muts)).get();

        BOOST_REQUIRE(sst->get_version() == sstable_version_types::lc);

        auto got = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }
    }).get();
}

// The Data component must be a Lance file any other implementation can open;
// the trailing magic is the format's only signature. SCYLLA_LC_DUMP writes it
// out so pylance can be pointed at it (sstables/lance/run_tests.sh does the
// equivalent for the standalone writer).
SEASTAR_THREAD_TEST_CASE(test_lc_data_component_is_a_lance_file) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = lc_schema();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::lc), make_muts(s, 4, 25)).get();

        const uint64_t len = sst->ondisk_data_size();
        auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
        BOOST_REQUIRE_GE(buf.size(), 40u);
        BOOST_REQUIRE_EQUAL(std::string_view(buf.get() + buf.size() - 4, 4), "LANC");

        if (const char* dst = std::getenv("SCYLLA_LC_DUMP")) {
            auto f = std::fopen(dst, "wb");
            BOOST_REQUIRE(f);
            BOOST_REQUIRE_EQUAL(std::fwrite(buf.get(), 1, buf.size(), f), buf.size());
            std::fclose(f);
        }
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_lc_sstable_has_index_summary_and_filter) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = lc_schema();
        auto muts = make_muts(s, 30, 5);
        const size_t n_part = muts.size();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::lc), std::move(muts)).get();

        BOOST_REQUIRE(sst->has_component(component_type::Index));
        BOOST_REQUIRE(sst->has_component(component_type::Summary));
        BOOST_REQUIRE(sst->has_component(component_type::Filter));
        BOOST_REQUIRE(sst->has_component(component_type::Statistics));
        BOOST_REQUIRE(sst->has_component(component_type::TOC));

        auto full = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(full.size(), n_part);
        for (const auto& m : full) {
            auto k = key::from_partition_key(*s, m.key());
            BOOST_REQUIRE(sst->filter_has_key(k));
        }
    }).get();
}

// Single-partition reads take the indexed path: ordinal window from the
// partition index, then chunk-grained fetches -- the format's design center.
SEASTAR_THREAD_TEST_CASE(test_lc_single_partition_read) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = lc_schema();
        auto muts = make_muts(s, 16, 10);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::lc), std::move(muts)).get();

        for (const auto& want : expected) {
            auto pr = dht::partition_range::make_singular(want.decorated_key());
            auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
            auto close = deferred_close(rd);
            auto got = read_mutation_from_mutation_reader(rd).get();
            BOOST_REQUIRE(got);
            assert_that(*got).is_equal_to(want);
            BOOST_REQUIRE(!read_mutation_from_mutation_reader(rd).get());
        }
    }).get();
}

// Deletions, TTLs, statics and range tombstones: all of it is scalar channels
// after folding, so it must survive the lance container unchanged.
SEASTAR_THREAD_TEST_CASE(test_lc_tombstones_statics_and_rtc_round_trip) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = schema_builder(1, "ks", "lc_tomb")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("st", utf8_type, column_kind::static_column)
            .with_column("v", utf8_type)
            .build();

        utils::chunked_vector<mutation> muts;
        const gc_clock::time_point now = gc_clock::now();

        {   // p0: static row + live and dead cells + a row tombstone
            auto pk = partition_key::from_single_value(*s, utf8_type->decompose(sstring("p0")));
            mutation m(s, pk);
            m.set_static_cell(*s->get_column_definition(to_bytes("st")),
                    atomic_cell::make_live(*utf8_type, 7, utf8_type->decompose(sstring("stat"))));
            auto ck0 = clustering_key::from_single_value(*s, int32_type->decompose(0));
            m.set_clustered_cell(ck0, *s->get_column_definition(to_bytes("v")),
                    atomic_cell::make_live(*utf8_type, 10, utf8_type->decompose(sstring("live"))));
            auto ck1 = clustering_key::from_single_value(*s, int32_type->decompose(1));
            m.set_clustered_cell(ck1, *s->get_column_definition(to_bytes("v")),
                    atomic_cell::make_dead(11, now));
            auto ck2 = clustering_key::from_single_value(*s, int32_type->decompose(2));
            m.partition().apply_delete(*s, ck2, tombstone(12, now));
            muts.push_back(std::move(m));
        }
        {   // p1: a range tombstone and an expiring cell
            auto pk = partition_key::from_single_value(*s, utf8_type->decompose(sstring("p1")));
            mutation m(s, pk);
            auto ck_lo = clustering_key_prefix::from_single_value(*s, int32_type->decompose(10));
            auto ck_hi = clustering_key_prefix::from_single_value(*s, int32_type->decompose(20));
            m.partition().apply_delete(*s,
                    range_tombstone(bound_view(ck_lo, bound_kind::incl_start),
                                    bound_view(ck_hi, bound_kind::incl_end),
                                    tombstone(15, now)));
            auto ck = clustering_key::from_single_value(*s, int32_type->decompose(30));
            m.set_clustered_cell(ck, *s->get_column_definition(to_bytes("v")),
                    atomic_cell::make_live(*utf8_type, 16, utf8_type->decompose(sstring("ttl")),
                                           now + std::chrono::seconds(3600),
                                           std::chrono::seconds(3600)));
            muts.push_back(std::move(m));
        }
        {   // p2: partition tombstone only
            auto pk = partition_key::from_single_value(*s, utf8_type->decompose(sstring("p2")));
            mutation m(s, pk);
            m.partition().apply(tombstone(20, now));
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });
        auto expected = muts;

        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::lc), std::move(muts)).get();
        auto got = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }
    }).get();
}

// Enough rows that every column spills into multiple pages, so page_for_row,
// cross-page windows and the miniblock chunk lookups are all on the read path.
SEASTAR_THREAD_TEST_CASE(test_lc_multi_page_round_trip) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = lc_schema();
        auto muts = make_muts(s, 40, 500);   // 20 000 rows
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::lc), std::move(muts)).get();

        auto got = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }

        // And a few point reads on the same file, through the index.
        for (size_t i : {size_t(0), expected.size() / 2, expected.size() - 1}) {
            auto pr = dht::partition_range::make_singular(expected[i].decorated_key());
            auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
            auto close = deferred_close(rd);
            auto got_one = read_mutation_from_mutation_reader(rd).get();
            BOOST_REQUIRE(got_one);
            assert_that(*got_one).is_equal_to(expected[i]);
        }
    }).get();
}

// The writer-side backstop for the v1 scope cut: a non-frozen collection
// reaches the writer only if the DDL guard was bypassed (this test constructs
// the schema directly), and must fail the write loudly rather than corrupt.
SEASTAR_THREAD_TEST_CASE(test_lc_writer_refuses_non_frozen_collections) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto map_type = map_type_impl::get_instance(utf8_type, utf8_type, true);
        auto s = schema_builder(1, "ks", "lc_coll")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("m", map_type)
            .build();

        auto pk = partition_key::from_single_value(*s, utf8_type->decompose(sstring("p")));
        mutation m(s, pk);
        collection_mutation_writer w{tombstone{}};
        auto k = utf8_type->decompose(sstring("k"));
        auto kb = managed_bytes(reinterpret_cast<const int8_t*>(k.data()), k.size());
        w.push_back(managed_bytes_view(kb),
                atomic_cell::make_live(*utf8_type, 1, utf8_type->decompose(sstring("v"))));
        m.set_clustered_cell(clustering_key::make_empty(),
                *s->get_column_definition(to_bytes("m")),
                atomic_cell_or_collection(std::move(w).finish()));

        utils::chunked_vector<mutation> muts;
        muts.push_back(std::move(m));
        BOOST_REQUIRE_THROW(
                make_sstable_containing(env.make_sstable(s, sstable_version_types::lc),
                                        std::move(muts)).get(),
                std::invalid_argument);
    }).get();
}
