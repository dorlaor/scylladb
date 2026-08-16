/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// End-to-end tests for the `pq` sstable format: mutations in through the normal
// sstable_writer, a real Parquet file on disk, and the same mutations back out
// through sstable::make_reader.
//
// The suites under sstables/parquet/ cover the format codec and the shredder in
// isolation and without Seastar. This file covers what those cannot: that the
// sstable layer dispatches to the pq writer and reader at all, that the
// components a loadable sstable needs are written, and that the round trip
// survives the real read path rather than a test harness.
//
// Deliberately limited to what the shredder currently handles: no static rows,
// no partition or range tombstones, no collections, no counters, no row
// markers. Those are tracked in docs/dev/parquet-storage-format.md section 11,
// and are the reason `pq` is not yet in all_sstable_versions.

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/reader_concurrency_semaphore.hh"

#include "schema/schema_builder.hh"
#include "readers/from_mutations.hh"
#include "sstables/sstables.hh"
#include "mutation/mutation.hh"

#include <cstdio>
#include <filesystem>
#include <cstdlib>

using namespace sstables;

namespace {

// A schema made only of types the shredder maps to native Parquet columns, so a
// failure here is a failure of the pq path rather than of the blob fallback.
schema_ptr pq_schema() {
    return schema_builder(1, "ks", "pq_tbl")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("v_int", int32_type)
        .with_column("v_big", long_type)
        .with_column("v_dbl", double_type)
        .with_column("v_txt", utf8_type)
        .build();
}

// `n_part` partitions of `n_rows` rows each, with a scattering of absent values
// so definition levels are exercised, and per-cell timestamps that mostly agree
// (which is what L1 row-folding is built for) but sometimes do not.
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
            // Every fifth row disagrees with its row timestamp in one column,
            // which is exactly what the sparse exception channel encodes.
            if (r % 5 == 0) {
                put("v_txt", utf8_type->decompose(sstring(format("v{}", r))), row_ts + 1);
            } else {
                put("v_txt", utf8_type->decompose(sstring(format("v{}", r))), row_ts);
            }
        }
        muts.push_back(std::move(m));
    }
    // The writer requires token order, which is not key order.
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

SEASTAR_THREAD_TEST_CASE(test_pq_sstable_is_written_and_read_back) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 12, 40);
        auto expected = muts;

        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        BOOST_REQUIRE(sst->get_version() == sstable_version_types::pq);

        auto got = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }
    }).get();
}

// The Data component must be a Parquet file any other implementation can open:
// the whole premise of the format is that it is not a Scylla-private container.
SEASTAR_THREAD_TEST_CASE(test_pq_data_component_is_a_parquet_file) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), make_muts(s, 4, 25)).get();

        const uint64_t len = sst->ondisk_data_size();
        auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
        BOOST_REQUIRE_GE(buf.size(), 12u);
        BOOST_REQUIRE_EQUAL(std::string_view(buf.get(), 4), "PAR1");
        BOOST_REQUIRE_EQUAL(std::string_view(buf.get() + buf.size() - 4, 4), "PAR1");

        // Magic bytes only prove the envelope. Setting SCYLLA_PQ_DUMP writes the
        // component out so an external Parquet implementation can be pointed at
        // it; sstables/parquet/run_tests.sh does exactly that with pyarrow.
        if (const char* dst = std::getenv("SCYLLA_PQ_DUMP")) {
            auto f = std::fopen(dst, "wb");
            BOOST_REQUIRE(f);
            BOOST_REQUIRE_EQUAL(std::fwrite(buf.get(), 1, buf.size(), f), buf.size());
            std::fclose(f);
            auto idx = seastar::format("{}", sst->index_filename());
            std::filesystem::copy_file(std::filesystem::path(idx.c_str()),
                                       std::filesystem::path(std::string(dst) + ".index"),
                                       std::filesystem::copy_options::overwrite_existing);
        }
    }).get();
}

// The index, summary and filter are what make an sstable loadable at all. A pq
// sstable that reads back correctly but has no partition index would still be
// useless to everything above the sstable layer.
SEASTAR_THREAD_TEST_CASE(test_pq_sstable_has_index_summary_and_filter) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 30, 5);
        const size_t n_part = muts.size();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        BOOST_REQUIRE(sst->has_component(component_type::Index));
        BOOST_REQUIRE(sst->has_component(component_type::Summary));
        BOOST_REQUIRE(sst->has_component(component_type::Filter));
        BOOST_REQUIRE(sst->has_component(component_type::Statistics));
        BOOST_REQUIRE(sst->has_component(component_type::TOC));

        // Every key written must be found by the filter. A filter that says no
        // to a key that is present is a lost read, not a false positive.
        auto full = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(full.size(), n_part);
        for (const auto& m : full) {
            auto k = key::from_partition_key(*s, m.key());
            BOOST_REQUIRE(sst->filter_has_key(k));
        }

        BOOST_REQUIRE(sst->get_first_decorated_key().less_compare(
                *s, sst->get_last_decorated_key()));
    }).get();
}

// Single-partition reads go down a different path than a full scan: they use
// the partition range to seek rather than streaming everything.
SEASTAR_THREAD_TEST_CASE(test_pq_single_partition_read) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 16, 10);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

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

// A full scan is what compaction and scrub use, and it goes through a separate
// entry point that had to be taught about pq independently of make_reader.
SEASTAR_THREAD_TEST_CASE(test_pq_full_scan_reader) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 10, 12);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto rd = sst->make_full_scan_reader(s, env.make_reader_permit(), nullptr,
                                             default_read_monitor());
        auto close = deferred_close(rd);
        size_t n = 0;
        while (auto m = read_mutation_from_mutation_reader(rd).get()) {
            assert_that(*m).is_equal_to(expected[n]);
            ++n;
        }
        BOOST_REQUIRE_EQUAL(n, expected.size());
    }).get();
}
