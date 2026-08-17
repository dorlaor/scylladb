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
#include "readers/combined.hh"
#include "readers/mutation_fragment_v1_stream.hh"
#include "sstables/sstables.hh"
#include "partition_slice_builder.hh"
#include "mutation/mutation.hh"
#include "mutation/collection_mutation.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "types/list.hh"
#include "utils/UUID_gen.hh"

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

// The raw fragment stream, printed. Reassembling into mutations normalises
// range-tombstone bounds, which hides a lost bound weight; the fragments do not.
std::vector<sstring> fragments_of(shared_sstable sst, schema_ptr s, reader_permit permit) {
    auto rd = sst->make_reader(s, permit, query::full_partition_range, s->full_slice());
    auto close = deferred_close(rd);
    std::vector<sstring> out;
    while (auto mf = rd().get()) {
        out.push_back(seastar::format("{}", mutation_fragment_v2::printer(*s, *mf)));
    }
    return out;
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

// Row markers and tombstones through the real sstable path. Each of these was a
// silent-data-loss bug before: a lost marker deletes a row that exists, and a
// lost tombstone resurrects one that does not.
SEASTAR_THREAD_TEST_CASE(test_pq_markers_and_tombstones_round_trip) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        utils::chunked_vector<mutation> muts;

        for (int p = 0; p < 24; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:04d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 2000 + p;

            // Every fourth partition is deleted outright.
            if (p % 4 == 0) {
                m.partition().apply(tombstone(ts - 1, gc_clock::time_point(gc_clock::duration(p + 1))));
            }
            for (int r = 0; r < 6; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                auto& dr = m.partition().clustered_row(*s, ck);

                if (r % 3 == 0) {
                    // A row that exists purely because of its marker: no cells at
                    // all. This is the case that vanished entirely before.
                    dr.apply(row_marker(ts));
                } else if (r % 3 == 1) {
                    dr.apply(row_marker(ts, gc_clock::duration(3600),
                                        gc_clock::time_point(gc_clock::duration(ts + 3600))));
                    dr.cells().apply(*s->get_column_definition(to_bytes("v_int")),
                            atomic_cell::make_live(*int32_type, ts, int32_type->decompose(r)));
                } else {
                    dr.apply(row_tombstone(tombstone(ts,
                            gc_clock::time_point(gc_clock::duration(p * 10 + r)))));
                }
            }
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });
        auto expected = muts;

        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();
        auto got = read_all(sst, s, env.make_reader_permit());

        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }
    }).get();
}

// Static rows: held by the shredder and replayed onto every row of the
// partition, then split back out on read. The interesting cases are a partition
// with a static row and no clustering rows at all, which has no row to attach it
// to, and a static-only partition that is also deleted.
SEASTAR_THREAD_TEST_CASE(test_pq_static_rows_round_trip) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = schema_builder(1, "ks", "pq_static")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("st", int32_type, column_kind::static_column)
            .with_column("st2", utf8_type, column_kind::static_column)
            .with_column("v", int32_type)
            .build();

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 20; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("k{:04d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 500 + p;

            if (p % 4 != 3) {
                m.set_static_cell(*s->get_column_definition(to_bytes("st")),
                        atomic_cell::make_live(*int32_type, ts, int32_type->decompose(p)));
                m.set_static_cell(*s->get_column_definition(to_bytes("st2")),
                        atomic_cell::make_live(*utf8_type, ts,
                                utf8_type->decompose(sstring(format("s{}", p % 5)))));
            }
            // p % 4 == 1 is static-only: no clustering rows at all.
            if (p % 4 != 1) {
                for (int r = 0; r < 3; ++r) {
                    auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                    m.set_clustered_cell(ck, *s->get_column_definition(to_bytes("v")),
                            atomic_cell::make_live(*int32_type, ts, int32_type->decompose(r * 3)));
                }
            }
            // p % 4 == 3 has neither statics nor rows; skip it entirely.
            if (p % 4 != 3) {
                muts.push_back(std::move(m));
            }
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });
        auto expected = muts;

        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();
        auto got = read_all(sst, s, env.make_reader_permit());

        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }
    }).get();
}

// Range tombstones. They are fragments *between* rows, not attributes of one, so
// they are carried as marked rows that keep their place in the clustering order:
// the clustering columns hold the bound's prefix, and __rtc_len says how much of
// that prefix is real.
SEASTAR_THREAD_TEST_CASE(test_pq_range_tombstones_round_trip) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 12; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:04d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 3000 + p;
            for (int r = 0; r < 10; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                m.set_clustered_cell(ck, *s->get_column_definition(to_bytes("v_int")),
                        atomic_cell::make_live(*int32_type, ts, int32_type->decompose(r)));
            }
            // A deleted band in the middle, with the bound kinds varied so both
            // inclusive and exclusive bounds are exercised.
            auto lo = clustering_key_prefix::from_single_value(*s, int32_type->decompose(3));
            auto hi = clustering_key_prefix::from_single_value(*s, int32_type->decompose(6));
            m.partition().apply_delete(*s, range_tombstone(
                    std::move(lo), p % 2 ? bound_kind::incl_start : bound_kind::excl_start,
                    std::move(hi), p % 2 ? bound_kind::excl_end : bound_kind::incl_end,
                    tombstone(ts + 1, gc_clock::time_point(gc_clock::duration(p + 1)))));
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });
        // Compare pq against the default format rather than against the in-memory
        // mutation: the write path legitimately drops rows a newer range tombstone
        // shadows, and the question here is whether pq behaves like mc, not
        // whether either matches an unwritten mutation.
        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto want = read_all(ref, s, env.make_reader_permit());
        auto got  = read_all(sst, s, env.make_reader_permit());

        BOOST_REQUIRE_EQUAL(got.size(), want.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(want[i]);
        }

        // And the raw streams, which keep the bound weights that reassembly
        // normalises away.
        auto fw = fragments_of(ref, s, env.make_reader_permit());
        auto fg = fragments_of(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(fg.size(), fw.size());
        for (size_t i = 0; i < fg.size(); ++i) {
            BOOST_REQUIRE_EQUAL(fg[i], fw[i]);
        }
    }).get();
}

// sstable::validate() deliberately excludes pq from mx::validate -- mx walks the
// mx data format, which a Parquet Data component is not -- so it falls through
// to the generic validator, which reads through make_full_scan_reader and hence
// through the pq reader. That fall-through is easy to get wrong and silent when
// it is: a validator that cannot parse the format would either crash or report
// phantom errors.
SEASTAR_THREAD_TEST_CASE(test_pq_sstable_validates_clean) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), make_muts(s, 20, 8)).get();

        abort_source abort;
        uint64_t reported = 0;
        auto errors = sst->validate(env.make_reader_permit(), abort,
                                    [&reported] (sstring) { ++reported; },
                                    default_read_monitor()).get();
        BOOST_REQUIRE_EQUAL(errors, 0);
        BOOST_REQUIRE_EQUAL(reported, 0);
    }).get();
}

// Compaction reads through make_full_scan_reader and writes through the normal
// writer, so a pq-to-pq compaction exercises both halves at once. It is also the
// path that a hybrid LSM would use to converge a table onto Parquet.
SEASTAR_THREAD_TEST_CASE(test_pq_sstables_compact_into_one) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();

        // Two disjoint halves, so the merged output should be their union.
        auto all = make_muts(s, 24, 6);
        utils::chunked_vector<mutation> a, b;
        for (size_t i = 0; i < all.size(); ++i) {
            (i % 2 ? b : a).push_back(all[i]);
        }
        auto sst_a = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(a)).get();
        auto sst_b = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(b)).get();

        // Merge the two full-scan readers and write the result as a third pq
        // sstable, which is what compaction does.
        auto out = env.make_sstable(s, sstable_version_types::pq);
        {
            std::vector<mutation_reader> rds;
            rds.push_back(sst_a->make_full_scan_reader(s, env.make_reader_permit(), nullptr,
                                                       default_read_monitor()));
            rds.push_back(sst_b->make_full_scan_reader(s, env.make_reader_permit(), nullptr,
                                                       default_read_monitor()));
            auto merged = make_combined_reader(s, env.make_reader_permit(), std::move(rds));
            auto cfg = env.manager().configure_writer("test");
            out->write_components(std::move(merged), all.size(), s, cfg, encoding_stats{}).get();
            out->open_data().get();
        }

        auto got = read_all(out, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), all.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(all[i]);
        }
    }).get();
}

// Intra-partition forwarding. The reader itself cannot seek by clustering
// position, so make_reader() wraps it in the forwardable adapter rather than
// accepting forwarding::yes and ignoring the position range -- which is what it
// used to do, and which silently returned rows the caller had not asked for.
SEASTAR_THREAD_TEST_CASE(test_pq_forwarding_reader_honours_position_range) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 6, 10);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        const auto& want = expected[0];
        auto pr = dht::partition_range::make_singular(want.decorated_key());
        auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice(),
                                   nullptr, streamed_mutation::forwarding::yes);
        auto ck = [&] (int i) {
            return clustering_key::from_single_value(*s, int32_type->decompose(i));
        };
        assert_that(std::move(rd))
            .produces_partition_start(want.decorated_key())
            // Nothing until forwarded: that is what forwarding means.
            .produces_end_of_stream()
            .fast_forward_to(ck(3), ck(6))
            .produces_row_with_key(ck(3))
            .produces_row_with_key(ck(4))
            .produces_row_with_key(ck(5))
            .produces_end_of_stream()
            .fast_forward_to(ck(8), ck(9))
            .produces_row_with_key(ck(8))
            .produces_end_of_stream();
    }).get();
}

// A read with a clustering slice must not return rows outside it. The reader
// used to take the slice and drop it, which the mutation-source conformance
// suite catches in test_range_tombstones_v2 -- and which also left a dangling
// reference, because the reversed path builds its slice with reverse_slice()
// and a reader outlives the call that made it.
SEASTAR_THREAD_TEST_CASE(test_pq_read_honours_clustering_slice) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 8, 12);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto ck = [&] (int i) {
            return clustering_key::from_single_value(*s, int32_type->decompose(i));
        };
        auto slice = partition_slice_builder(*s)
                .with_range(query::clustering_range::make(ck(4), ck(7)))
                .build();

        const auto& want = expected[0];
        auto pr = dht::partition_range::make_singular(want.decorated_key());
        auto rd = sst->make_reader(s, env.make_reader_permit(), pr, slice);
        auto close = deferred_close(rd);
        auto got = read_mutation_from_mutation_reader(rd).get();
        BOOST_REQUIRE(got);

        sstring keys;
        for (const auto& re : got->partition().clustered_rows()) {
            keys += format("{}{}", keys.empty() ? "" : ",",
                           value_cast<int32_t>(int32_type->deserialize(
                                   re.key().explode(*s)[0])));
        }
        BOOST_REQUIRE_EQUAL(keys, sstring("4,5,6,7"));
    }).get();
}

// Non-frozen collections, through the real sstable path. They are the one thing
// in the mutation stream that needs Dremel nesting rather than another leaf, and
// the states that matter are absent, present-but-empty, populated, and deleted --
// conflating absent with empty resurrects a collection the user cleared.
SEASTAR_THREAD_TEST_CASE(test_pq_collections_round_trip) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto map_si = map_type_impl::get_instance(utf8_type, int32_type, true);
        auto set_i  = set_type_impl::get_instance(int32_type, true);
        auto s = schema_builder(1, "ks", "pq_coll")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .with_column("m", map_si)
            .with_column("t", set_i)
            .with_column("sm", map_si, column_kind::static_column)
            .build();

        const auto& mdef = *s->get_column_definition(to_bytes("m"));
        const auto& tdef = *s->get_column_definition(to_bytes("t"));
        const auto& smdef = *s->get_column_definition(to_bytes("sm"));

        auto make_map = [&] (api::timestamp_type ts, int n, bool tomb, bool dead_first) {
            collection_mutation_writer w(tomb
                    ? tombstone(ts - 1, gc_clock::time_point(gc_clock::duration(7)))
                    : tombstone());
            for (int i = 0; i < n; ++i) {
                auto k = utf8_type->decompose(sstring(format("k{}", i)));
                auto kb = managed_bytes(reinterpret_cast<const int8_t*>(k.data()), k.size());
                if (dead_first && i == 0) {
                    w.push_back(managed_bytes_view(kb), atomic_cell::make_dead(ts,
                            gc_clock::time_point(gc_clock::duration(3))));
                } else {
                    w.push_back(managed_bytes_view(kb), atomic_cell::make_live(
                            *int32_type, ts, int32_type->decompose(i * 10)));
                }
            }
            return atomic_cell_or_collection(std::move(w).finish());
        };
        auto make_set = [&] (api::timestamp_type ts, int n) {
            collection_mutation_writer w{tombstone{}};
            for (int i = 0; i < n; ++i) {
                auto k = int32_type->decompose(i);
                auto kb = managed_bytes(reinterpret_cast<const int8_t*>(k.data()), k.size());
                w.push_back(managed_bytes_view(kb),
                            atomic_cell::make_live(*bytes_type, ts, bytes_view()));
            }
            return atomic_cell_or_collection(std::move(w).finish());
        };

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 18; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:04d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 4000 + p;
            // A static collection on most partitions: it belongs to the partition,
            // so it must come back on the static row rather than on any row.
            if (p % 3 != 2) {
                m.set_static_cell(smdef, make_map(ts, 2, p % 6 == 1, false));
            }
            for (int r = 0; r < 3; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                m.set_clustered_cell(ck, *s->get_column_definition(to_bytes("v")),
                        atomic_cell::make_live(*int32_type, ts, int32_type->decompose(r)));
                const int kind = (p + r) % 5;
                if (kind == 1) {
                    m.set_clustered_cell(ck, mdef, make_map(ts, 3, false, false));
                } else if (kind == 2) {
                    m.set_clustered_cell(ck, mdef, make_map(ts, 2, true, false));
                } else if (kind == 3) {
                    m.set_clustered_cell(ck, mdef, make_map(ts, 2, false, true));
                } else if (kind == 4) {
                    m.set_clustered_cell(ck, tdef, make_set(ts, 4));
                }
                // kind == 0: neither collection present at all
            }
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });

        // Against the reference format rather than the in-memory mutation: the
        // write path may legitimately normalise a collection, and the question is
        // whether pq behaves like mc.
        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto want = read_all(ref, s, env.make_reader_permit());
        auto got  = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), want.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(want[i]);
        }
    }).get();
}

// A local stand-in for the conformance corpus's schema: two bytes clustering
// columns, and every column doubled as regular and static with a mix of scalars
// and multi-cell lists. That combination is what test_reader_conversions builds,
// and it is worth having locally because iterating on the conformance suite means
// a three-minute build for every guess.
SEASTAR_THREAD_TEST_CASE(test_pq_corpus_shaped_schema) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto list_b = list_type_impl::get_instance(bytes_type, true);
        auto sb = schema_builder(1, "ks", "pq_corpus")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("ck1", bytes_type, column_kind::clustering_key)
            .with_column("ck2", bytes_type, column_kind::clustering_key);
        // Match the corpus more closely: it is 64 + 64 columns with 33 rows and
        // range tombstones, and it was the range tombstones that this test was
        // missing.
        for (int i = 0; i < 12; ++i) {
            sb.with_column(to_bytes(format("v{}", i)),
                           i % 2 ? data_type(list_b) : bytes_type);
            sb.with_column(to_bytes(format("s{}", i)),
                           i % 2 ? data_type(list_b) : bytes_type,
                           column_kind::static_column);
        }
        auto s = sb.build();

        auto blob = [] (int n) { return bytes(bytes::initialized_later(), size_t(2 + n % 3)); };
        auto make_list = [&] (api::timestamp_type ts, int n) {
            collection_mutation_writer w{tombstone{}};
            for (int i = 0; i < n; ++i) {
                auto k = timeuuid_type->decompose(
                        utils::UUID_gen::get_time_UUID(std::chrono::system_clock::now()));
                auto kb = managed_bytes(reinterpret_cast<const int8_t*>(k.data()), k.size());
                w.push_back(managed_bytes_view(kb),
                            atomic_cell::make_live(*bytes_type, ts + i, bytes_view(blob(i))));
            }
            return atomic_cell_or_collection(std::move(w).finish());
        };

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 6; ++p) {
            auto pk = partition_key::from_single_value(*s, blob(p));
            mutation m(s, pk);
            // The corpus uses timestamps near the extremes of int64 -- values like
            // -9223372036854775737 appear in its output. The folding scheme stores
            // per-cell and row-marker timestamps as *deltas* against the row's
            // timestamp, and those subtractions overflow for spans that wide.
            static const api::timestamp_type extremes[] = {
                std::numeric_limits<api::timestamp_type>::min() + 70,
                std::numeric_limits<api::timestamp_type>::max() - 70,
                -9223372036854775737LL,
                7000,
            };
            const api::timestamp_type ts = extremes[p % 4] + p;
            for (int i = 0; i < 12; ++i) {
                const auto& cdef = *s->get_column_definition(to_bytes(format("s{}", i)));
                if (i % 2) { m.set_static_cell(cdef, make_list(ts, 2)); }
                else       { m.set_static_cell(cdef, atomic_cell::make_live(
                                     *bytes_type, ts, bytes_view(blob(i)))); }
            }
            for (int r = 0; r < 9; ++r) {
                auto ck = clustering_key::from_exploded(*s, {blob(r), blob(r + 1)});
                for (int i = 0; i < 12; ++i) {
                    const auto& cdef = *s->get_column_definition(to_bytes(format("v{}", i)));
                    if (i % 2) { m.set_clustered_cell(ck, cdef, make_list(ts, 1 + r % 2)); }
                    else       { m.set_clustered_cell(ck, cdef, atomic_cell::make_live(
                                        *bytes_type, ts, bytes_view(blob(r + i)))); }
                }
            }
            // Two range tombstones per partition, which is what the corpus has and
            // what this test was missing.
            for (int t = 0; t < 2; ++t) {
                auto lo = clustering_key_prefix::from_exploded(*s, {blob(1 + t * 3)});
                auto hi = clustering_key_prefix::from_exploded(*s, {blob(3 + t * 3)});
                m.partition().apply_delete(*s, range_tombstone(
                        std::move(lo), t ? bound_kind::incl_start : bound_kind::excl_start,
                        std::move(hi), t ? bound_kind::excl_end : bound_kind::incl_end,
                        tombstone(ts + 1 + t,
                                  gc_clock::time_point(gc_clock::duration(p + t + 1)))));
            }
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });

        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto want = read_all(ref, s, env.make_reader_permit());
        auto got  = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), want.size());
        for (size_t i = 0; i < got.size(); ++i) {
            // Row count first: an empty `rows: []` against a populated one is the
            // failure mode the conformance suite showed, and comparing whole
            // mutations buries it in 400 lines of diff.
            BOOST_REQUIRE_EQUAL(got[i].partition().clustered_rows().calculate_size(),
                                want[i].partition().clustered_rows().calculate_size());
            assert_that(got[i]).is_equal_to(want[i]);
        }

        // And through the v1 fragment stream, which is what
        // test_reader_conversions exercises: the v2->v1 conversion reassembles
        // range tombstones and is the one reader path nothing else here covers.
        auto read_v1 = [&] (shared_sstable t) {
            mutation_fragment_v1_stream st(
                    t->make_reader(s, env.make_reader_permit(),
                                   query::full_partition_range, s->full_slice()));
            auto close = deferred_close(st);
            utils::chunked_vector<mutation> out;
            while (auto m = read_mutation_from_mutation_reader(st).get()) {
                out.push_back(std::move(*m));
            }
            return out;
        };
        auto want_v1 = read_v1(ref);
        auto got_v1  = read_v1(sst);
        BOOST_REQUIRE_EQUAL(got_v1.size(), want_v1.size());
        for (size_t i = 0; i < got_v1.size(); ++i) {
            BOOST_REQUIRE_EQUAL(got_v1[i].partition().clustered_rows().calculate_size(),
                                want_v1[i].partition().clustered_rows().calculate_size());
            assert_that(got_v1[i]).is_equal_to(want_v1[i]);
        }
    }).get();
}
