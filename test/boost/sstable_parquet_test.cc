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
// The whole mutation model is covered here: row markers, row and partition
// tombstones, static rows, range tombstones, non-frozen collections and counters.
// sstable_conforms_to_mutation_source_test holds pq to the same contract as every
// other writable version and is the broader net; these cases are the targeted
// ones, each aimed at a specific way the encoding can go wrong, and they exist
// because iterating on the conformance suite means a three-minute build per guess.

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include "test/lib/simple_schema.hh"
#include "test/lib/sstable_test_env.hh"
#include "sstables/parquet/gain_estimator.hh"
#include "compaction/size_tiered_compaction_strategy.hh"
#include "sstables/parquet/tiering_context.hh"
#include "sstables/compressor.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/eventually.hh"

#include "schema/schema_builder.hh"
#include "readers/from_mutations.hh"
#include "readers/combined.hh"
#include "readers/mutation_fragment_v1_stream.hh"
#include "sstables/sstables.hh"
#include "sstables/parquet/format/parquet_metadata.hh"
#include "sstables/parquet/footer_cache.hh"
#include "partition_slice_builder.hh"
#include "mutation/mutation.hh"
#include "mutation/collection_mutation.hh"
#include "mutation/counters.hh"
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
std::vector<sstring> fragments_in(shared_sstable sst, schema_ptr s, reader_permit permit,
                                  const dht::partition_range& pr,
                                  const query::partition_slice& slice) {
    auto rd = sst->make_reader(s, permit, pr, slice);
    auto close = deferred_close(rd);
    std::vector<sstring> out;
    while (auto mf = rd().get()) {
        out.push_back(seastar::format("{}", mutation_fragment_v2::printer(*s, *mf)));
    }
    return out;
}

std::vector<sstring> fragments_of(shared_sstable sst, schema_ptr s, reader_permit permit) {
    return fragments_in(sst, s, permit, query::full_partition_range, s->full_slice());
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

// A *bounded* range wide enough that the reader streams whole row groups instead of paging
// 512 rows at a time. That choice used to be made from the presence of a bound rather than the
// width of the range (design doc 10.26), so this is the case whose read path changed, and the
// thing to prove about it is that the rows did not: fragment for fragment against the row
// format, over the whole span, over an interior sub-range that starts and ends mid-row-group,
// and over a single partition -- the three shapes the per-row-group decision has to get right.
//
// 40 x 200 = 8 000 rows cuts more than one row group at the 5 000-row default, so the span case
// really does contain interior groups wanted whole, which is what the cheap half of the
// predicate resolves; the sub-range case exercises the other half, where a partial group is
// compared against its own page index.
SEASTAR_THREAD_TEST_CASE(test_pq_bounded_range_streams_and_agrees_with_row_format) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 40, 200);
        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), muts).get();

        auto check = [&] (const char* what, const dht::partition_range& pr, bool expect_rows = true) {
            auto fw = fragments_in(ref, s, env.make_reader_permit(), pr, s->full_slice());
            auto fg = fragments_in(sst, s, env.make_reader_permit(), pr, s->full_slice());
            BOOST_TEST_CONTEXT(what) {
                BOOST_REQUIRE_EQUAL(fg.size(), fw.size());
                for (size_t i = 0; i < fg.size(); ++i) {
                    BOOST_REQUIRE_EQUAL(fg[i], fw[i]);
                }
                // Guards the guard: a range that silently returned nothing would pass every
                // comparison above.
                BOOST_REQUIRE_EQUAL(!fg.empty(), expect_rows);
            }
        };

        // The whole ring segment the sstable holds, bounded at both ends -- what a range scan
        // looks like once the coordinator has split it at a tablet boundary.
        check("full span, both bounds closed",
              dht::partition_range::make({muts.front().decorated_key(), true},
                                         {muts.back().decorated_key(), true}));
        // Open start, closed end: the first piece of a split, and the shape that made
        // `start() || end()` true for every scan in the first place.
        check("open start, closed end",
              dht::partition_range::make_ending_with({muts.back().decorated_key(), true}));
        check("closed start, open end",
              dht::partition_range::make_starting_with({muts.front().decorated_key(), true}));
        // An interior slab, and the same slab with its bounds excluded, so the boundary row
        // groups are partial at both ends.
        check("interior, inclusive",
              dht::partition_range::make({muts[7].decorated_key(), true},
                                         {muts[31].decorated_key(), true}));
        check("interior, exclusive",
              dht::partition_range::make({muts[7].decorated_key(), false},
                                         {muts[31].decorated_key(), false}));
        // One partition, which must keep taking the narrow path it was measured on.
        check("singular", dht::partition_range::make_singular(muts[19].decorated_key()));
        // A range that selects nothing between two adjacent keys.
        check("empty interior",
              dht::partition_range::make({muts[5].decorated_key(), false},
                                         {muts[6].decorated_key(), false}),
              false);
    }).get();
}

// Why the reader does *not* push column projection down, pinned as a test rather than left as a
// comment, because the natural "optimisation" is a data-loss bug and it would pass every other
// case in this file.
//
// A CQL row is live if its marker is live or any of its cells is. make_muts() writes cells with no
// row marker -- which is what an UPDATE produces -- so every row here is live only by virtue of
// its cells. A reader that honoured `with_no_regular_columns()` by not reading them would return
// rows with nothing in them, the compacting reader above it would judge those rows dead, and a
// `SELECT count(*)` or a `SELECT other_column` would quietly lose them. mx returns every cell
// whatever the slice says, and Cassandra reads every regular column from storage for this exact
// reason, so agreement with the row format is the contract.
SEASTAR_THREAD_TEST_CASE(test_pq_restricted_slice_still_returns_every_cell) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 8, 30);
        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), muts).get();

        for (auto&& [what, slice] : std::vector<std::pair<const char*, query::partition_slice>>{
                {"no regular columns",
                 partition_slice_builder(*s).with_no_regular_columns().build()},
                {"one regular column",
                 partition_slice_builder(*s).with_regular_column(to_bytes("v_int")).build()}}) {
            auto fw = fragments_in(ref, s, env.make_reader_permit(),
                                   query::full_partition_range, slice);
            auto fg = fragments_in(sst, s, env.make_reader_permit(),
                                   query::full_partition_range, slice);
            BOOST_TEST_CONTEXT(what) {
                BOOST_REQUIRE(!fg.empty());
                BOOST_REQUIRE_EQUAL(fg.size(), fw.size());
                for (size_t i = 0; i < fg.size(); ++i) {
                    BOOST_REQUIRE_EQUAL(fg[i], fw[i]);
                }
                // And the cells really are there: a stream that had dropped them would still
                // match a row format that had dropped them too, if one ever did.
                BOOST_REQUIRE(std::ranges::any_of(fg, [] (const sstring& f) {
                    return f.find("v_txt") != sstring::npos;
                }));
            }
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

// ---------------------------------------------------------------------------
// Mixed-format (hybrid) tables.
//
// `storage_format = 'hybrid'` means native and pq sstables coexist in one table, so every read
// merges across both and every compaction can take one of each as input. Until now nothing tested
// that. The pq suite built a native sstable beside a pq one in eight places, but always to compare
// two *separate* readers -- never to merge them -- and the one place a mixed compaction actually
// happens (cql_ddl_test's test_storage_format_converts_on_compaction) neither asserts that the
// input set was mixed nor reads back the rows that came through the native input.
//
// That gap matters more than the single-format round trip, because merging is where a wrong
// encoding turns into wrong data rather than a wrong file. A tombstone that pq stores as an absent
// cell reads back correctly from the pq sstable alone -- there is nothing there to contradict it --
// and only resurrects the value it was meant to shadow when an older sstable supplies one. The
// merge is the first place the difference between `dead` and `absent` is observable at all.
//
// The reference in all of these is the same merge with both inputs in the native format: the
// question is whether pq behaves like the row format under merge, not whether either matches some
// hand-written expectation.

namespace {

// A static column is included deliberately: statics are shredded as regular `__s_<name>` columns
// replayed onto every row and split back out on read, so a static cell deleted in the overlay is
// the case where a lost `__dmask` bit resurrects a value on *every* row of the partition.
schema_ptr hybrid_schema() {
    return schema_builder(1, "ks", "hyb_tbl")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("a", int32_type)
        .with_column("b", int32_type)
        .with_column("c", utf8_type)
        .with_column("st", int32_type, column_kind::static_column)
        .build();
}

constexpr int hyb_parts = 8;
constexpr int hyb_rows = 10;

partition_key hyb_pk(const schema& s, int p) {
    return partition_key::from_single_value(s, utf8_type->decompose(sstring(format("hk{:04d}", p))));
}

clustering_key hyb_ck(const schema& s, int r) {
    return clustering_key::from_single_value(s, int32_type->decompose(r));
}

void hyb_sort(utils::chunked_vector<mutation>& muts) {
    std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
        return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
    });
}

// The older generation: every row fully populated, one low timestamp band. Partition 6 is absent,
// so the merge has a partition that exists only in the overlay.
utils::chunked_vector<mutation> hybrid_base(schema_ptr s) {
    const auto& adef = *s->get_column_definition(to_bytes("a"));
    const auto& bdef = *s->get_column_definition(to_bytes("b"));
    const auto& cdef = *s->get_column_definition(to_bytes("c"));
    const auto& stdef = *s->get_column_definition(to_bytes("st"));

    utils::chunked_vector<mutation> muts;
    for (int p = 0; p < hyb_parts; ++p) {
        if (p == 6) { continue; }
        mutation m(s, hyb_pk(*s, p));
        const api::timestamp_type ts = 1000 + p;
        m.set_static_cell(stdef, atomic_cell::make_live(*int32_type, ts,
                                                        int32_type->decompose(p * 11)));
        for (int r = 0; r < hyb_rows; ++r) {
            if (r == 9) { continue; }   // row 9 exists only in the overlay
            auto ck = hyb_ck(*s, r);
            // Row 8's marker is deliberately *newer* than its cells, and newer than the
            // shadowable tombstone the overlay puts on it. That is the only arrangement under
            // which the shadowable and regular halves of a row tombstone are distinguishable:
            // the marker cancels a shadowable tombstone but not a regular one, so a pq reader
            // that collapses the two halves deletes cells that must survive. It is observable
            // only against an older generation -- from the overlay alone there are no cells for
            // the difference to act on -- which is why it lives here and not in the
            // single-sstable round-trip tests.
            m.partition().clustered_row(*s, ck).apply(row_marker(r == 8 ? ts + 2000 : ts));
            m.set_clustered_cell(ck, adef, atomic_cell::make_live(
                    *int32_type, ts, int32_type->decompose(r)));
            m.set_clustered_cell(ck, bdef, atomic_cell::make_live(
                    *int32_type, ts, int32_type->decompose(r * 100)));
            m.set_clustered_cell(ck, cdef, atomic_cell::make_live(
                    *utf8_type, ts, utf8_type->decompose(sstring(format("base{}", r)))));
        }
        muts.push_back(std::move(m));
    }
    hyb_sort(muts);
    return muts;
}

// The newer generation: one of every tombstone and update shape, at a strictly higher timestamp
// band so it always wins the merge. Partition 5 is absent, so the merge also has a partition that
// exists only in the base.
utils::chunked_vector<mutation> hybrid_overlay(schema_ptr s) {
    const auto& adef = *s->get_column_definition(to_bytes("a"));
    const auto& cdef = *s->get_column_definition(to_bytes("c"));
    const auto& stdef = *s->get_column_definition(to_bytes("st"));

    utils::chunked_vector<mutation> muts;
    for (int p = 0; p < hyb_parts; ++p) {
        if (p == 5) { continue; }
        mutation m(s, hyb_pk(*s, p));
        const api::timestamp_type ts = 2000 + p;
        const auto ldt = gc_clock::time_point(gc_clock::duration(70000 + p));

        // Partition 7 is deleted outright: everything the base holds for it must disappear.
        if (p == 7) {
            m.partition().apply(tombstone(ts, ldt));
            muts.push_back(std::move(m));
            continue;
        }

        // A static cell updated, deleted, or left alone -- the deleted arm is the one that must
        // not come back as absent.
        if (p % 3 == 0) {
            m.set_static_cell(stdef, atomic_cell::make_live(*int32_type, ts,
                                                           int32_type->decompose(p * 11 + 1)));
        } else if (p % 3 == 1) {
            m.set_static_cell(stdef, atomic_cell::make_dead(ts, ldt));
        }

        // Row 0: a cell updated, with `b` untouched, so the merge must take `a` from the overlay
        // and `b` from the base. A cell wrongly written as absent is indistinguishable from
        // "untouched" here, which is exactly the confusion being tested.
        m.set_clustered_cell(hyb_ck(*s, 0), adef, atomic_cell::make_live(
                *int32_type, ts, int32_type->decompose(999)));

        // Row 1: a cell deleted. `b` and `c` must survive from the base, `a` must not.
        m.set_clustered_cell(hyb_ck(*s, 1), adef, atomic_cell::make_dead(ts, ldt));

        // Row 2: a row tombstone.
        m.partition().clustered_row(*s, hyb_ck(*s, 2)).apply(row_tombstone(tombstone(ts, ldt)));

        // Row 3: a shadowable tombstone plus a marker -- what an UPDATE produces. The regular half
        // must stay empty; collapsing the two would delete cells that should survive.
        {
            auto& dr = m.partition().clustered_row(*s, hyb_ck(*s, 3));
            dr.apply(row_marker(ts));
            dr.apply(shadowable_tombstone(ts, ldt));
        }

        // Rows 4-6: a range tombstone over a band the base populated. If pq loses the bound weight
        // or the tombstone itself, three rows come back from the dead.
        m.partition().apply_delete(*s, range_tombstone(
                clustering_key_prefix::from_single_value(*s, int32_type->decompose(4)),
                bound_kind::incl_start,
                clustering_key_prefix::from_single_value(*s, int32_type->decompose(6)),
                bound_kind::incl_end,
                tombstone(ts, ldt)));

        // Row 7: an expiring cell. A live cell with a TTL carries a deletion time too, so it is
        // the case that must *not* be read as dead.
        m.set_clustered_cell(hyb_ck(*s, 7), cdef, atomic_cell::make_live(
                *utf8_type, ts, utf8_type->decompose(sstring("ttl")),
                gc_clock::time_point(gc_clock::duration(90000 + p)), gc_clock::duration(600)));

        // Row 8: a shadowable tombstone with no regular half, sitting *below* the base row's
        // marker. The marker cancels it, so every base cell on row 8 must survive. If pq rebuilds
        // the row tombstone with its regular half as strong as its shadowable one, those cells are
        // deleted instead -- a silent data loss that no single-format round trip can see.
        m.partition().clustered_row(*s, hyb_ck(*s, 8)).apply(shadowable_tombstone(ts, ldt));

        // Row 9 exists only in the overlay.
        m.partition().clustered_row(*s, hyb_ck(*s, 9)).apply(row_marker(ts));
        m.set_clustered_cell(hyb_ck(*s, 9), adef, atomic_cell::make_live(
                *int32_type, ts, int32_type->decompose(9)));

        muts.push_back(std::move(m));
    }
    hyb_sort(muts);
    return muts;
}

// Dead atomic cells, regular and static, in a reassembled result. The merge assertions compare
// against a native reference, so they would pass just as well if *both* sides lost every
// tombstone; this is what stops that.
size_t count_dead_cells(const schema& s, const utils::chunked_vector<mutation>& ms) {
    size_t n = 0;
    auto scan = [&] (const row& cells, column_kind kind) {
        cells.for_each_cell([&] (column_id id, const atomic_cell_or_collection& acoc) {
            const auto& def = s.column_at(kind, id);
            if (def.is_atomic() && !acoc.as_atomic_cell(def).is_live()) { ++n; }
        });
    };
    for (const auto& m : ms) {
        scan(m.partition().static_row().get(), column_kind::static_column);
        for (const rows_entry& re : m.partition().clustered_rows()) {
            scan(re.row().cells(), column_kind::regular_column);
        }
    }
    return n;
}

size_t count_rows(const utils::chunked_vector<mutation>& ms) {
    size_t n = 0;
    for (const auto& m : ms) {
        n += std::distance(m.partition().clustered_rows().begin(),
                           m.partition().clustered_rows().end());
    }
    return n;
}

} // namespace

// A read that spans a native and a pq sstable in the same table, in both orders.
//
// The three arms are the two mixed orders plus an all-pq control. The control matters: if pq lost a
// tombstone, the all-pq arm would lose it on both sides of the merge and could still agree with
// itself -- it is the *mixed* arms that catch it, and having the control in the same test makes it
// obvious which one failed.
SEASTAR_THREAD_TEST_CASE(test_hybrid_read_merges_native_and_parquet) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = hybrid_schema();
        auto base = hybrid_base(s);
        auto over = hybrid_overlay(s);
        const auto nat = sstables::get_highest_sstable_version();

        auto mk = [&] (sstable_version_types v, const utils::chunked_vector<mutation>& ms) {
            return make_sstable_containing(env.make_sstable(s, v), ms).get();
        };
        auto nat_base = mk(nat, base), nat_over = mk(nat, over);
        auto pq_base = mk(sstable_version_types::pq, base);
        auto pq_over = mk(sstable_version_types::pq, over);

        // The query path: two make_reader()s over the full range, combined. This is what a
        // coordinator read of a hybrid table does.
        auto merged = [&] (shared_sstable x, shared_sstable y) {
            std::vector<mutation_reader> rds;
            rds.push_back(x->make_reader(s, env.make_reader_permit(),
                                         query::full_partition_range, s->full_slice()));
            rds.push_back(y->make_reader(s, env.make_reader_permit(),
                                         query::full_partition_range, s->full_slice()));
            return make_combined_reader(s, env.make_reader_permit(), std::move(rds));
        };
        auto merged_frags = [&] (shared_sstable x, shared_sstable y) {
            auto rd = merged(x, y);
            auto close = deferred_close(rd);
            std::vector<sstring> out;
            while (auto mf = rd().get()) {
                out.push_back(seastar::format("{}", mutation_fragment_v2::printer(*s, *mf)));
            }
            return out;
        };
        auto merged_muts = [&] (shared_sstable x, shared_sstable y) {
            auto rd = merged(x, y);
            auto close = deferred_close(rd);
            utils::chunked_vector<mutation> out;
            while (auto m = read_mutation_from_mutation_reader(rd).get()) {
                out.push_back(std::move(*m));
            }
            return out;
        };

        const auto want_f = merged_frags(nat_base, nat_over);
        const auto want_m = merged_muts(nat_base, nat_over);

        // Preconditions on the reference, so none of the comparisons below can pass by being
        // uniformly empty. The row count is the range-tombstone check: the base wrote 9 rows for
        // each of 7 partitions and the overlay adds row 9, but rows 4-6 are deleted in every
        // partition the overlay touches and partition 7 is deleted whole.
        const size_t dead_ref = count_dead_cells(*s, want_m);
        BOOST_REQUIRE_GT(dead_ref, 0u);
        BOOST_REQUIRE_GT(want_m.size(), 0u);
        const size_t rows_ref = count_rows(want_m);
        BOOST_REQUIRE_GT(rows_ref, 0u);
        BOOST_REQUIRE_LT(rows_ref, size_t(hyb_parts * hyb_rows));

        struct arm { const char* what; shared_sstable lo; shared_sstable hi; };
        for (auto a : {arm{"native base + parquet overlay", nat_base, pq_over},
                       arm{"parquet base + native overlay", pq_base, nat_over},
                       arm{"parquet base + parquet overlay (control)", pq_base, pq_over}}) {
            BOOST_TEST_CONTEXT("mixed set: " << a.what) {
                // Fragments first: reassembling into mutations normalises range-tombstone bounds,
                // which is precisely what a lost bound weight would hide.
                auto got_f = merged_frags(a.lo, a.hi);
                BOOST_REQUIRE_EQUAL(got_f.size(), want_f.size());
                for (size_t i = 0; i < got_f.size(); ++i) {
                    BOOST_REQUIRE_EQUAL(got_f[i], want_f[i]);
                }

                auto got_m = merged_muts(a.lo, a.hi);
                BOOST_REQUIRE_EQUAL(got_m.size(), want_m.size());
                for (size_t i = 0; i < got_m.size(); ++i) {
                    assert_that(got_m[i]).is_equal_to(want_m[i]);
                }
                BOOST_REQUIRE_EQUAL(count_dead_cells(*s, got_m), dead_ref);
                BOOST_REQUIRE_EQUAL(count_rows(got_m), rows_ref);
            }
        }
    }).get();
}

// A compaction whose input set is mixed, writing each output format in turn.
//
// This is the ICS-under-hybrid steady state and the duration of any ALTER between formats: the
// merge happens through make_full_scan_reader rather than make_reader, and its result is written
// back out, so a tombstone lost here is lost *permanently* -- the next compaction has no older
// sstable left to contradict. Both output formats are covered because the hybrid decision can go
// either way for the same input set depending on tiering.
SEASTAR_THREAD_TEST_CASE(test_hybrid_compaction_merges_native_and_parquet) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = hybrid_schema();
        auto base = hybrid_base(s);
        auto over = hybrid_overlay(s);
        const auto nat = sstables::get_highest_sstable_version();
        const size_t n_part = std::max(base.size(), over.size());

        auto mk = [&] (sstable_version_types v, const utils::chunked_vector<mutation>& ms) {
            return make_sstable_containing(env.make_sstable(s, v), ms).get();
        };
        auto nat_base = mk(nat, base), nat_over = mk(nat, over);
        auto pq_base = mk(sstable_version_types::pq, base);
        auto pq_over = mk(sstable_version_types::pq, over);

        // Compact `x` and `y` into one sstable of version `out_v`, the way compaction does.
        auto compact = [&] (shared_sstable x, shared_sstable y, sstable_version_types out_v) {
            auto out = env.make_sstable(s, out_v);
            std::vector<mutation_reader> rds;
            rds.push_back(x->make_full_scan_reader(s, env.make_reader_permit(), nullptr,
                                                  default_read_monitor()));
            rds.push_back(y->make_full_scan_reader(s, env.make_reader_permit(), nullptr,
                                                  default_read_monitor()));
            auto merged = make_combined_reader(s, env.make_reader_permit(), std::move(rds));
            auto cfg = env.manager().configure_writer("test");
            out->write_components(std::move(merged), n_part, s, cfg, encoding_stats{}).get();
            out->open_data().get();
            return out;
        };

        // The reference: native inputs, native output -- the pre-existing behaviour.
        auto want_sst = compact(nat_base, nat_over, nat);
        const auto want_m = read_all(want_sst, s, env.make_reader_permit());
        const auto want_f = fragments_of(want_sst, s, env.make_reader_permit());
        const size_t dead_ref = count_dead_cells(*s, want_m);
        const size_t rows_ref = count_rows(want_m);
        BOOST_REQUIRE_GT(dead_ref, 0u);
        BOOST_REQUIRE_GT(rows_ref, 0u);
        BOOST_REQUIRE_LT(rows_ref, size_t(hyb_parts * hyb_rows));

        struct arm { const char* what; shared_sstable lo; shared_sstable hi;
                     sstable_version_types out; };
        for (auto a : {arm{"mixed in, parquet out", nat_base, pq_over,
                           sstable_version_types::pq},
                       arm{"mixed in, native out", nat_base, pq_over, nat},
                       arm{"mixed in reversed, parquet out", pq_base, nat_over,
                           sstable_version_types::pq},
                       arm{"mixed in reversed, native out", pq_base, nat_over, nat},
                       arm{"parquet in, parquet out (control)", pq_base, pq_over,
                           sstable_version_types::pq}}) {
            BOOST_TEST_CONTEXT("compaction: " << a.what) {
                auto out = compact(a.lo, a.hi, a.out);
                BOOST_REQUIRE(out->get_version() == a.out);

                auto got_f = fragments_of(out, s, env.make_reader_permit());
                BOOST_REQUIRE_EQUAL(got_f.size(), want_f.size());
                for (size_t i = 0; i < got_f.size(); ++i) {
                    BOOST_REQUIRE_EQUAL(got_f[i], want_f[i]);
                }

                auto got_m = read_all(out, s, env.make_reader_permit());
                BOOST_REQUIRE_EQUAL(got_m.size(), want_m.size());
                for (size_t i = 0; i < got_m.size(); ++i) {
                    assert_that(got_m[i]).is_equal_to(want_m[i]);
                }
                BOOST_REQUIRE_EQUAL(count_dead_cells(*s, got_m), dead_ref);
                BOOST_REQUIRE_EQUAL(count_rows(got_m), rows_ref);
            }
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

// A deleted cell must not come back as a cell that was never written.
//
// L0 folding stores a per-column `__live_` flag, so it can tell "dead" from "absent".
// L1 and L2 -- and L1 is the default -- do not: they carry the value, a `__ttl_` and a
// `__ldt_`. Deadness therefore has to be read off `__ldt_`, and the reassembler used to
// bail on a missing value before looking at it:
//
//     if (!present) { continue; }
//
// so every dead cell in an L1 file was silently dropped on the way back. That is the
// worst shape of bug this format can have: the file is valid, the read succeeds, and a
// cell the user deleted returns as though the delete never happened -- resurrecting
// whatever it shadowed on merge. It was invisible to a round-trip test that only wrote
// live cells, and it is why this case gets its own test.
SEASTAR_THREAD_TEST_CASE(test_pq_dead_cells_are_not_lost) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = schema_builder(1, "ks", "pq_dead")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("a", int32_type)
            .with_column("b", int32_type)
            .with_column("c", utf8_type)
            .build();
        const auto& adef = *s->get_column_definition(to_bytes("a"));
        const auto& bdef = *s->get_column_definition(to_bytes("b"));
        const auto& cdef = *s->get_column_definition(to_bytes("c"));

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 12; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:04d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 5000 + p;
            for (int r = 0; r < 5; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                const auto ldt = gc_clock::time_point(gc_clock::duration(3600 + r));
                switch ((p + r) % 5) {
                case 0:
                    // The case that was lost: the row's *only* content is a dead cell.
                    m.set_clustered_cell(ck, adef, atomic_cell::make_dead(ts, ldt));
                    break;
                case 1:
                    // Dead beside live, so the row survives either way and only the
                    // deletion itself goes missing.
                    m.set_clustered_cell(ck, adef, atomic_cell::make_dead(ts, ldt));
                    m.set_clustered_cell(ck, bdef, atomic_cell::make_live(
                            *int32_type, ts, int32_type->decompose(r)));
                    break;
                case 2:
                    // Every column dead.
                    m.set_clustered_cell(ck, adef, atomic_cell::make_dead(ts, ldt));
                    m.set_clustered_cell(ck, bdef, atomic_cell::make_dead(ts, ldt));
                    m.set_clustered_cell(ck, cdef, atomic_cell::make_dead(ts, ldt));
                    break;
                case 3:
                    // A live cell with a TTL also carries an ldt (its expiry), so it
                    // must still read back as live -- the discriminator is the value,
                    // not the presence of an ldt.
                    m.set_clustered_cell(ck, adef, atomic_cell::make_live(
                            *int32_type, ts, int32_type->decompose(r),
                            gc_clock::time_point(gc_clock::duration(9000 + r)),
                            gc_clock::duration(600)));
                    m.set_clustered_cell(ck, bdef, atomic_cell::make_dead(ts, ldt));
                    break;
                default:
                    // Absent must stay absent: `b` and `c` are never written here.
                    m.set_clustered_cell(ck, adef, atomic_cell::make_live(
                            *int32_type, ts, int32_type->decompose(r)));
                    break;
                }
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
            assert_that(got[i]).is_equal_to(want[i]);
        }

        // Count the dead cells explicitly, so this cannot pass by both sides being
        // equally empty -- which is exactly how the bug hid.
        auto count_dead = [&] (const utils::chunked_vector<mutation>& ms) {
            size_t n = 0;
            for (const auto& m : ms) {
                for (const rows_entry& re : m.partition().clustered_rows()) {
                    re.row().cells().for_each_cell([&] (column_id id,
                                                       const atomic_cell_or_collection& acoc) {
                        const auto& def = s->regular_column_at(id);
                        if (def.is_atomic() && !acoc.as_atomic_cell(def).is_live()) { ++n; }
                    });
                }
            }
            return n;
        };
        const size_t dead_ref = count_dead(want), dead_pq = count_dead(got);
        BOOST_REQUIRE_GT(dead_ref, 0u);
        BOOST_REQUIRE_EQUAL(dead_pq, dead_ref);
    }).get();
}

// The folded deletion channel, on both write paths, from one body of data.
//
// A dead cell's deletion time used to go into its own column's `__ldt_<col>` leaf: one leaf per
// column, where the same rows' *write* times were folded into a single `__ts` for the whole row.
// On the corpus's 197-column Backblaze slice that was 195 leaves and 60.1 MB against 1.9 MB for
// the write times -- the same information shape at ~32x the cost, and the reason `pq` paid ~63 MB
// where the row format paid ~10 MB for the same tombstones. `__ldt` + `__dmask` + the `__ldtx`
// pair replace it with four leaves regardless of table width.
//
// Asserted on both paths in one test, deliberately. An sstable is written either by
// cut_row_group() once it outgrows the row-group budget or by write_rows() in one shot when it
// fits, the choice is a function of data size and invisible to the operator, and that divergence
// has already produced two separate bugs here (design doc 8.2b: per-column encodings applied only
// on the cutting path; 10.15: the L2 footer key written only on the other). The two paths also
// differ in leaf set -- the cutting path must fix its leaves before it has seen all the rows, so
// it uses the conservative set -- which is exactly the kind of asymmetry that hides a bug, so the
// same rows go down both and the expectations differ only where they must.
//
// No TTLs anywhere here, on purpose: a live cell with a TTL legitimately keeps a per-column
// `__ldt_<col>` for its expiry, and leaving that case out is what lets this test assert the strong
// thing -- that on the derived path the per-column deletion leaves are *gone*, not merely empty.
// The mixed case is covered by test_pq_dead_cells_are_not_lost and by test_shred's matrix.
SEASTAR_THREAD_TEST_CASE(test_pq_folded_deletion_channel_on_both_write_paths) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        constexpr int NCOL = 8;
        auto build = [] (const char* name, std::optional<int> rows_per_rg) {
            auto sb = schema_builder(1, "ks", name)
                .with_column("pk", utf8_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key);
            for (int i = 0; i < NCOL; ++i) {
                sb.with_column(to_bytes(format("v{}", i)), int32_type);
            }
            if (rows_per_rg) {
                sb.set_parquet_options({{"rows_per_row_group", format("{}", *rows_per_rg)}});
            }
            return sb.build();
        };
        // Same shape, same data; the only difference is the row-group budget, which is what
        // selects the write path. 1 000 is the minimum the option accepts.
        auto s_cut   = build("pq_fold_cut", 1000);
        auto s_whole = build("pq_fold_whole", std::nullopt);

        // 3 000 rows: three row groups under a 1 000-row budget, one under the 5 000 default.
        constexpr int PARTS = 150, ROWS = 20;
        auto make_muts = [&] (const schema_ptr& s) {
            std::vector<const column_definition*> defs;
            for (int i = 0; i < NCOL; ++i) {
                defs.push_back(s->get_column_definition(to_bytes(format("v{}", i))));
            }
            utils::chunked_vector<mutation> muts;
            muts.reserve(PARTS);
            for (int p = 0; p < PARTS; ++p) {
                auto pk = partition_key::from_single_value(
                        *s, utf8_type->decompose(sstring(format("key{:06d}", p))));
                mutation m(s, pk);
                for (int r = 0; r < ROWS; ++r) {
                    auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                    const api::timestamp_type ts = 1700000000000000 + p * 100 + r;
                    // The row's deletion time, shared by its dead cells -- which is what a
                    // bind-NULL INSERT produces, since one statement carries one timestamp.
                    const auto row_ldt = gc_clock::time_point(gc_clock::duration(40000 + p));
                    for (int i = 0; i < NCOL; ++i) {
                        switch ((p + r + i) % 5) {
                        case 0:
                        case 1:
                            // Dead, sharing the row's deletion time. The common case, and the
                            // one the fold collapses.
                            m.set_clustered_cell(ck, *defs[i],
                                                 atomic_cell::make_dead(ts, row_ldt));
                            break;
                        case 2:
                            // Dead, but with its own deletion time -- an exception entry, as a
                            // cell deleted by a separate statement would be.
                            m.set_clustered_cell(ck, *defs[i], atomic_cell::make_dead(
                                    ts, gc_clock::time_point(
                                            gc_clock::duration(40000 + p + i * 13 + 1))));
                            break;
                        case 3:
                            m.set_clustered_cell(ck, *defs[i], atomic_cell::make_live(
                                    *int32_type, ts, int32_type->decompose(r * 7 + i)));
                            break;
                        default:
                            break;   // absent: never written, and must stay that way
                        }
                    }
                }
                muts.push_back(std::move(m));
            }
            std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
                return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
            });
            return muts;
        };

        auto count_dead = [] (const schema_ptr& s,
                              const utils::chunked_vector<mutation>& ms) {
            size_t n = 0;
            for (const auto& m : ms) {
                for (const rows_entry& re : m.partition().clustered_rows()) {
                    re.row().cells().for_each_cell([&] (column_id id,
                                                       const atomic_cell_or_collection& acoc) {
                        const auto& def = s->regular_column_at(id);
                        if (def.is_atomic() && !acoc.as_atomic_cell(def).is_live()) { ++n; }
                    });
                }
            }
            return n;
        };

        // The reference is a native-format sstable over the same mutations: it is what the rows
        // must still be after a pq round trip, deletion times included.
        auto ref_muts = make_muts(s_whole);
        auto ref = make_sstable_containing(
                env.make_sstable(s_whole, sstables::get_highest_sstable_version()),
                ref_muts).get();
        auto want = read_all(ref, s_whole, env.make_reader_permit());
        const size_t dead_ref = count_dead(s_whole, ref_muts);
        BOOST_REQUIRE_GT(dead_ref, 0u);

        struct arm { const char* what; schema_ptr s; bool expect_cut; };
        for (auto a : {arm{"cut_row_group", s_cut, true},
                       arm{"write_rows", s_whole, false}}) {
            BOOST_TEST_CONTEXT("write path: " << a.what) {
                auto muts = make_muts(a.s);
                auto sst = make_sstable_containing(
                        env.make_sstable(a.s, sstable_version_types::pq), std::move(muts)).get();

                const uint64_t len = sst->ondisk_data_size();
                auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
                std::vector<uint8_t> img(buf.get(), buf.get() + buf.size());
                auto md = sstables::parquet::format::parse_footer(img);

                // The arm is only meaningful if it actually took the path it names.
                if (a.expect_cut) {
                    BOOST_REQUIRE_GT(md.row_groups.size(), 1u);
                } else {
                    BOOST_REQUIRE_EQUAL(md.row_groups.size(), 1u);
                }

                std::vector<std::string> leaves;
                for (size_t i = 1; i < md.schema.size(); ++i) {
                    if (md.schema[i].is_leaf()) { leaves.push_back(md.schema[i].name); }
                }
                auto has = [&] (const std::string& n) {
                    return std::find(leaves.begin(), leaves.end(), n) != leaves.end();
                };
                // The folded channel, on both paths. This is the assertion the two prior
                // divergence bugs would have failed.
                BOOST_REQUIRE(has("__ldt"));
                BOOST_REQUIRE(has("__dmask"));
                BOOST_REQUIRE(has("__ldtx_mask"));
                BOOST_REQUIRE(has("__ldtx_vals"));

                // And the per-column deletion leaves must carry nothing. On the derived leaf
                // set they are not emitted at all -- with no TTLs there is no live expiry to
                // hold -- which is the leaf-count collapse the fold is for. On the
                // conservative set they exist because the writer had to fix its leaves before
                // seeing every row, but every chunk of them must be all-null: a value there
                // would mean a dead cell's deletion time still went per column.
                size_t percol = 0, percol_values = 0;
                for (int i = 0; i < NCOL; ++i) {
                    if (has(format("__ldt_v{}", i))) { ++percol; }
                }
                for (const auto& rg : md.row_groups) {
                    for (const auto& cc : rg.columns) {
                        if (!cc.meta) { continue; }
                        const std::string p = cc.meta->path();
                        if (p.rfind("__ldt_", 0) != 0) { continue; }
                        const int64_t nulls = cc.meta->stats && cc.meta->stats->null_count
                                ? *cc.meta->stats->null_count : -1;
                        BOOST_REQUIRE_EQUAL(nulls, cc.meta->num_values);
                        percol_values += size_t(cc.meta->num_values) - size_t(nulls);
                    }
                }
                BOOST_REQUIRE_EQUAL(percol_values, 0u);
                if (a.expect_cut) {
                    BOOST_REQUIRE_EQUAL(percol, size_t(NCOL));   // conservative: present, empty
                } else {
                    BOOST_REQUIRE_EQUAL(percol, 0u);             // derived: gone entirely
                }

                // Losslessness is the point of all of it: the rows, and every deletion time in
                // them, must match the native-format reference exactly.
                auto got = read_all(sst, a.s, env.make_reader_permit());
                BOOST_REQUIRE_EQUAL(got.size(), want.size());
                for (size_t i = 0; i < got.size(); ++i) {
                    assert_that(got[i]).is_equal_to(want[i]);
                }
                BOOST_REQUIRE_EQUAL(count_dead(a.s, got), dead_ref);
            }
        }
    }).get();
}

// `dead` and `absent` told apart on disk, per row group, with statistics-based leaf elision in
// play.
//
// Everything else in this file establishes that a deletion survives a round trip. That is necessary
// and not sufficient: a deletion stored as an absent cell round-trips perfectly through the pq
// reader, because there is nothing in the file to contradict it. It only becomes wrong when an
// older sstable supplies the value it should have shadowed -- which the hybrid merge tests now
// cover from above, and which this test covers from below, by asserting what is actually in the
// file rather than what comes back out of it.
//
// `__dmask` is the only thing in the format that distinguishes the two states: an absent cell
// contributes no def-level to it, a dead one contributes a set bit. So the count of non-null
// `__dmask` entries, summed over row groups, must equal the number of rows carrying at least one
// dead cell -- a quantity this test's generator knows exactly. Comparing against a total rather
// than per row group is deliberate: cut_row_group() cuts on partition boundaries and overshoots
// its budget, so which rows land in which group is the writer's business, and pinning it would
// make this a change-detector.
//
// It also pins the interaction between the fold and leaf elision, which is the one coupling in the
// read path where a lost tombstone would be silent. The reader elides a leaf whose statistics say
// it is null in every row of the group (`null_count == num_values`) and substitutes "no value for
// any row", so an all-null `__dmask` row group is skipped entirely and every column in it resolves
// to absent. That is correct exactly when the group really holds no dead cell. Partitions 20-59
// carry none, which at 50 rows each is 2 000 rows against a 1 000-row budget and therefore
// guarantees at least one wholly elidable group; partitions 0-19 and 60-79 do carry them, so at
// least one group is not elidable. Both states have to occur in the same file for the pairing of
// the two to be exercised at all.
SEASTAR_THREAD_TEST_CASE(test_pq_dead_and_absent_are_distinguishable_on_disk) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = schema_builder(1, "ks", "pq_deadmask")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("a", int32_type)
            .with_column("b", int32_type)
            .with_column("c", utf8_type)
            .with_column("st", int32_type, column_kind::static_column)
            .set_parquet_options({{"rows_per_row_group", "1000"}})
            .build();
        const auto& adef = *s->get_column_definition(to_bytes("a"));
        const auto& bdef = *s->get_column_definition(to_bytes("b"));
        const auto& cdef = *s->get_column_definition(to_bytes("c"));
        const auto& stdef = *s->get_column_definition(to_bytes("st"));

        constexpr int parts = 80, rows = 50;
        // The quiet band, wide enough to contain a whole row group with nothing dead in it.
        //
        // Banded by position in *token* order, not by key. The writer lays partitions out in token
        // order and cuts row groups on partition boundaries, so a band that is contiguous in the
        // key is scattered through the file and no row group comes out wholly quiet -- which is
        // exactly what the first run of this test found. So the empty partitions are built and
        // sorted first, and the band is a contiguous run of the sorted sequence.
        auto quiet = [] (size_t i) { return i >= 20 && i < 60; };
        // One partition in the noisy band has its static cell deleted. A static is shredded as a
        // regular column replayed onto every row, so this sets a __dmask bit on all 50 of them --
        // and if that bit is lost the value comes back on every row, not just one.
        constexpr size_t dead_static_part = 65;

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < parts; ++p) {
            muts.emplace_back(s, partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("dk{:04d}", p)))));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });

        // Rows carrying at least one dead cell, counted the way the shredder sees them: a deleted
        // static counts for every row of its partition.
        size_t expect_rows_with_dead = 0;
        for (size_t p = 0; p < muts.size(); ++p) {
            mutation& m = muts[p];
            const api::timestamp_type ts = 8000 + p;
            const auto ldt = gc_clock::time_point(gc_clock::duration(60000 + p));

            const bool dead_static = (p == dead_static_part);
            if (dead_static) {
                m.set_static_cell(stdef, atomic_cell::make_dead(ts, ldt));
            } else if (p % 7 == 0) {
                m.set_static_cell(stdef, atomic_cell::make_live(
                        *int32_type, ts, int32_type->decompose(int32_t(p))));
            }

            for (int r = 0; r < rows; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                bool row_has_dead = dead_static;

                // `b` is always live, so no row is ever empty and the row count is not itself
                // what carries the signal.
                m.set_clustered_cell(ck, bdef, atomic_cell::make_live(
                        *int32_type, ts, int32_type->decompose(r)));

                if (quiet(p)) {
                    // Live and absent only. `a` present, `c` absent on odd rows -- so the quiet
                    // band still exercises absence, which is the state a lost tombstone decays
                    // into and must therefore be distinguishable from.
                    m.set_clustered_cell(ck, adef, atomic_cell::make_live(
                            *int32_type, ts, int32_type->decompose(r * 3)));
                    if (r % 2 == 0) {
                        m.set_clustered_cell(ck, cdef, atomic_cell::make_live(
                                *utf8_type, ts, utf8_type->decompose(sstring(format("q{}", r)))));
                    }
                } else {
                    switch (r % 4) {
                    case 0:
                        // `a` dead, `c` absent: the pair that must not be conflated.
                        m.set_clustered_cell(ck, adef, atomic_cell::make_dead(ts, ldt));
                        row_has_dead = true;
                        break;
                    case 1:
                        // `c` dead with its own deletion time, so the exception channel is used.
                        m.set_clustered_cell(ck, cdef, atomic_cell::make_dead(
                                ts, gc_clock::time_point(gc_clock::duration(60000 + p + r))));
                        row_has_dead = true;
                        break;
                    case 2:
                        // Both dead, sharing the row's deletion time.
                        m.set_clustered_cell(ck, adef, atomic_cell::make_dead(ts, ldt));
                        m.set_clustered_cell(ck, cdef, atomic_cell::make_dead(ts, ldt));
                        row_has_dead = true;
                        break;
                    default:
                        // Nothing dead: `a` live, `c` absent. Interleaving these among the dead
                        // rows means __dmask's def-levels are genuinely sparse rather than a
                        // constant, which is what makes the null count meaningful.
                        m.set_clustered_cell(ck, adef, atomic_cell::make_live(
                                *int32_type, ts, int32_type->decompose(r * 5)));
                        break;
                    }
                }
                if (row_has_dead) { ++expect_rows_with_dead; }
            }
        }
        BOOST_REQUIRE_GT(expect_rows_with_dead, 0u);

        auto ref = make_sstable_containing(
                env.make_sstable(s, sstables::get_highest_sstable_version()), muts).get();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), muts).get();

        // ---- on disk ----
        const uint64_t len = sst->ondisk_data_size();
        auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
        std::vector<uint8_t> img(buf.get(), buf.get() + buf.size());
        auto md = sstables::parquet::format::parse_footer(img);

        // The premise of the whole test: more than one row group, so "elidable in one group and
        // not in another" is a state this file can actually be in.
        BOOST_REQUIRE_GT(md.row_groups.size(), 1u);

        size_t dmask_live = 0, dmask_groups = 0, all_null_groups = 0, partial_groups = 0;
        for (const auto& rg : md.row_groups) {
            for (const auto& cc : rg.columns) {
                if (!cc.meta || cc.meta->path() != "__dmask") { continue; }
                ++dmask_groups;
                // Statistics must be present, or elision is off and this test proves nothing
                // about it.
                BOOST_REQUIRE(cc.meta->stats && cc.meta->stats->null_count);
                const int64_t nulls = *cc.meta->stats->null_count;
                const int64_t n = cc.meta->num_values;
                BOOST_REQUIRE_GE(nulls, 0);
                BOOST_REQUIRE_LE(nulls, n);
                dmask_live += size_t(n - nulls);
                if (n > 0 && nulls == n) { ++all_null_groups; } else { ++partial_groups; }
            }
        }
        BOOST_REQUIRE_EQUAL(dmask_groups, md.row_groups.size());

        // The assertion this test exists for: exactly one non-null __dmask entry per row carrying
        // a dead cell, and none for a row whose columns are merely absent. A tombstone written as
        // an absent cell lowers this; nothing else does.
        BOOST_REQUIRE_EQUAL(dmask_live, expect_rows_with_dead);

        // Both elision states occur, so the read-back below exercises the elided path and the
        // decoded path in one file.
        BOOST_REQUIRE_GT(all_null_groups, 0u);
        BOOST_REQUIRE_GT(partial_groups, 0u);

        // The fold's own invariant, cheap to re-check here: no dead cell's deletion time went to a
        // per-column leaf. There are no TTLs in this schema's data, so any value in a
        // `__ldt_<col>` leaf would have to be a dead cell's.
        for (const auto& rg : md.row_groups) {
            for (const auto& cc : rg.columns) {
                if (!cc.meta || cc.meta->path().rfind("__ldt_", 0) != 0) { continue; }
                BOOST_REQUIRE(cc.meta->stats && cc.meta->stats->null_count);
                BOOST_REQUIRE_EQUAL(*cc.meta->stats->null_count, cc.meta->num_values);
            }
        }

        // ---- through the reader ----
        auto want = read_all(ref, s, env.make_reader_permit());
        auto got  = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), want.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(want[i]);
        }

        // And the counts, so this cannot pass by both sides losing the same thing. `absent` is
        // counted as well as `dead`: a dead cell wrongly stored as absent lowers the dead count,
        // while an absent cell wrongly materialised as dead raises it, and only checking both
        // pins the distinction in the direction that matters.
        auto tally = [&] (const utils::chunked_vector<mutation>& ms) {
            size_t dead = 0, live = 0;
            for (const auto& m : ms) {
                auto scan = [&] (const row& cells, column_kind kind) {
                    cells.for_each_cell([&] (column_id id,
                                            const atomic_cell_or_collection& acoc) {
                        const auto& def = s->column_at(kind, id);
                        if (!def.is_atomic()) { return; }
                        if (acoc.as_atomic_cell(def).is_live()) { ++live; } else { ++dead; }
                    });
                };
                scan(m.partition().static_row().get(), column_kind::static_column);
                for (const rows_entry& re : m.partition().clustered_rows()) {
                    scan(re.row().cells(), column_kind::regular_column);
                }
            }
            return std::pair<size_t, size_t>{dead, live};
        };
        const auto [dead_w, live_w] = tally(want);
        const auto [dead_g, live_g] = tally(got);
        BOOST_REQUIRE_GT(dead_w, 0u);
        BOOST_REQUIRE_GT(live_w, 0u);
        BOOST_REQUIRE_EQUAL(dead_g, dead_w);
        BOOST_REQUIRE_EQUAL(live_g, live_w);
    }).get();
}

// `metadata_folding = 'uniform'` is silently not honoured once an sstable cuts a row group.
//
// This started as a hunt for a bug that turned out not to be reachable, and the real finding is
// the one worth pinning. L2 keeps a single timestamp in the footer instead of a per-row column,
// and the reader requires that key. write_rows() -- the path taken when the whole sstable fits one
// row group -- emits it; cut_row_group() did not. That looked like "any L2 table large enough to
// cut is unreadable", which would have been serious.
//
// It is not, because the two paths differ in a second way that happens to cover the first: the
// cutting path fixes its leaf set before it has seen all the rows, so it uses the *conservative*
// set, which sets all_same_ts = false and turns on every optional metadata leaf. That breaks L2's
// precondition, build_mapped_schema() falls the level back to L1, and no uniform timestamp is ever
// needed.
//
// So the operator-visible behaviour is: the same table is L2 while it is small and L1 once it is
// large, with nothing logged. That is not data loss -- L1 is lossless and the rows read back
// exactly -- but it is a setting that stops applying at scale, and it belongs in a test rather
// than in someone's afternoon. If a later change makes the cutting path able to reach L2, this
// test fails and the footer-key guard in cut_row_group() is what keeps the file readable.
SEASTAR_THREAD_TEST_CASE(test_pq_uniform_folding_falls_back_when_row_groups_are_cut) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto sb = schema_builder(1, "ks", "pq_l2rg")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v_txt", utf8_type);
        for (int i = 0; i < 7; ++i) {
            sb.with_column(to_bytes(format("v{}", i)), int32_type);
        }
        sb.set_parquet_options({{"metadata_folding", "uniform"}});
        auto s = sb.build();
        const auto& vt = *s->get_column_definition(to_bytes("v_txt"));

        // One timestamp for every cell and no markers, TTLs or deletions -- L2's precondition is
        // satisfied by the *data*, so anything that stops it being used is the writer's choice
        // rather than the input's.
        constexpr int PARTS = 2500, ROWS = 24;
        constexpr api::timestamp_type TS = 1700000000000000;
        utils::chunked_vector<mutation> muts;
        muts.reserve(PARTS);
        for (int p = 0; p < PARTS; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:06d}", p))));
            mutation m(s, pk);
            for (int r = 0; r < ROWS; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                m.set_clustered_cell(ck, vt, atomic_cell::make_live(
                        *utf8_type, TS, utf8_type->decompose(sstring(format("v{}", r % 40)))));
                for (int i = 0; i < 7; ++i) {
                    m.set_clustered_cell(ck, *s->get_column_definition(to_bytes(format("v{}", i))),
                            atomic_cell::make_live(*int32_type, TS,
                                                   int32_type->decompose(r * 3 + i)));
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

        const uint64_t len = sst->ondisk_data_size();
        auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
        std::vector<uint8_t> img(buf.get(), buf.get() + buf.size());
        auto md = sstables::parquet::format::parse_footer(img);
        // A single row group would make the whole test vacuous.
        BOOST_REQUIRE_GT(md.row_groups.size(), 1u);

        const std::string* lvl = md.kv("scylla.folding_level");
        BOOST_REQUIRE(lvl);
        BOOST_REQUIRE_EQUAL(*lvl, "L1");
        // And the invariant that made this look dangerous: an L2 footer must carry its timestamp,
        // an L1 footer has no business carrying one.
        BOOST_REQUIRE(!md.kv("scylla.uniform_timestamp"));

        // Whatever level it landed on, the data has to come back exactly -- the fallback is a
        // size choice, not a correctness one.
        auto got = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }
    }).get();
}

// The same schema and data, small enough that no cut happens, does reach L2 -- which is what makes
// the fallback above a difference between the two write paths rather than a property of the data.
SEASTAR_THREAD_TEST_CASE(test_pq_uniform_folding_applies_without_a_cut) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto sb = schema_builder(1, "ks", "pq_l2small")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v_txt", utf8_type);
        sb.set_parquet_options({{"metadata_folding", "uniform"}});
        auto s = sb.build();
        const auto& vt = *s->get_column_definition(to_bytes("v_txt"));

        constexpr api::timestamp_type TS = 1700000000000000;
        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 20; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:06d}", p))));
            mutation m(s, pk);
            for (int r = 0; r < 10; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                m.set_clustered_cell(ck, vt, atomic_cell::make_live(
                        *utf8_type, TS, utf8_type->decompose(sstring(format("v{}", r)))));
            }
            muts.push_back(std::move(m));
        }
        std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
            return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
        });
        auto expected = muts;

        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();
        const uint64_t len = sst->ondisk_data_size();
        auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
        std::vector<uint8_t> img(buf.get(), buf.get() + buf.size());
        auto md = sstables::parquet::format::parse_footer(img);
        BOOST_REQUIRE_EQUAL(md.row_groups.size(), 1u);

        const std::string* lvl = md.kv("scylla.folding_level");
        BOOST_REQUIRE(lvl);
        BOOST_REQUIRE_EQUAL(*lvl, "L2");
        const std::string* u = md.kv("scylla.uniform_timestamp");
        BOOST_REQUIRE(u);
        BOOST_REQUIRE_EQUAL(*u, std::to_string(TS));

        auto got = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }
    }).get();
}

// Row groups are cut when the write buffer exceeds its budget, and the result still
// round-trips.
//
// Until this existed the writer emitted exactly one row group per sstable, so
// `fragment_shredder` buffered every row before encoding anything -- about 1.8 kB per row,
// which is 17 GiB at ten million rows (R-13, design doc 5.5a). It also meant the reader's
// multi-row-group path, which has existed all along, was never once exercised through the
// sstable layer. Turning on code that has never run is exactly how the delta-encoding
// bug got in, so this test drives the real thing rather than a unit of it.
//
// It uses the *shipping* budget rather than a test-only override, so the row count has to
// be large enough to trip it: 64 MiB at ~1.9 kB/row is about 35 600 rows.
SEASTAR_THREAD_TEST_CASE(test_pq_row_groups_are_cut_by_the_memory_budget) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        // Eight value columns rather than two: the budget is on *buffered bytes*, and a
        // wider row reaches it with far fewer rows, which keeps the test's runtime down.
        // A row here costs roughly 1.7 kB buffered (a std::map entry plus a cell per
        // column), so ~40 000 rows is one cut.
        auto sb = schema_builder(1, "ks", "pq_rg")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v_txt", utf8_type);
        for (int i = 0; i < 7; ++i) {
            sb.with_column(to_bytes(format("v{}", i)), int32_type);
        }
        auto s = sb.build();
        const auto& vt = *s->get_column_definition(to_bytes("v_txt"));

        constexpr int PARTS = 2500, ROWS = 24;      // 60 000 rows -> at least two cuts
        utils::chunked_vector<mutation> muts;
        muts.reserve(PARTS);
        for (int p = 0; p < PARTS; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:06d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 1000 + p;
            for (int r = 0; r < ROWS; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                m.set_clustered_cell(ck, vt, atomic_cell::make_live(
                        *utf8_type, ts, utf8_type->decompose(sstring(format("v{}", r % 40)))));
                for (int i = 0; i < 7; ++i) {
                    m.set_clustered_cell(ck, *s->get_column_definition(to_bytes(format("v{}", i))),
                            atomic_cell::make_live(*int32_type, ts,
                                                   int32_type->decompose(r * 3 + i)));
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

        // The point of the test: more than one row group actually happened. Without this
        // the round-trip below would pass on a single-row-group file and prove nothing.
        const uint64_t len = sst->ondisk_data_size();
        auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
        std::vector<uint8_t> img(buf.get(), buf.get() + buf.size());
        auto md = sstables::parquet::format::parse_footer(img);
        BOOST_TEST_MESSAGE(fmt::format("row groups: {}, rows: {}",
                                       md.row_groups.size(), md.num_rows));
        BOOST_REQUIRE_GT(md.row_groups.size(), 1u);
        BOOST_REQUIRE_EQUAL(md.num_rows, int64_t(PARTS) * ROWS);

        // Row-group row counts must sum to the total, or the reader's cumulative
        // ordinal table would point at the wrong group.
        int64_t sum = 0;
        for (const auto& g : md.row_groups) {
            BOOST_REQUIRE_GT(g.num_rows, 0);
            sum += g.num_rows;
        }
        BOOST_REQUIRE_EQUAL(sum, md.num_rows);

        auto got = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), expected.size());
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(expected[i]);
        }

        // And a single-partition read, which is the path that turns an index row ordinal
        // into a page and is where a row group boundary is most likely to be mishandled.
        for (int p : {0, PARTS / 2, PARTS - 1}) {
            auto& want = expected[size_t(p)];
            auto pr = dht::partition_range::make_singular(want.decorated_key());
            auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
            auto close = deferred_close(rd);
            auto m = read_mutation_from_mutation_reader(rd).get();
            BOOST_REQUIRE(m);
            assert_that(*m).is_equal_to(want);
        }
    }).get();
}

// Counter cells, which are atomic but not scalar: their value is a set of
// per-replica shards, and merging two counter cells means merging shards by id
// rather than taking the newer value. Stored as an opaque blob they would still
// read back byte-identical from a single sstable while being wrong the moment
// anything merged them, so this checks the shards individually as well as
// comparing whole mutations against the reference format.
SEASTAR_THREAD_TEST_CASE(test_pq_counters_round_trip) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = schema_builder(1, "ks", "pq_counters")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("c", counter_type)
            .with_column("c2", counter_type)
            .with_column("sc", counter_type, column_kind::static_column)
            .build();

        const auto& cdef   = *s->get_column_definition(to_bytes("c"));
        const auto& c2def  = *s->get_column_definition(to_bytes("c2"));
        const auto& scdef  = *s->get_column_definition(to_bytes("sc"));
        BOOST_REQUIRE(cdef.is_counter());

        // Deterministic shard ids, so the fixture is reproducible.
        auto shard_id = [] (int n) {
            return counter_id(utils::UUID(0x1000000000000000LL + n, 0x2000000000000000LL + n * 7));
        };
        auto make_counter = [&] (api::timestamp_type ts, int nshards, int salt) {
            counter_cell_builder b{size_t(nshards)};
            for (int i = 0; i < nshards; ++i) {
                b.add_maybe_unsorted_shard(counter_shard(
                        shard_id(i), int64_t(salt) * 1000 + i, int64_t(i) + 1));
            }
            b.sort_and_remove_duplicates();
            return b.build(ts);
        };

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 14; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:04d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 9000 + p;
            if (p % 5 != 4) {
                m.set_static_cell(scdef, make_counter(ts, 1 + p % 3, p + 50));
            }
            for (int r = 0; r < 4; ++r) {
                auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                const int kind = (p + r) % 4;
                if (kind == 0) {
                    m.set_clustered_cell(ck, cdef, make_counter(ts, 1, p));
                } else if (kind == 1) {
                    // Several shards: the case an opaque blob would fail to merge.
                    m.set_clustered_cell(ck, cdef, make_counter(ts, 5, p));
                } else if (kind == 2) {
                    // A deleted counter cell, which has no shards at all.
                    m.set_clustered_cell(ck, cdef, atomic_cell::make_dead(
                            ts, gc_clock::time_point(gc_clock::duration(p + 3))));
                } else {
                    m.set_clustered_cell(ck, cdef, make_counter(ts, 2, p));
                    m.set_clustered_cell(ck, c2def, make_counter(ts, 3, p + 20));
                }
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
            assert_that(got[i]).is_equal_to(want[i]);
        }

        // Shard-level check, so this cannot pass on blobs that merely compare
        // equal: every live counter cell must come back with its shard ids,
        // values and logical clocks intact, and multi-shard cells must exist.
        size_t live_cells = 0, multi_shard_cells = 0;
        for (size_t i = 0; i < got.size(); ++i) {
            for (const rows_entry& re : got[i].partition().clustered_rows()) {
                const auto* cell = re.row().cells().find_cell(cdef.id);
                if (!cell) { continue; }
                auto av = cell->as_atomic_cell(cdef);
                if (!av.is_live()) { continue; }
                ++live_cells;
                counter_cell_view ccv(av);
                size_t n = 0;
                for (auto&& cs : ccv.shards()) {
                    BOOST_REQUIRE_EQUAL(cs.id(), shard_id(int(n)));
                    BOOST_REQUIRE_EQUAL(cs.logical_clock(), int64_t(n) + 1);
                    ++n;
                }
                BOOST_REQUIRE_GT(n, 0u);
                if (n > 1) { ++multi_shard_cells; }
            }
        }
        BOOST_REQUIRE_GT(live_cells, 0u);
        BOOST_REQUIRE_GT(multi_shard_cells, 0u);
    }).get();
}

// Static content when the partition's *first* row is not a clustering row.
//
// The writer replays static cells onto every row of the partition and the reader
// rebuilds the static row from whichever row it sees first. That makes the first
// row's identity load-bearing, and two shapes make it something other than a
// clustering row: a range tombstone change that opens before all rows, and the
// placeholder emitted for a partition that has no rows at all. Both used to
// replay only the atomic static cells, so every static collection was silently
// dropped -- and only in those shapes, which is why the ordinary collections
// round-trip test never saw it.
//
// The partition-wide range tombstone matters for a second reason: its bounds are
// before/after_all_clustered_rows, whose clustering prefix is *empty but present*.
// Rebuilding those as an absent prefix yields a position that compares as
// nonsense rather than failing, which sent the walker past every range and
// dropped the partition's rows wholesale.
SEASTAR_THREAD_TEST_CASE(test_pq_statics_survive_a_leading_range_tombstone) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto map_si = map_type_impl::get_instance(utf8_type, int32_type, true);
        auto s = schema_builder(1, "ks", "pq_static_rtc")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v", int32_type)
            .with_column("sa", int32_type, column_kind::static_column)
            .with_column("sm", map_si, column_kind::static_column)
            .with_column("sm2", map_si, column_kind::static_column)
            .build();

        const auto& sadef  = *s->get_column_definition(to_bytes("sa"));
        const auto& smdef  = *s->get_column_definition(to_bytes("sm"));
        const auto& sm2def = *s->get_column_definition(to_bytes("sm2"));

        auto make_map = [&] (api::timestamp_type ts, int n, int salt) {
            collection_mutation_writer w{tombstone{}};
            for (int i = 0; i < n; ++i) {
                auto k = utf8_type->decompose(sstring(format("k{}_{}", salt, i)));
                auto kb = managed_bytes(reinterpret_cast<const int8_t*>(k.data()), k.size());
                w.push_back(managed_bytes_view(kb), atomic_cell::make_live(
                        *int32_type, ts, int32_type->decompose(i * 10 + salt)));
            }
            return atomic_cell_or_collection(std::move(w).finish());
        };

        utils::chunked_vector<mutation> muts;
        for (int p = 0; p < 16; ++p) {
            auto pk = partition_key::from_single_value(
                    *s, utf8_type->decompose(sstring(format("key{:04d}", p))));
            mutation m(s, pk);
            const api::timestamp_type ts = 7000 + p;

            // Every partition carries an atomic static cell and two static
            // collections. The atomic one is the control: it survived the bug, so
            // if only it comes back the collections were dropped.
            m.set_static_cell(sadef, atomic_cell::make_live(
                    *int32_type, ts, int32_type->decompose(p)));
            m.set_static_cell(smdef, make_map(ts, 3, p));
            if (p % 4 != 3) {
                m.set_static_cell(sm2def, make_map(ts, 2, p + 100));
            }

            const int shape = p % 4;
            if (shape != 2) {                       // shape 2: no rows at all
                for (int r = 0; r < 6; ++r) {
                    auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                    m.set_clustered_cell(ck, *s->get_column_definition(to_bytes("v")),
                            atomic_cell::make_live(*int32_type, ts, int32_type->decompose(r)));
                }
            }

            if (shape == 0) {
                // Covers the whole partition: both bounds are an empty prefix.
                m.partition().apply_delete(*s, range_tombstone(
                        bound_view::bottom(), bound_view::top(),
                        tombstone(ts - 1, gc_clock::time_point(gc_clock::duration(p + 1)))));
            } else if (shape == 1) {
                // Opens before every row and closes in the middle, so the first
                // fragment after the static row is still a range tombstone change
                // but the partition keeps some live rows.
                auto hi = clustering_key_prefix::from_single_value(*s, int32_type->decompose(3));
                m.partition().apply_delete(*s, range_tombstone(
                        bound_view::bottom(),
                        bound_view(hi, bound_kind::incl_end),
                        tombstone(ts - 1, gc_clock::time_point(gc_clock::duration(p + 1)))));
            }
            // shape 3: ordinary partition, and sm2 absent -- the control case.
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
        // Assert the static collections are actually there, so this cannot pass by
        // both sides being equally empty.
        size_t with_static_collection = 0;
        for (size_t i = 0; i < got.size(); ++i) {
            assert_that(got[i]).is_equal_to(want[i]);
            const auto& sr = got[i].partition().static_row().get();
            if (sr.find_cell(smdef.id)) { ++with_static_collection; }
        }
        BOOST_REQUIRE_EQUAL(with_static_collection, got.size());

        auto fw = fragments_of(ref, s, env.make_reader_permit());
        auto fg = fragments_of(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(fg.size(), fw.size());
        for (size_t i = 0; i < fg.size(); ++i) {
            BOOST_REQUIRE_EQUAL(fg[i], fw[i]);
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
        for (int i = 0; i < 64; ++i) {
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
            for (int i = 0; i < 64; ++i) {
                const auto& cdef = *s->get_column_definition(to_bytes(format("s{}", i)));
                if (i % 2) { m.set_static_cell(cdef, make_list(ts, 2)); }
                else       { m.set_static_cell(cdef, atomic_cell::make_live(
                                     *bytes_type, ts, bytes_view(blob(i)))); }
            }
            for (int r = 0; r < 33; ++r) {
                auto ck = clustering_key::from_exploded(*s, {blob(r), blob(r + 1)});
                for (int i = 0; i < 64; ++i) {
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

// C6 of the tiering decision: the gain must be *measured* with the real writer over
// real data, and it must fail closed. Both halves are asserted here, because the
// failure mode that matters is a bad estimate silently converting a table.
SEASTAR_THREAD_TEST_CASE(test_c6_parquet_gain_is_measured_over_real_data) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        // A native sstable, which is what the estimator sees in a hybrid table:
        // the question it answers is "what would this become as Parquet".
        auto sst = make_sstable_containing(env.make_sstable(s), make_muts(s, 40, 250)).get();

        auto gain = sstables::parquet::estimate_parquet_gain(
                s, env.make_reader_permit(), {sst}, sstables::parquet::pq_writer_config{}).get();
        BOOST_REQUIRE(gain.has_value());
        BOOST_TEST_MESSAGE(seastar::format("measured gain: {:.3f} on {} on-disk bytes",
                                           *gain, sst->ondisk_data_size()));
        // A ratio, so bounded; and repetitive test data should not come out larger.
        BOOST_REQUIRE_GT(*gain, 0.0);
        BOOST_REQUIRE_LT(*gain, 1.0);

        // Deterministic: the same sample must yield the same answer, or the tiering
        // decision would flap between compactions.
        auto again = sstables::parquet::estimate_parquet_gain(
                s, env.make_reader_permit(), {sst}, sstables::parquet::pq_writer_config{}).get();
        BOOST_REQUIRE(again.has_value());
        BOOST_REQUIRE_EQUAL(*gain, *again);

        // Nothing to measure must read as "unknown", never as a gain. The policy
        // turns an unset gain into a rejection, so this is what keeps an
        // unmeasurable table in the native format.
        auto none = sstables::parquet::estimate_parquet_gain(
                s, env.make_reader_permit(), {}, sstables::parquet::pq_writer_config{}).get();
        BOOST_REQUIRE(!none.has_value());
    }).get();
}

// A Parquet sstable has no CompressionInfo component -- it compresses inside the file -- so
// sstable::get_compression_ratio() reported NO_COMPRESSION_RATIO (-1.0) for every Parquet table,
// i.e. nodetool and the REST API showed no ratio for a table that has a perfectly good one. The
// writer now records it in the statistics and the accessor falls back to that.
//
// Lives here rather than in cql_ddl_test because it is a property of the writer, and building the
// sstable directly avoids depending on whether a test-env major compaction chooses to rewrite.
SEASTAR_THREAD_TEST_CASE(test_pq_records_a_compression_ratio) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), make_muts(s, 30, 200)).get();

        const auto ratio = sst->get_compression_ratio();
        BOOST_TEST_MESSAGE(seastar::format("compression ratio: {}", ratio));
        // -1.0 is the "not recorded" sentinel, so a positive value is exactly the regression
        // being pinned.
        BOOST_REQUIRE_GT(ratio, 0.0);
        // And it must be a ratio, not a byte count: the test data repeats, so a real codec has
        // to come in under 1.0.
        BOOST_REQUIRE_LT(ratio, 1.0);

        // Cross-check against the file itself, so this cannot pass on a plausible-looking number
        // that bears no relation to the sstable: the numerator is the data component's size.
        const auto on_disk = double(sst->ondisk_data_size());
        BOOST_REQUIRE_GT(on_disk, 0.0);
        BOOST_REQUIRE_GT(on_disk / ratio, on_disk);   // implied uncompressed size is larger
    }).get();
}

// A counter column's map values are two big-endian int64s, not an opaque blob, and the Parquet
// schema cannot say so without a group inside the MAP value -- a third level of Dremel nesting,
// which is a schema change and not yet done. Until then the footer declares the convention, so a
// reader is not required to know it in advance. This pins that declaration.
SEASTAR_THREAD_TEST_CASE(test_pq_declares_the_counter_convention) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        // Counter and non-counter columns cannot coexist in one table -- Scylla rejects it -- so
        // the positive and negative cases need separate schemas.
        auto ctr = schema_builder(1, "ks", "ctr")
                .with_column("pk", utf8_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("hits", counter_type)
                .build();

        const auto cols = sstables::parquet::columns_of(*ctr);
        auto hits = std::ranges::find_if(cols, [] (const auto& c) { return c.name == "hits"; });
        BOOST_REQUIRE(hits != cols.end());
        // The flag the declaration depends on. A counter is multi_cell like a collection, and
        // before this nothing downstream could tell the two apart.
        BOOST_REQUIRE(hits->counter);
        BOOST_REQUIRE(hits->multi_cell);

        // A written file carries the declaration.
        utils::chunked_vector<mutation> muts;
        auto pk = partition_key::from_single_value(*ctr, utf8_type->decompose(sstring("p")));
        mutation m(ctr, pk);
        // A row marker, not an empty partition: make_sstable_containing has nothing to write for
        // a partition with no content. The declaration under test is a property of the schema, so
        // the row does not need a counter cell in it -- the counter round-trip is covered by the
        // conformance cases.
        auto ck = clustering_key::from_single_value(*ctr, int32_type->decompose(1));
        m.partition().apply_insert(*ctr, ck, api::timestamp_type(1000));
        muts.push_back(std::move(m));
        auto sst = make_sstable_containing(
                env.make_sstable(ctr, sstable_version_types::pq), std::move(muts)).get();

        const auto len = sst->ondisk_data_size();
        auto buf = sst->data_read(0, len, env.make_reader_permit()).get();
        auto md = sstables::parquet::format::parse_footer(
                std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(buf.get()), buf.size()));
        auto kv = [&] (const char* k) -> std::optional<std::string> {
            for (const auto& p : md.key_value_metadata) { if (p.key == k) { return p.value; } }
            return std::nullopt;
        };
        auto names = kv("scylla.counter_columns");
        BOOST_REQUIRE(names.has_value());
        BOOST_REQUIRE_EQUAL(*names, "hits");
        auto enc = kv("scylla.counter_encoding");
        BOOST_REQUIRE(enc.has_value());
        BOOST_REQUIRE(enc->find("logical_clock") != std::string::npos);

        // The declaration is the fallback; the schema itself is now typed, which is the part that
        // makes the column interpretable without knowing Scylla. Both `value` and `clock` must be
        // INT64 leaves of the counter's group -- not a packed BYTE_ARRAY.
        auto leaves = sstables::parquet::format::walk_leaves(md);
        int typed = 0;
        for (const auto& l : leaves) {
            const auto& path = l.path;
            if (path.size() < 2 || path.front() != "hits") { continue; }
            const auto& name = path.back();
            if (name == "value" || name == "clock") {
                // leaf_info carries the schema index rather than the type, so read the type from
                // the schema element it points at.
                BOOST_REQUIRE(md.schema.at(l.index).type
                              == sstables::parquet::format::phys_type::int64);
                ++typed;
            }
        }
        BOOST_REQUIRE_EQUAL(typed, 2);

        // And a table with no counters says nothing, rather than emitting an empty key.
        const auto plain_cols = sstables::parquet::columns_of(*pq_schema());
        BOOST_REQUIRE(std::ranges::none_of(plain_cols, [] (const auto& c) { return c.counter; }));
    }).get();
}

// Size-tiered bucketing has to compare like with like across formats.
//
// `data_size()` is two different quantities: for a compressed native sstable it is the data
// component's *uncompressed* length, and for a `pq` sstable -- which has no CompressionInfo,
// because Parquet compresses internally -- it is the file size. Bucketing is built on ratios, so
// inside an all-native set that inconsistency cancels out and is invisible. In a hybrid table it
// does not: the converted file reports several times smaller than the native sstable holding the
// same rows, buckets several tiers below its true peer, and compaction settles into repeatedly
// rewriting the one format that is most expensive to rewrite.
//
// Nothing about that failure is loud -- no error, no log line, just compaction declining to group
// the converted files -- so it needs a test. Both supported strategies are affected: ICS buckets
// this way directly and TWCS falls back to it within a window.
SEASTAR_THREAD_TEST_CASE(test_size_tiered_buckets_compare_one_unit_across_formats) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        // Compression has to be on for the two units to differ at all, and the data has to be
        // genuinely compressible for them to differ by enough to matter.
        //
        // make_muts() will not do for this: its mixed ints, doubles and short strings are
        // incompressible at a 4 KiB chunk length -- measured at 59 440 uncompressed against 67 172
        // on disk, i.e. LZ4 *expanded* it by 13 %. So a payload built to compress instead, which is
        // also the realistic case: the tables Parquet is aimed at are ones whose values repeat.
        auto s = schema_builder(1, "ks", "hybrid_buckets")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", int32_type, column_kind::clustering_key)
            .with_column("v_txt", utf8_type)
            .set_compressor_params(compression_parameters(compression_parameters::algorithm::lz4))
            .build();

        const sstring filler(400, 'a');
        auto make = [&] (sstable_version_types v, int n_part, int n_rows) {
            utils::chunked_vector<mutation> muts;
            for (int part = 0; part < n_part; ++part) {
                auto pk = partition_key::from_single_value(
                        *s, utf8_type->decompose(sstring(format("key{:05d}", part))));
                mutation m(s, pk);
                for (int r = 0; r < n_rows; ++r) {
                    auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
                    auto& cdef = *s->get_column_definition(to_bytes("v_txt"));
                    m.set_clustered_cell(ck, cdef, atomic_cell::make_live(
                            *cdef.type, 1000 + part, utf8_type->decompose(filler)));
                }
                muts.push_back(std::move(m));
            }
            std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
                return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
            });
            return make_sstable_containing(env.make_sstable(s, v), std::move(muts)).get();
        };

        constexpr int n_part = 20;
        constexpr int native_rows = 50;
        std::vector<shared_sstable> native;
        for (int i = 0; i < 3; ++i) {
            native.push_back(make(sstable_version_types::me, n_part, native_rows));
        }
        const auto native_ondisk = native[0]->ondisk_data_size();

        // The point of the fixture is a `pq` sstable that occupies *the same disk space* as the
        // native ones. Two sstables of equal on-disk size belong in one bucket whatever wrote them,
        // so if they split, the only possible cause is the unit -- which is precisely the defect.
        //
        // The row count has to be derived rather than hardcoded, because the two formats are not
        // remotely comparable per row here: LZ4 gets ~39x on this payload and Parquet, which
        // dictionary-encodes a column of one repeated value, gets over 400x. So probe for Parquet's
        // bytes-per-row and scale up to match. Deriving it also means the fixture survives a change
        // in either codec's effectiveness, which a hardcoded multiplier would not.
        auto probe = make(sstable_version_types::pq, n_part, native_rows);
        const double pq_bytes_per_row = double(probe->ondisk_data_size()) / (n_part * native_rows);
        const int pq_rows = std::max(1, int(double(native_ondisk) / pq_bytes_per_row / n_part));
        auto pq = make(sstable_version_types::pq, n_part, pq_rows);

        // Fixture preconditions. Both must hold for the assertions below to mean anything, so they
        // are checked rather than assumed: the units must genuinely diverge on the native side, must
        // coincide on the pq side, and the two files must land within a bucket-width of each other
        // on disk.
        BOOST_REQUIRE_GT(native[0]->data_size(), native_ondisk * 2);
        BOOST_REQUIRE_EQUAL(pq->data_size(), pq->ondisk_data_size());
        const double ondisk_ratio = double(pq->ondisk_data_size()) / double(native_ondisk);
        BOOST_REQUIRE_MESSAGE(ondisk_ratio > 0.6 && ondisk_ratio < 1.4,
                seastar::format("fixture failed to match on-disk sizes: pq {} vs native {} ({:.2f}x)",
                                pq->ondisk_data_size(), native_ondisk, ondisk_ratio));
        // ... and on `data_size()` they must be far enough apart that the old behaviour splits
        // them, or this would pass either way.
        const double data_ratio = double(pq->data_size()) / double(native[0]->data_size());
        BOOST_REQUIRE_MESSAGE(data_ratio < 0.5,
                seastar::format("fixture would not discriminate: data_size ratio {:.3f}", data_ratio));

        compaction::size_tiered_compaction_strategy_options opts;

        // All-native: one bucket, and this path must keep using `data_size()` -- every existing
        // cluster's bucketing depends on it and none of them has a `pq` sstable to trip over.
        auto native_buckets = compaction::size_tiered_compaction_strategy::get_buckets(native, opts);
        BOOST_REQUIRE_EQUAL(native_buckets.size(), 1u);

        // Mixed: equal on disk, so one bucket.
        auto mixed = native;
        mixed.push_back(pq);
        auto mixed_buckets = compaction::size_tiered_compaction_strategy::get_buckets(mixed, opts);
        BOOST_REQUIRE_EQUAL(mixed_buckets.size(), 1u);
        BOOST_REQUIRE_EQUAL(mixed_buckets[0].size(), mixed.size());
    }).get();
}

// Under TWCS, 'hybrid' means the same thing as 'parquet': the whole table.
//
// Hybrid tiering exists to keep Parquet out of the levels that get rewritten, since re-encoding and
// recompressing a Parquet run is the expensive thing this format does. TWCS has no such levels -- a
// window is compacted once and then closed -- so there is nothing for the criteria to protect and no
// reason to leave part of a TWCS table in the row format.
//
// This is asserted on the rule rather than on an end-to-end conversion because the rule has three
// callers -- compaction, memtable flush and streaming -- and the failure it guards against is them
// disagreeing. A table whose flushes and compactions answer differently never converges: one keeps
// adding files in the format the other keeps converting away from.
SEASTAR_THREAD_TEST_CASE(test_twcs_hybrid_is_parquet_for_the_whole_table) {
    auto build = [] (storage_format_type fmt, compaction::compaction_strategy_type cs) {
        auto b = schema_builder(1, "ks", "fmt_tbl")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("ck", timestamp_type, column_kind::clustering_key)
            .with_column("v", int32_type);
        b.set_storage_format(fmt);
        b.set_compaction_strategy(cs);
        return b.build();
    };
    using ct = compaction::compaction_strategy_type;
    const auto unconditional = [] (schema_ptr s) {
        return sstables::parquet::writes_parquet_unconditionally(*s);
    };

    // The change: hybrid + TWCS is now unconditional, where it used to go through C1/C5/C6.
    BOOST_REQUIRE(unconditional(build(storage_format_type::hybrid, ct::time_window)));

    // Hybrid under a size-tiered strategy still decides per compaction -- that is what hybrid is
    // for, and ICS does have levels that get rewritten.
    BOOST_REQUIRE(!unconditional(build(storage_format_type::hybrid, ct::incremental)));

    // The explicit settings are unchanged by all of this, under either strategy.
    BOOST_REQUIRE(unconditional(build(storage_format_type::parquet, ct::time_window)));
    BOOST_REQUIRE(unconditional(build(storage_format_type::parquet, ct::incremental)));
    BOOST_REQUIRE(!unconditional(build(storage_format_type::sstable, ct::time_window)));
    BOOST_REQUIRE(!unconditional(build(storage_format_type::sstable, ct::incremental)));
}

// An unknown sstable version must be an error, not a silently skipped file.
//
// This is the primitive that two different downgrade-safety paths rest on, which is why it is worth a
// test of its own rather than being left implicit:
//
//   * **Local storage** puts the version in the filename, so `parse_path()` fails on an unrecognised
//     prefix and `sstable_directory` turns that into a malformed-sstable exception that aborts boot.
//     Observed directly: a file with an unknown version prefix planted in a live table directory made
//     the node exit with "malformed sstable error (aborting): invalid version" (design doc 10.9).
//   * **Object storage** has no filename -- the key is `sstables/<uuid>/Data.db` -- so the version
//     comes from the `version` column of `system.sstables_registry`, and
//     `system_keyspace::sstables_registry_list()` calls the same `version_from_string()` while
//     scanning it. That throw propagates through `table_populator::start()`, which logs and rethrows
//     everything except `compaction_stopped_exception`, so population fails.
//
// The end-to-end object-storage case has *not* been observed: the registry is not queryable through
// CQL ("unconfigured table sstables_registry"), so a bogus version cannot be planted the way the
// local file could be. What is asserted here is the shared primitive -- if this ever started
// returning a default instead of throwing, both paths would degrade from "refuses to start" to
// "silently mis-reads", and that is the difference between a safe downgrade and data loss.
SEASTAR_THREAD_TEST_CASE(test_unknown_sstable_version_is_rejected) {
    // Every version this build knows must round-trip, `pq` included -- otherwise the negative case
    // below could pass simply because the map is empty.
    for (const char* v : {"ka", "la", "mc", "md", "me", "ms", "mt", "pq"}) {
        auto parsed = sstables::version_from_string(v);
        BOOST_REQUIRE_EQUAL(fmt::to_string(parsed), std::string(v));
    }
    // And anything else throws. "zz" stands in for what `pq` looks like to a binary that predates it.
    for (const char* bad : {"zz", "pqq", "p", "", "PQ"}) {
        BOOST_REQUIRE_THROW(sstables::version_from_string(bad), std::out_of_range);
    }
}

// The parsed-footer cache (design doc 10.4l) must be invisible to a reader except in the metrics.
// Two things have to hold, and the second is the one that can silently break: a dropped entry has
// to be re-parsed rather than read as an empty footer, and the entry has to stay immutable after
// publication -- a reader that materialised a row group *into* the shared entry would both grow it
// without bound over a scan and hand another reader half-decoded state.
SEASTAR_THREAD_TEST_CASE(test_pq_footer_cache_is_transparent_across_reclaim) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 16, 10);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto stats_of = [] { return sstables::parquet::footer_cache_stats_local(); };

        // make_sstable_containing() validates by reading, which has already populated the entry.
        // Drop it so that the first counted read below is a miss.
        sstables::test(sst).reclaim_memory_from_components();
        BOOST_REQUIRE(!sst->pq_footer_cache());

        const auto before_first = stats_of();
        auto first = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(first.size(), expected.size());
        BOOST_REQUIRE_EQUAL(stats_of().misses - before_first.misses, 1u);
        BOOST_REQUIRE_EQUAL(stats_of().populations - before_first.populations, 1u);

        auto entry = sst->pq_footer_cache();
        BOOST_REQUIRE(entry);
        const size_t entry_bytes = entry->memory_size();
        // Measured, not assumed: an entry that reported zero would be reclaim-invisible, which is
        // the failure mode that makes an evictable cache un-evictable.
        BOOST_REQUIRE_GT(entry_bytes, 0u);
        BOOST_REQUIRE_EQUAL(stats_of().bytes, entry_bytes);

        // A second read hits, and returns the same data.
        const auto before_second = stats_of();
        auto second = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(stats_of().hits - before_second.hits, 1u);
        BOOST_REQUIRE_EQUAL(stats_of().misses - before_second.misses, 0u);
        BOOST_REQUIRE_EQUAL(second.size(), expected.size());
        for (size_t i = 0; i < second.size(); ++i) {
            assert_that(second[i]).is_equal_to(expected[i]);
        }
        // The hit did not mutate the entry: same object, same size.
        BOOST_REQUIRE(sst->pq_footer_cache().get() == entry.get());
        BOOST_REQUIRE_EQUAL(entry->memory_size(), entry_bytes);

        // Now what the reclaimer does. This is the same call sstables_manager makes when
        // _total_reclaimable_memory crosses components_memory_reclaim_threshold.
        const auto before_evict = stats_of();
        const size_t reclaimed = sstables::test(sst).reclaim_memory_from_components();
        BOOST_REQUIRE_GE(reclaimed, entry_bytes);
        BOOST_REQUIRE(!sst->pq_footer_cache());
        BOOST_REQUIRE_EQUAL(stats_of().evictions - before_evict.evictions, 1u);
        BOOST_REQUIRE_EQUAL(stats_of().bytes, before_evict.bytes - entry_bytes);
        // Nothing is owed to the reload fiber for it: the next read re-parses it. Reclaiming the
        // bloom filter alongside is what the remaining balance is.
        BOOST_REQUIRE_LT(sstables::test(sst).total_reclaimable_memory_size(), entry_bytes);

        // And the read after the eviction is a miss that returns identical data.
        const auto before_third = stats_of();
        auto third = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(stats_of().misses - before_third.misses, 1u);
        BOOST_REQUIRE_EQUAL(third.size(), expected.size());
        for (size_t i = 0; i < third.size(); ++i) {
            assert_that(third[i]).is_equal_to(expected[i]);
        }
        // Re-parsing an immutable file must produce a byte-identical entry.
        BOOST_REQUIRE(sst->pq_footer_cache());
        BOOST_REQUIRE_EQUAL(sst->pq_footer_cache()->memory_size(), entry_bytes);

        // Single-partition reads go through the same footer, so evicting between them must not
        // change an answer either. This is the path a point read takes.
        for (const auto& want : expected) {
            sstables::test(sst).reclaim_memory_from_components();
            auto pr = dht::partition_range::make_singular(want.decorated_key());
            auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
            auto close = deferred_close(rd);
            auto got = read_mutation_from_mutation_reader(rd).get();
            BOOST_REQUIRE(got);
            assert_that(*got).is_equal_to(want);
        }
    }).get();
}

// The reclaimer has to be able to reach the footer cache through the manager's own policy, not
// only through a direct call: the whole point of registering with the existing machinery is that
// components_memory_reclaim_threshold governs it. available_memory = 0 makes the threshold zero,
// so anything the read publishes is immediately over it.
SEASTAR_THREAD_TEST_CASE(test_pq_footer_cache_is_reclaimed_by_the_manager) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = pq_schema();
        auto muts = make_muts(s, 8, 10);
        auto expected = muts;
        auto sst = make_sstable_containing(
                env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

        auto before = sstables::parquet::footer_cache_stats_local();
        auto got = read_all(sst, s, env.make_reader_permit());
        BOOST_REQUIRE_EQUAL(got.size(), expected.size());

        // The reclaim fiber runs on the maintenance scheduling group, so this is eventual.
        REQUIRE_EVENTUALLY_EQUAL<bool>([&] { return bool(sst->pq_footer_cache()); }, false);
        BOOST_REQUIRE_GT(sstables::parquet::footer_cache_stats_local().evictions, before.evictions);

        // And the sstable still reads correctly with the reclaimer racing every read.
        for (int i = 0; i < 3; ++i) {
            auto again = read_all(sst, s, env.make_reader_permit());
            BOOST_REQUIRE_EQUAL(again.size(), expected.size());
            for (size_t j = 0; j < again.size(); ++j) {
                assert_that(again[j]).is_equal_to(expected[j]);
            }
        }
    }, {
        // Zero available memory means the reclaim threshold is zero: the cache is dropped as soon
        // as it is published.
        .available_memory = 0
    }).get();
}
