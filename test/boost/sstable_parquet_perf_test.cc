/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Side-by-side timings for `pq` against the current default format, on the same
// mutations, in the same process. Covers the performance requirements in
// docs/dev/parquet-storage-format.md section 4 that were previously blocked on
// Parquet sstables existing on disk at all:
//
//   R-9   write throughput must not regress materially
//   R-10  full-scan reads should be faster
//   R-11  point reads may be slower, but bounded
//   R-13  memory must stay bounded regardless of sstable size
//
// This is a measurement harness, not an assertion suite: it prints a table and
// only fails if something is functionally wrong. Absolute numbers depend on the
// machine; the ratios are the point. Run it directly:
//
//   ./build/dev/test/boost/sstable_parquet_perf_test -- -c1 -m4G

#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/format.hh>

#include <algorithm>
#include <cstdlib>
#include "test/lib/sstable_test_env.hh"
#include "test/lib/sstable_utils.hh"

#include <map>
#include "sstables/parquet/reader.hh"
#include "schema/schema_builder.hh"
#include "sstables/sstables.hh"
#include "mutation/mutation.hh"
#include "readers/from_mutations.hh"

#include <chrono>
#include <random>
#include <string_view>

using namespace sstables;
using namespace std::chrono;

namespace {

using clk = steady_clock;

// Extra int columns appended to the schema, via PQ_PERF_EXTRA_COLS. Exists because the
// size cost of a small row group scales with *leaf count* -- every row group writes a
// column-chunk header plus statistics per leaf -- so a 5-column table cannot tell you what
// the row-group default costs a 197-column one (design doc 10.1f-prod, open question 15).
// Default 0 keeps the historical schema, so previously published numbers stay comparable.
int extra_cols() {
    if (const char* e = std::getenv("PQ_PERF_EXTRA_COLS")) {
        return std::max(0, std::atoi(e));
    }
    return 0;
}

sstring extra_col_name(int i) { return sstring(format("x_{:03d}", i)); }

schema_ptr perf_schema() {
    auto b = schema_builder(1, "ks", "perf");
    b.with_column("pk", utf8_type, column_kind::partition_key)
     .with_column("ck", int32_type, column_kind::clustering_key)
     .with_column("v_int", int32_type)
     .with_column("v_big", long_type)
     .with_column("v_dbl", double_type)
     .with_column("v_txt", utf8_type)
     .with_column("v_txt2", utf8_type);
    for (int i = 0, n = extra_cols(); i < n; ++i) {
        b.with_column(to_bytes(extra_col_name(i)), int32_type);
    }
    // Row-group size, via the same per-table property an operator would set. Needed
    // because the size cost of a small row group scales with leaf count while the
    // latency benefit does not obviously do so, and open question 15 turns on whether
    // one default can serve both shapes.
    std::map<sstring, sstring> opts;
    if (const char* e = std::getenv("PQ_PERF_RG_ROWS")) {
        opts["rows_per_row_group"] = sstring(e);
    }
    // Page size. Worth a knob because it stopped being reachable: the writer uses
    // min(page_values, row group size), and with row groups cut at 5 000 rows the 8 192
    // default never binds -- so every point read decodes a whole row group's page.
    if (const char* e = std::getenv("PQ_PERF_PAGE_ROWS")) {
        opts["page_rows"] = sstring(e);
    }
    // Anything else, spelled the way CQL spells it: PQ_PERF_PQ_OPTS="compression=lz4,dictionary=none".
    // The two knobs above predate this and stay, being the two that get swept most; this exists so
    // that measuring a *new* sub-option does not need a new env var and a rebuild of the harness.
    // Notably `compression`: the parquet arm is zstd while the row format's default is
    // LZ4WithDictsCompressor, so any CPU comparison between them that does not vary the codec is
    // measuring the codec as much as the format (design doc 10.28).
    if (const char* e = std::getenv("PQ_PERF_PQ_OPTS")) {
        std::string_view all(e);
        while (!all.empty()) {
            const auto comma = all.find(',');
            auto kv = all.substr(0, comma);
            const auto eq = kv.find('=');
            if (eq != std::string_view::npos) {
                opts[sstring(kv.substr(0, eq))] = sstring(kv.substr(eq + 1));
            }
            if (comma == std::string_view::npos) { break; }
            all = all.substr(comma + 1);
        }
    }
    if (!opts.empty()) {
        b.set_parquet_options(std::move(opts));
    }
    return b.build();
}

// Values with realistic redundancy: a small vocabulary for the text columns and
// correlated numerics. Random bytes would make every format look identical
// because nothing would compress -- the same trap section 9 calls out.
utils::chunked_vector<mutation> gen(schema_ptr s, int n_part, int n_rows) {
    static const char* WORDS[] = {"active", "pending", "closed", "archived", "error",
                                  "retry", "queued", "done"};
    std::mt19937_64 rng(42);
    utils::chunked_vector<mutation> muts;
    muts.reserve(n_part);
    for (int p = 0; p < n_part; ++p) {
        auto pk = partition_key::from_single_value(
                *s, utf8_type->decompose(sstring(format("user{:07d}", p))));
        mutation m(s, pk);
        const api::timestamp_type row_ts = 1'700'000'000'000'000LL + p;
        for (int r = 0; r < n_rows; ++r) {
            auto ck = clustering_key::from_single_value(*s, int32_type->decompose(r));
            auto put = [&] (const char* name, bytes val) {
                const auto& cd = *s->get_column_definition(to_bytes(name));
                m.set_clustered_cell(ck, cd, atomic_cell::make_live(*cd.type, row_ts, val));
            };
            put("v_int", int32_type->decompose(int32_t(rng() % 1000)));
            put("v_big", long_type->decompose(int64_t(p) * 1000 + r));
            put("v_dbl", double_type->decompose(double(rng() % 100000) / 100.0));
            put("v_txt", utf8_type->decompose(sstring(WORDS[rng() % 8])));
            put("v_txt2", utf8_type->decompose(
                    sstring(format("{}/{}/{}", WORDS[rng() % 8], p % 997, r))));
            // Low-cardinality, like the SMART counters that make real wide tables wide:
            // the point of these columns is their number, not their content.
            for (int i = 0, n = extra_cols(); i < n; ++i) {
                put(extra_col_name(i).c_str(),
                    int32_type->decompose(int32_t((p + r + i) % 64)));
            }
        }
        muts.push_back(std::move(m));
    }
    std::sort(muts.begin(), muts.end(), [] (const mutation& a, const mutation& b) {
        return a.decorated_key().less_compare(*a.schema(), b.decorated_key());
    });
    return muts;
}

struct result {
    sstring   label;
    double    write_ms = 0;
    double    scan_ms = 0;
    // Same rows as scan_ms, read through a partition range that carries explicit bounds
    // instead of query::full_partition_range. Nothing else differs -- same schema, same
    // slice, same sstable, same process -- so the gap between the two is the cost of the
    // *path* the bound selects, which is what design doc 10.26 exists to measure.
    double    bscan_ms = 0;
    size_t    bscan_rows = 0;
    double    point_us = 0;      // mean per point read
    double    point_p50 = 0, point_p95 = 0, point_p99 = 0;
    size_t    point_n = 0;
    uint64_t  bytes = 0;
    int64_t   scan_peak_kb = 0;  // live-memory high-water during the scan
    size_t    rows = 0;
};

result measure(sstables::test_env& env, schema_ptr s,
               const utils::chunked_vector<mutation>& muts,
               sstable_version_types ver, const sstring& label,
               const std::vector<size_t>& point_idx) {
    result r;
    r.label = label;

    auto copy = muts;
    auto t0 = clk::now();
    auto sst = make_sstable_containing(env.make_sstable(s, ver), std::move(copy)).get();
    r.write_ms = duration<double, std::milli>(clk::now() - t0).count();
    r.bytes = sst->ondisk_data_size();

    // Full scan. Sample live memory as we go so R-13 is measured, not assumed.
    const auto before = int64_t(memory::stats().allocated_memory());
    int64_t peak = 0;
    t0 = clk::now();
    {
        auto rd = sst->make_reader(s, env.make_reader_permit(), query::full_partition_range,
                                   s->full_slice());
        auto close = deferred_close(rd);
        while (auto m = read_mutation_from_mutation_reader(rd).get()) {
            ++r.rows;
            peak = std::max(peak, int64_t(memory::stats().allocated_memory()) - before);
        }
    }
    r.scan_ms = duration<double, std::milli>(clk::now() - t0).count();
    r.scan_peak_kb = peak / 1024;

    // The same scan again, over a bounded range that spans every partition in the file.
    // `muts` is sorted by decorated key, so [front, back] is the whole ring segment the
    // sstable holds and this reads exactly the rows the unbounded scan just read.
    //
    // It exists because a token-restricted CQL scan -- `WHERE token(pk) >= a AND token(pk) < b`,
    // and every page of a paged range query after the first -- arrives at the sstable reader as
    // a *bounded* range, and pq_reader::next_window() forks on precisely that: a bounded read
    // takes the point-read machinery (512-row windows, per-window page fetch and decode) while
    // an unbounded one streams whole row groups in 16 384-row windows. Any scan figure that does
    // not say which of the two it measured is unreadable, which is how 10.4c and 10.25 ended up
    // contradicting each other.
    {
        auto pr = dht::partition_range::make({muts.front().decorated_key(), true},
                                             {muts.back().decorated_key(), true});
        t0 = clk::now();
        auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
        auto close = deferred_close(rd);
        while (auto m = read_mutation_from_mutation_reader(rd).get()) {
            ++r.bscan_rows;
        }
        r.bscan_ms = duration<double, std::milli>(clk::now() - t0).count();
    }

    // Point reads, each on a fresh reader so nothing is carried over.
    //
    // Timed individually rather than as one bulk average. A mean over a handful of reads
    // hides the distribution, and the distribution is the interesting part of a point-read
    // number: the cost here is dominated by per-read setup -- opening a row group and
    // decompressing a dictionary page -- so a few cheap reads can flatter the mean badly.
    std::vector<double> samples;
    samples.reserve(point_idx.size());
    for (size_t i : point_idx) {
        auto pr = dht::partition_range::make_singular(muts[i].decorated_key());
        auto t1 = clk::now();
        auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
        auto close = deferred_close(rd);
        auto m = read_mutation_from_mutation_reader(rd).get();
        samples.push_back(duration<double, std::micro>(clk::now() - t1).count());
        if (!m) { BOOST_FAIL("point read returned nothing"); }
    }
    double sum = 0;
    for (double v : samples) { sum += v; }
    r.point_us = sum / double(samples.size());
    r.point_n = samples.size();
    std::sort(samples.begin(), samples.end());
    auto pct = [&] (double q) {
        return samples[std::min(samples.size() - 1,
                                size_t(q * double(samples.size())))];
    };
    r.point_p50 = pct(0.50);
    r.point_p95 = pct(0.95);
    r.point_p99 = pct(0.99);

    return r;
}

} // namespace

SEASTAR_THREAD_TEST_CASE(perf_pq_vs_default) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = perf_schema();

        // Sized for CI by default, and scaled up by env var for measurement.
        //
        // This test is enrolled in the standard boost suite (configure.py), so it runs on every
        // CI invocation -- and at the sizes every figure in section 10.4 was measured with
        // (20 000 partitions, 10 000 point reads) it is minutes of runtime for numbers nobody
        // reads on a shared runner. It asserts nothing about latency, so it cannot fail on a
        // loaded machine; it can only be slow. Small default, big when asked:
        //
        //   PQ_PERF_PARTITIONS=20000 PQ_PERF_POINTS=10000 ./sstable_parquet_perf_test -c1 -m2G
        //
        // which is what width_curve.sh and any figure quoted in section 10.4 must use. The
        // small default is a smoke test of both writers and the point-read path; it is not a
        // measurement, and its numbers should not be quoted.
        const int n_part = [] {
            if (const char* e = std::getenv("PQ_PERF_PARTITIONS")) {
                return int(std::max(1L, std::atol(e)));
            }
            return 2'000;
        }();
        const int n_rows = 5;
        auto muts = gen(s, n_part, n_rows);

        // A fixed spread of partitions, same set for both formats.
        // Uniformly random distinct partitions, not a stride.
        //
        // This used to be 50 evenly-spaced indices, which is both too small a sample to
        // say anything about a latency distribution and too friendly an access pattern:
        // a stride walks the partition index and summary in order, so it gets locality
        // that a real point-read workload does not have. Both formats get the identical
        // key list, so the comparison stays fair either way -- but the absolute numbers
        // were optimistic for both.
        const size_t n_points = [] {
            if (const char* e = std::getenv("PQ_PERF_POINTS")) {
                return size_t(std::max(1L, std::atol(e)));
            }
            return size_t(1000);   // see PQ_PERF_PARTITIONS above
        }();
        std::vector<size_t> point_idx(muts.size());
        for (size_t i = 0; i < muts.size(); ++i) { point_idx[i] = i; }
        std::shuffle(point_idx.begin(), point_idx.end(), std::mt19937_64(12345));
        if (point_idx.size() > n_points) { point_idx.resize(n_points); }

        auto def = measure(env, s, muts, sstables::get_highest_sstable_version(), "default (me)", point_idx);
        auto pq  = measure(env, s, muts, sstable_version_types::pq, "pq (parquet)", point_idx);

        BOOST_REQUIRE_EQUAL(def.rows, size_t(n_part));
        BOOST_REQUIRE_EQUAL(pq.rows, size_t(n_part));

        auto line = [] (const result& r) {
            std::printf("  %-14s  %9.0f  %9.0f  %9.0f  %11.1f  %12llu  %10lld\n",
                        r.label.c_str(), r.write_ms, r.scan_ms, r.bscan_ms, r.point_us,
                        (unsigned long long)r.bytes, (long long)r.scan_peak_kb);
        };
        std::printf("\n=== pq vs default: %d partitions x %d rows = %d rows ===\n",
                    n_part, n_rows, n_part * n_rows);
        std::printf("  %-14s  %9s  %9s  %9s  %11s  %12s  %10s\n",
                    "format", "write ms", "scan ms", "bscan ms", "point us", "data bytes",
                    "scan kB");
        line(def);
        line(pq);
        std::printf("\n  point-read distribution over %zu uniformly random partitions:\n",
                    def.point_n);
        std::printf("  %-14s  %10s  %10s  %10s  %10s\n",
                    "format", "mean us", "p50 us", "p95 us", "p99 us");
        for (const auto& r : {def, pq}) {
            std::printf("  %-14s  %10.1f  %10.1f  %10.1f  %10.1f\n",
                        r.label.c_str(), r.point_us, r.point_p50, r.point_p95, r.point_p99);
        }
        // Where the pq point read went, when PQ_READER_PROFILE is set. Printed next to the
        // ratios so the attribution and the number being attributed are read together.
        std::printf("%s", sstables::parquet::reader_profile_report().c_str());
        std::printf("  point ratios: mean %.1fx  p50 %.1fx  p95 %.1fx  p99 %.1fx\n",
                    pq.point_us / def.point_us, pq.point_p50 / def.point_p50,
                    pq.point_p95 / def.point_p95, pq.point_p99 / def.point_p99);
        std::printf("  ratios (pq/default): write %.2fx  scan %.2fx  bscan %.2fx  point %.2fx"
                    "  size %.3fx\n",
                    pq.write_ms / def.write_ms, pq.scan_ms / def.scan_ms,
                    pq.bscan_ms / def.bscan_ms,
                    pq.point_us / def.point_us, double(pq.bytes) / double(def.bytes));
        // What the bound costs each format on its own terms. The row format's index makes a
        // bounded range no harder than an unbounded one; pq's fork makes it a different reader.
        std::printf("  bounded/unbounded, same rows: default %.2fx  pq %.2fx"
                    "  (rows %zu vs %zu)\n\n",
                    def.bscan_ms / def.scan_ms, pq.bscan_ms / pq.scan_ms,
                    pq.rows, pq.bscan_rows);
    }).get();
}

// R-13 specifically: memory during a scan must not grow with the sstable. Two
// sizes, same schema -- if peak scan memory tracks the file, the reader is
// materialising it.
SEASTAR_THREAD_TEST_CASE(perf_pq_scan_memory_scaling) {
    sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = perf_schema();
        std::printf("\n=== R-13: scan memory vs sstable size (pq) ===\n");
        std::printf("  %10s  %12s  %12s  %10s\n", "rows", "data bytes", "scan peak kB", "point us");

        int64_t small_peak = 0, large_peak = 0;
        for (int n_part : {4'000, 32'000}) {
            auto muts = gen(s, n_part, 5);
            auto sst = make_sstable_containing(
                    env.make_sstable(s, sstable_version_types::pq), std::move(muts)).get();

            const auto before = int64_t(memory::stats().allocated_memory());
            int64_t peak = 0;
            size_t n = 0;
            {
                auto rd = sst->make_reader(s, env.make_reader_permit(),
                                           query::full_partition_range, s->full_slice());
                auto close = deferred_close(rd);
                while (auto m = read_mutation_from_mutation_reader(rd).get()) {
                    ++n;
                    peak = std::max(peak, int64_t(memory::stats().allocated_memory()) - before);
                }
            }
            BOOST_REQUIRE_EQUAL(n, size_t(n_part));

            // Point-read cost at each size. If it tracks the file rather than the
            // partition, the reader is I/O bound on the row-group read and the
            // OffsetIndex-driven per-page read is the fix; if it is flat, it is not.
            auto muts2 = gen(s, n_part, 5);
            double pt = 0;
            {
                auto t0 = clk::now();
                const int probes = 20;
                for (int i = 0; i < probes; ++i) {
                    auto pr = dht::partition_range::make_singular(
                            muts2[size_t(i) * (muts2.size() / probes)].decorated_key());
                    auto rd = sst->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
                    auto close = deferred_close(rd);
                    auto m = read_mutation_from_mutation_reader(rd).get();
                    if (!m) { BOOST_FAIL("point read missed"); }
                }
                pt = duration<double, std::micro>(clk::now() - t0).count() / probes;
            }
            std::printf("  %10d  %12llu  %12lld  %10.1f\n", n_part * 5,
                        (unsigned long long)sst->ondisk_data_size(), (long long)(peak / 1024), pt);
            (n_part == 4'000 ? small_peak : large_peak) = peak;
        }
        if (small_peak > 0) {
            const double growth = double(large_peak) / double(small_peak);
            std::printf("  8x the rows cost %.2fx the peak scan memory "
                        "(bounded would be ~1x)\n\n", growth);
            // R-13 is a requirement, so assert it rather than print it. This block measured the
            // ratio and then discarded it, which means a reader that started materialising the
            // whole file would have gone green and simply printed 8.0x -- exactly the regression
            // the test exists to catch.
            //
            // The bound is 3x, not 1.5x. A bounded reader is ~1x, but "peak allocated during the
            // scan" also picks up per-partition churn and allocator granularity at two quite
            // different file sizes, so the floor is noisy at these sizes. 3x is comfortably above
            // that noise and far below the ~8x that tracking the file would produce, which is the
            // only failure mode worth catching here.
            BOOST_REQUIRE_MESSAGE(growth < 3.0,
                    seastar::format("R-13: peak scan memory grew {:.2f}x for 8x the rows "
                                    "({} -> {} bytes); a bounded reader is ~1x, so the reader is "
                                    "materialising the sstable", growth, small_peak, large_peak));
        }
    }).get();
}
