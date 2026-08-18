/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for the hybrid tiering policy.
//
// The policy is a conjunction of seven criteria, so the useful tests are: an
// output that satisfies all of them is accepted, and relaxing exactly one
// criterion at a time rejects it for the right reason. Anything less and a
// criterion could silently stop being enforced.

#include "sstables/parquet/tiering_policy.hh"

#include <cstdio>
#include <string>
#include <vector>

using namespace sstables::parquet;

namespace {

size_t g_fail = 0, g_cases = 0;

// The canonical accept: a large, old, clean, measured bottom-tier output.
tiering_inputs good() {
    tiering_inputs in;
    in.bottom_tier = true;
    in.estimated_output_bytes = 512ull << 20;
    in.data_age = std::chrono::seconds{48 * 3600};
    in.garbage_fraction = 0.01;
    in.schema_eligible = true;
    in.estimated_leaf_columns = 110;   // ClickBench's width: admitted, and saves 40 %
    in.predicted_gain = 0.42;
    return in;
}

void expect(bool cond, const std::string& what) {
    ++g_cases;
    if (!cond) { ++g_fail; std::printf("  FAIL %s\n", what.c_str()); }
}

void expect_reject(const tiering_inputs& in, const std::string& what,
                   const std::string& reason_contains,
                   tiering_mode mode = tiering_mode::hybrid) {
    ++g_cases;
    auto d = evaluate_tiering(in, {}, mode);
    if (d.parquet()) {
        ++g_fail;
        std::printf("  FAIL %s: accepted when it should have been rejected\n", what.c_str());
        return;
    }
    if (d.reason.find(reason_contains) == std::string::npos) {
        ++g_fail;
        std::printf("  FAIL %s: rejected for the wrong reason: %s\n", what.c_str(), d.reason.c_str());
    }
}

} // namespace

int main() {
    // Baseline: everything satisfied.
    {
        auto d = evaluate_tiering(good());
        expect(d.parquet(), "canonical good output is accepted");
        expect(d.reason.find("predicted gain") != std::string::npos,
               "acceptance explains itself");
    }

    // C1 -- position
    { auto in = good(); in.bottom_tier = false;
      expect_reject(in, "C1 non-bottom-tier", "bottom-tier"); }

    // C2 -- size. Exactly at the threshold must pass; one byte under must not.
    { auto in = good(); in.estimated_output_bytes = (256ull << 20) - 1;
      expect_reject(in, "C2 one byte under", "below the"); }
    { auto in = good(); in.estimated_output_bytes = 256ull << 20;
      expect(evaluate_tiering(in).parquet(), "C2 exactly at threshold accepted"); }

    // C3 -- age
    { auto in = good(); in.data_age = std::chrono::seconds{3600};
      expect_reject(in, "C3 too young", "younger than"); }
    { auto in = good(); in.data_age = std::chrono::seconds{24 * 3600};
      expect(evaluate_tiering(in).parquet(), "C3 exactly at threshold accepted"); }

    // C4 -- garbage
    { auto in = good(); in.garbage_fraction = 0.5;
      expect_reject(in, "C4 too much garbage", "garbage fraction"); }
    { auto in = good(); in.garbage_fraction = 0.10;
      expect(evaluate_tiering(in).parquet(), "C4 exactly at threshold accepted"); }

    // C5 -- schema
    { auto in = good(); in.schema_eligible = false;
      expect_reject(in, "C5 ineligible schema", "not eligible"); }
    // 200 leaves, the width at which point reads measured 134x native (design doc
    // 10.4e) and the default ceiling of 128 exists to refuse. Was 5 000, which no CQL
    // table can reach -- it proved the comparison worked without pinning a real boundary.
    { auto in = good(); in.estimated_leaf_columns = 200;
      expect_reject(in, "C5 too many leaves", "leaf columns"); }

    // C6 -- the load-bearing criterion. Unmeasured must be a rejection, not an
    // optimistic guess: that is the whole point of it existing.
    { auto in = good(); in.predicted_gain.reset();
      expect_reject(in, "C6 unmeasured", "no measured gain"); }
    { auto in = good(); in.predicted_gain = 0.05;
      expect_reject(in, "C6 gain too small", "predicted gain"); }
    { auto in = good(); in.predicted_gain = 0.15;
      expect(evaluate_tiering(in).parquet(), "C6 exactly at threshold accepted"); }

    // C7 -- only consulted in adaptive mode.
    { auto in = good(); in.point_read_dominated = true;
      expect(evaluate_tiering(in, {}, tiering_mode::hybrid).parquet(),
             "C7 ignored in hybrid mode");
      expect_reject(in, "C7 honoured in adaptive mode", "point-read dominated",
                    tiering_mode::adaptive); }

    // Custom thresholds must actually be honoured, not just the defaults.
    {
        tiering_thresholds th;
        th.min_output_bytes = 1ull << 20;
        th.min_gain_ratio = 0.80;
        auto in = good();
        in.estimated_output_bytes = 2ull << 20;   // under the default, over this one
        ++g_cases;
        auto d = evaluate_tiering(in, th);
        if (d.parquet()) { ++g_fail; std::printf("  FAIL custom min_gain_ratio not applied\n"); }
        in.predicted_gain = 0.9;
        expect(evaluate_tiering(in, th).parquet(), "custom thresholds accepted");
    }

    // A rejection must never be silent.
    { auto in = good(); in.bottom_tier = false;
      expect(!evaluate_tiering(in).reason.empty(), "rejections carry a reason"); }

    std::printf("tiering policy: %zu cases, %zu failures\n", g_cases, g_fail);
    std::printf("%s\n", g_fail ? "TIERING FAIL" : "TIERING PASS");
    return g_fail ? 1 : 0;
}
