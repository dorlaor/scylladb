/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Hybrid tiering: decides, for one candidate compaction output, whether to write
// it as Parquet or leave it in the native format.
//
// Parquet is bad at small files (fixed metadata cost), bad at churn (any merge
// re-encodes and recompresses the whole thing), and good at large, stable,
// scan-read data. The bottom tier of a large table is all three of the good
// things and none of the bad, so that is what this aims at.
//
// Deliberately a pure function over a plain inputs struct: no compaction
// manager, no schema, no I/O. That keeps every criterion testable on its own and
// keeps the policy honest -- if a criterion cannot be expressed as a number the
// caller can supply, it does not belong here.
//
// See docs/dev/parquet-storage-format.md section 6.3.

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

namespace sstables::parquet {

// Every threshold is a per-table knob (design doc section 8.3). Defaults are the
// ones argued for there.
struct tiering_thresholds {
    uint64_t min_output_bytes   = 256ull << 20;   // C2: >= 4 row groups at 64 MiB
    std::chrono::seconds min_data_age{24 * 3600}; // C3
    double   max_garbage_fraction = 0.10;         // C4
    size_t   max_leaf_columns   = 2000;           // C5
    double   min_gain_ratio     = 0.15;           // C6: >= 15 % saved
};

// What the caller must be able to say about a candidate output. Anything the
// caller cannot determine is left unset, and the policy treats "unknown" as
// "not proven safe" rather than guessing.
struct tiering_inputs {
    // C1: is this output in the largest size tier / max level? Operationally,
    // "expected remaining rewrites <= 1".
    bool bottom_tier = false;
    // C2
    uint64_t estimated_output_bytes = 0;
    // C3: age of the newest data in the output.
    std::chrono::seconds data_age{0};
    // C4: droppable tombstones + shadowed cells, as a fraction of the output.
    double garbage_fraction = 0.0;
    // C5
    bool   schema_eligible = true;      // no counters, no unsupported types
    size_t estimated_leaf_columns = 0;  // after folding
    // C6: measured, not guessed. Fraction saved vs the table's current
    // compressor, e.g. 0.42 for "42 % smaller". Unset means not measured yet.
    std::optional<double> predicted_gain;
    // C7: only consulted in 'auto' mode.
    std::optional<bool> point_read_dominated;
};

// The parts the compaction layer must supply because only it can know them.
//
// Lives in this header rather than tiering_context.hh so that
// compaction_descriptor can carry one: this file has no Scylla dependencies at
// all, so including it from the compaction layer costs nothing.
struct compaction_context {
    // C1. Only the strategy knows its own tiering, so it says so rather than
    // this code guessing from a level number that means different things to
    // ICS, LCS and STCS.
    bool bottom_tier = false;
    // C4. The compaction code already computes this while estimating output size.
    double estimated_droppable_tombstone_ratio = 0.0;
    // C6. Filled from the estimator; unset means "not measured", which the
    // policy treats as a rejection.
    std::optional<double> predicted_gain;
    // C7, adaptive mode only.
    std::optional<bool> point_read_dominated;
};

enum class tiering_verdict { use_parquet, use_native };

// Why, in words, so the decision can be logged and explained to an operator
// rather than being an unexplained bool.
struct tiering_decision {
    tiering_verdict verdict = tiering_verdict::use_native;
    std::string reason;

    bool parquet() const { return verdict == tiering_verdict::use_parquet; }
    explicit operator bool() const { return parquet(); }
};

// `adaptive` corresponds to storage_format = 'auto', which additionally consults
// C7. In plain 'hybrid' mode the read pattern is ignored.
enum class tiering_mode { hybrid, adaptive };

tiering_decision evaluate_tiering(const tiering_inputs&,
                                  const tiering_thresholds& = {},
                                  tiering_mode = tiering_mode::hybrid);

} // namespace sstables::parquet
