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
// Trimmed to three criteria on 2026-08-18: C1 (bottom tier), C5 (width) and C6 (measured
// gain). Four went, and each for a reason the measurements supplied rather than for tidiness:
//
//   C2, a minimum output size, is subsumed by C6. A file too small to pay is exactly one whose
//     *measured* gain is bad -- NOAA ISD-Lite at 5 000 rows measured 111.7 % of the SSTable,
//     a gain of -0.117, which fails C6's 0.15 floor on its own. The crossover C2 encoded is the
//     crossover C6 discovers, and C6 discovers it per table instead of assuming one byte count
//     fits every row width (10.1f-c2).
//   C4, a maximum garbage fraction, had no measurement behind its 0.10 and guarded against one
//     wasted encode: a bottom-tier output about to be tombstone-GC'd would be re-evaluated by
//     this same policy on the rewrite. A reasoned round number is the category that has been
//     wrong three times in this project.
//   C7, a read-pattern gate, cannot be evaluated at all -- Scylla has no counter separating
//     point reads from scans (6.2a). It is kept as a design note there, not as dead code here.
//     C5's leaf ceiling is the crude stand-in, and it is derived from measurement (10.4e).
//   C3 (a minimum data age) was removed earlier the same day.
//
// Fewer criteria also means fewer thresholds nobody has measured, which is the failure mode
// this decision function kept hitting.
//
// C3 detail: It gated on
// now - max_cell_timestamp, which is write time rather than settle time: a backfill
// carrying historical timestamps passed it while being brand new on disk, and a cold
// table with one recent write failed it. C1 answers the question C3 was proxying for --
// "is anything going to rewrite this again" -- structurally rather than by elapsed time,
// and C4 catches the churn that C3 was meant to smell. The remaining criteria keep their
// original numbers so that every reference to C4-C7 elsewhere stays valid.
//
// See docs/dev/parquet-storage-format.md section 6.3.

#include <cstdint>
#include <optional>
#include <string>

namespace sstables::parquet {

// Every threshold is a per-table knob (design doc section 8.3). Defaults are the
// ones argued for there.
struct tiering_thresholds {
    // C5. Was 2000, i.e. effectively unbounded -- no CQL table reaches it, so the
    // criterion never fired. Now derived from a latency budget, because a Parquet point
    // read costs a page locate-and-decode in *every* column chunk it projects, and that
    // cost is linear in leaf count at ~90 us per leaf (design doc 10.4e): 10 leaves is
    // 1.15 ms, 200 leaves is 18.3 ms, against 26-136 us for the native format.
    //
    // 128 leaves is a p50 point read of roughly 11.5 ms. It admits every dataset in the
    // corpus that saves meaningfully -- ClickBench at 110 leaves saves 40 % -- and
    // excludes the one that does not: Backblaze at 200 leaves saves 4 % and point-reads
    // at 134x. So on this corpus the criterion costs nothing it should not cost.
    //
    // This is a stand-in for C7, which is the criterion that ought to make this call and
    // cannot, because Scylla has no counter distinguishing point reads from scans (6.2a).
    // Refusing on width alone is strictly cruder: it declines a wide table that is only
    // ever scanned, and that is the case where Parquet is fastest. That is the trade until
    // the read path can say otherwise.
    size_t   max_leaf_columns   = 128;
    double   min_gain_ratio     = 0.15;           // C6: >= 15 % saved
};

// What the caller must be able to say about a candidate output. Anything the
// caller cannot determine is left unset, and the policy treats "unknown" as
// "not proven safe" rather than guessing.
struct tiering_inputs {
    // C1: is this output in the largest size tier / max level? Operationally,
    // "expected remaining rewrites <= 1".
    bool bottom_tier = false;
    // C5
    bool   schema_eligible = true;      // no counters, no unsupported types
    size_t estimated_leaf_columns = 0;  // after folding
    // C6: measured, not guessed. Fraction saved vs the table's current
    // compressor, e.g. 0.42 for "42 % smaller". Unset means not measured yet.
    std::optional<double> predicted_gain;
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
    // C6. Filled from the estimator; unset means "not measured", which the
    // policy treats as a rejection.
    std::optional<double> predicted_gain;
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


tiering_decision evaluate_tiering(const tiering_inputs&,
                                  const tiering_thresholds& = {});

} // namespace sstables::parquet
