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
// C3 (a minimum data age) was removed on 2026-08-18. It gated on
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
    // C2. Was 256 MiB, justified as ">= 4 row groups at 64 MiB" -- reasoning about the
    // byte budget when it was the thing that cut row groups. Row groups are now cut at
    // 5 000 rows (10.4c), so four of them is ~200 kB on a narrow table, not 256 MB. The
    // old value was three orders of magnitude too high and meant hybrid mode never fired:
    // real compaction outputs on a 298 MB table measured 13-26 MB, because compaction runs
    // per compaction group and this floor applies to one output, not to the table.
    //
    // 256 KiB is where the measurement puts the crossover (10.1f-c2). On NOAA ISD-Lite:
    // 5 000 rows -> Parquet is 12 % *bigger*; 20 000 rows -> 38 % smaller; and it asymptotes
    // at 49 % by 300 000. 5 000 rows is exactly one row group, so the original "four row
    // groups" instinct was right and only its arithmetic was stale.
    uint64_t min_output_bytes   = 256ull << 10;
    double   max_garbage_fraction = 0.10;         // C4
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
    // C2
    uint64_t estimated_output_bytes = 0;
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
