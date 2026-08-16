/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Builds the tiering_inputs for a candidate compaction from things the
// compaction layer already knows.
//
// Kept apart from tiering_policy.hh on purpose: the policy stays a pure function
// over plain numbers with no Scylla dependencies, so it can be unit-tested
// exhaustively. This is the only file that has to know about sstables and
// schemas, and all it does is fill in a struct.

#include "sstables/parquet/tiering_policy.hh"
#include "sstables/shared_sstable.hh"
#include "schema/schema_fwd.hh"

#include <optional>
#include <vector>

namespace sstables::parquet {

// The parts the compaction layer must supply because only it can know them.
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

// True when the schema can be shredded without losing information: no counters,
// and no multi-cell (non-frozen) collections, which the shredder does not
// handle yet.
bool schema_is_parquet_eligible(const ::schema&);

// Number of Parquet leaves the schema will produce after row-folding: one per
// column, plus __ts, plus the two sparse exception leaves in the worst case.
size_t estimated_leaf_columns(const ::schema&);

tiering_inputs make_tiering_inputs(const std::vector<sstables::shared_sstable>& inputs,
                                   const ::schema&,
                                   const compaction_context&);

// Convenience: build the inputs and evaluate in one step.
tiering_decision decide_output_format(const std::vector<sstables::shared_sstable>& inputs,
                                      const ::schema&,
                                      const compaction_context&,
                                      const tiering_thresholds& = {},
                                      tiering_mode = tiering_mode::hybrid);

} // namespace sstables::parquet
