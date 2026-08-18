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

// True when the schema can be shredded without losing information: no counters,
// and no multi-cell (non-frozen) collections, which the shredder does not
// handle yet.
bool schema_is_parquet_eligible(const ::schema&);

// CQL columns, which is what C5 bounds. Deliberately not the Parquet leaf count: that is
// data-dependent (per-column deletion and TTL leaves materialise only when cells carry them),
// so it cannot be derived from a schema, and a criterion should not rest on a guess.
size_t column_count(const ::schema&);

tiering_inputs make_tiering_inputs(const std::vector<sstables::shared_sstable>& inputs,
                                   const ::schema&,
                                   const compaction_context&);

// Convenience: build the inputs and evaluate in one step.
tiering_decision decide_output_format(const std::vector<sstables::shared_sstable>& inputs,
                                      const ::schema&,
                                      const compaction_context&,
                                      const tiering_thresholds& = {});

} // namespace sstables::parquet
