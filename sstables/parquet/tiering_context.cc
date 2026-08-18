/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/tiering_context.hh"
#include "sstables/sstables.hh"
#include "schema/schema.hh"

#include <algorithm>

namespace sstables::parquet {

// C5 of the tiering decision. Nothing in the mutation model is out of reach any
// more: non-frozen collections became a repeated group, counters became one
// element per shard, and every remaining type falls back to an opaque blob
// column, which round-trips because the bytes are what Scylla stores anyway. So
// no schema is currently ineligible.
//
// The gate stays because it is where a future encoding gap belongs -- refusing a
// schema is how the tiering policy avoids silently mangling one -- and because C5
// is part of the documented decision function. Returning a constant is the honest
// answer today, not an oversight.
bool schema_is_parquet_eligible(const ::schema&) {
    return true;
}

// Exact, unlike the leaf count it replaces. The number of Parquet *leaves* a table produces
// is data-dependent -- per-column deletion and TTL leaves appear in L1 only when cells carry
// them, which is why the old `columns + 3` estimate read 13 for a table the exporter reports
// 20 leaves for. C5 is bounded on columns instead, which the schema states outright.
size_t column_count(const ::schema& s) {
    return s.all_columns().size();
}

tiering_inputs make_tiering_inputs(const std::vector<sstables::shared_sstable>& inputs,
                                   const ::schema& s,
                                   const compaction_context& ctx) {
    tiering_inputs in;
    in.bottom_tier = ctx.bottom_tier;
    in.predicted_gain = ctx.predicted_gain;
    in.schema_eligible = schema_is_parquet_eligible(s);
    in.column_count = column_count(s);


    return in;
}

tiering_decision decide_output_format(const std::vector<sstables::shared_sstable>& inputs,
                                      const ::schema& s,
                                      const compaction_context& ctx,
                                      const tiering_thresholds& th) {
    // A table that has not opted in is never converted, whatever the numbers say.
    switch (s.storage_format()) {
    case storage_format_type::sstable:
        return {tiering_verdict::use_native, "table storage_format is 'sstable'"};
    case storage_format_type::parquet:
        // Explicit opt-in: the eligibility gate still applies, because writing a
        // 4 KiB flush as Parquet helps nobody.
        break;
    case storage_format_type::hybrid:
        break;
    }
    return evaluate_tiering(make_tiering_inputs(inputs, s, ctx), th);
}

} // namespace sstables::parquet
