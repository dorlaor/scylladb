/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/tiering_policy.hh"

#include <fmt/format.h>

namespace sstables::parquet {

namespace {

tiering_decision decline(std::string why) {
    return {tiering_verdict::use_native, std::move(why)};
}

} // namespace

tiering_decision evaluate_tiering(const tiering_inputs& in,
                                  const tiering_thresholds& th,
                                  tiering_mode mode) {
    // The criteria are a conjunction, and the order here is deliberate: cheapest
    // and most-often-decisive first, so the common "this is an L0 flush" case
    // costs one comparison.

    // C1 -- position in the LSM.
    if (!in.bottom_tier) {
        return decline("not a bottom-tier output; it would be re-compacted and "
                       "re-encoded again");
    }

    // C2 -- size. Below a few row groups the footer and per-chunk metadata
    // dominate and the ratio inverts.
    if (in.estimated_output_bytes < th.min_output_bytes) {
        return decline(fmt::format("output {} B is below the {} B minimum",
                                   in.estimated_output_bytes, th.min_output_bytes));
    }

    // C3 -- age. Converting data that is still being overwritten pays the encode
    // cost twice.
    if (in.data_age < th.min_data_age) {
        return decline(fmt::format("data is {}s old, younger than the {}s minimum",
                                   in.data_age.count(), th.min_data_age.count()));
    }

    // C4 -- garbage. High tombstone density means an imminent GC rewrite, and
    // tombstones force the deletion metadata columns to materialise.
    if (in.garbage_fraction > th.max_garbage_fraction) {
        return decline(fmt::format("garbage fraction {:.3f} exceeds {:.3f}",
                                   in.garbage_fraction, th.max_garbage_fraction));
    }

    // C5 -- schema.
    if (!in.schema_eligible) {
        return decline("schema is not eligible (counters or unsupported types)");
    }
    if (in.estimated_leaf_columns > th.max_leaf_columns) {
        return decline(fmt::format("{} leaf columns exceeds the {} limit",
                                   in.estimated_leaf_columns, th.max_leaf_columns));
    }

    // C6 -- the load-bearing one. An unmeasured table is not a table we convert:
    // section 3.4 can only guess at the gain, and guessing wrong means rewriting
    // terabytes for nothing.
    if (!in.predicted_gain) {
        return decline("no measured gain available; run the estimator first");
    }
    if (*in.predicted_gain < th.min_gain_ratio) {
        return decline(fmt::format("predicted gain {:.3f} is below the {:.3f} minimum",
                                   *in.predicted_gain, th.min_gain_ratio));
    }

    // C7 -- read pattern, only in adaptive mode.
    if (mode == tiering_mode::adaptive && in.point_read_dominated.value_or(false)) {
        return decline("table is point-read dominated; columnar would cost latency "
                       "for a size win it does not need");
    }

    return {tiering_verdict::use_parquet,
            fmt::format("bottom tier, {} B, {}s old, garbage {:.3f}, predicted gain {:.3f}",
                        in.estimated_output_bytes, in.data_age.count(),
                        in.garbage_fraction, *in.predicted_gain)};
}

} // namespace sstables::parquet
