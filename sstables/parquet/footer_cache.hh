/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The parsed Parquet footer of one pq sstable, retained between reads.
//
// Why this exists: a cold point read on a pq sstable costs 1.11 us per row group (design doc
// 10.21), and the part that scales is fetching and Thrift-walking the footer -- 1.4 kB per row
// group (10.22) -- which is a pure function of an immutable file and therefore need happen only
// once per sstable rather than once per read.
//
// Why it is *evictable* rather than merely bounded: at 1.4 kB per row group a 1 601-group sstable
// costs ~2.3 MB, so a node holding a thousand of them would want ~2.3 GB. That is far too much to
// pin, so the entry registers with the reclaim machinery in sstables_manager that already drops
// bloom filters under pressure, rather than inventing a second policy (10.22).
//
// This header is deliberately free of Parquet types so that sstables.hh -- which owns the entry,
// sizes it and drops it -- does not have to pull in the format layer. The concrete entry lives in
// sstables/parquet/reader.cc, next to the code that parses a footer in the first place.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <seastar/core/shared_ptr.hh>

namespace sstables::parquet {

class cached_footer_base {
public:
    virtual ~cached_footer_base() = default;
    // Bytes retained by this entry. Measured from the capacities of the containers that hold it,
    // not estimated from the on-disk footer length -- see cached_footer::retained_bytes().
    virtual size_t memory_size() const noexcept = 0;
};

// Immutable and shared: a reader that is mid-read keeps its entry alive across an eviction, which
// is what makes eviction transparent rather than a use-after-free.
using cached_footer_ptr = seastar::shared_ptr<const cached_footer_base>;

// Shard-local counters, exported as the `sstables_pq_footer_cache_*` metrics.
struct footer_cache_stats {
    uint64_t hits = 0;
    uint64_t misses = 0;
    uint64_t populations = 0;
    uint64_t evictions = 0;      // dropped by the reclaimer, i.e. under memory pressure
    uint64_t bytes = 0;          // currently retained across this shard's sstables
};

inline footer_cache_stats& footer_cache_stats_local() noexcept {
    static thread_local footer_cache_stats s;
    return s;
}

inline void note_footer_cache_populated(size_t bytes) noexcept {
    auto& s = footer_cache_stats_local();
    ++s.populations;
    s.bytes += bytes;
}

// `evicted` distinguishes a drop under memory pressure from a drop because the sstable is going
// away; only the former is something an operator wants to see in the eviction counter.
inline void note_footer_cache_dropped(size_t bytes, bool evicted) noexcept {
    auto& s = footer_cache_stats_local();
    if (evicted) {
        ++s.evictions;
    }
    s.bytes -= std::min(s.bytes, uint64_t(bytes));
}

} // namespace sstables::parquet
