/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Reader for the `lc` sstable format: the inverse of the lance codec path in
// sstables/parquet/writer_impl.cc.
//
// The Data component is a complete, externally-valid Lance 2.1 file. The
// partition index carries row ordinals exactly as pq's does; the ordinal maps
// to a page by Page.priority and to a miniblock chunk (or full-zip record) by
// the format's own structures, so a point read fetches per touched leaf: the
// page's chunk metadata (small, cached on the sstable) and one chunk-sized
// extent -- the format's design center (docs/dev/lance-storage-format.md 1).
//
// Structure and semantics deliberately mirror sstables/parquet/reader.cc: the
// ordinal-window machinery, the fragment reconstruction and the filter
// handling are the same design; only the "row range -> column_data" layer is
// Lance. The duplication is recorded as such in the design doc 3.1 -- the
// intended later refactor extracts the shared fragment builder.

#include "readers/mutation_reader_fwd.hh"
#include "readers/mutation_reader.hh"
#include "sstables/shared_sstable.hh"
#include "sstables/progress_monitor.hh"
#include "schema/schema_fwd.hh"
#include "dht/i_partitioner.hh"
#include "query/query-request.hh"
#include "tracing/trace_state.hh"

namespace sstables::lance {

mutation_reader make_reader(
        sstables::shared_sstable sst,
        schema_ptr query_schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding fwd_mr,
        sstables::read_monitor& mon);

mutation_reader make_full_scan_reader(
        sstables::shared_sstable sst,
        schema_ptr schema,
        reader_permit permit,
        tracing::trace_state_ptr trace_state,
        sstables::read_monitor& mon);

} // namespace sstables::lance
