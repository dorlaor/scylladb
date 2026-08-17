/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Layer 2: the Scylla-facing half of the Parquet storage format.
//
// Turns a stream of rows-with-cells into Parquet columns at a chosen metadata
// folding level, and reconstructs them losslessly on the way back. This is the
// piece that decides whether the format is viable at all -- see
// docs/dev/parquet-storage-format.md section 5.3 and the 26.8x measurement in
// section 10.3.
//
// NOTE ON TYPES. The cell/row structs below mirror the shape of Scylla's
// atomic_cell and clustering_row, not their implementation. Wiring this to the
// real mutation_fragment stream is the remaining Phase 2 work; keeping the
// shredder expressed against a small local model lets the folding logic be
// tested and fuzzed on its own, which is where the risk actually is.

#include "format/parquet_writer.hh"
#include "format/parquet_metadata.hh"

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace sstables::parquet {

using namespace sstables::parquet::format;

// ---------------------------------------------------------------- value model
using value = std::variant<int32_t, int64_t, double, std::string>;

enum class cql_type { int32, bigint, dbl, text, blob, timestamp };

inline phys_type phys_of(cql_type t) {
    switch (t) {
    case cql_type::int32:     return phys_type::int32;
    case cql_type::bigint:    return phys_type::int64;
    case cql_type::timestamp: return phys_type::int64;
    case cql_type::dbl:       return phys_type::dbl;
    case cql_type::text:      return phys_type::byte_array;
    case cql_type::blob:      return phys_type::byte_array;
    }
    return phys_type::byte_array;
}

inline std::optional<int32_t> converted_of(cql_type t) {
    switch (t) {
    case cql_type::text:      return int32_t(converted::utf8);
    // Scylla stores timestamps as microseconds since epoch.
    case cql_type::timestamp: return int32_t(converted::timestamp_micros);
    default:                  return std::nullopt;
    }
}

enum class column_kind { partition_key, clustering_key, regular };

struct cql_column {
    std::string name;
    cql_type    type;
    column_kind kind;
    // A non-frozen collection. It becomes a Dremel MAP group rather than a
    // single leaf (design doc 5.2); frozen ones are already opaque blobs and
    // travel as ordinary BYTE_ARRAY values.
    bool        multi_cell = false;
};

// A cell as the storage layer sees it: a value plus its own metadata. Key
// columns have no cell metadata -- they are part of the row's identity.
struct cell {
    bool                   live = true;
    std::optional<value>   v;                 // nullopt => the column is absent
    int64_t                timestamp = 0;
    std::optional<int32_t> ttl;               // seconds
    std::optional<int32_t> local_deletion_time;
};

// A deletion: when it happened, and when it becomes collectable.
struct deletion_info {
    int64_t timestamp = 0;
    int32_t local_deletion_time = 0;
    bool operator==(const deletion_info&) const = default;
};

// The CQL row marker -- what makes a row exist even with every column null.
// Almost every INSERT creates one, so it is stored as a delta against the row's
// own timestamp, which is nearly always zero and costs nothing after zstd.
struct marker_info {
    int64_t                timestamp = 0;
    std::optional<int32_t> ttl;
    std::optional<int32_t> expiry;
    bool operator==(const marker_info&) const = default;
};

// A range tombstone change: not a row, but carried as one so that it keeps its
// place in the clustering order without a second stream. The clustering-key
// columns hold the bound's prefix, padded past `prefix_len` with values that
// mean nothing.
struct rtc_info {
    int32_t weight = 0;       // bound_weight
    int32_t region = 0;       // partition_region
    int32_t prefix_len = 0;   // clustering components the bound actually sets
    std::optional<deletion_info> tomb;   // nullopt: the change closes a range
    bool operator==(const rtc_info&) const = default;
};

// One entry of a non-frozen collection. Scylla stores these as (key, cell)
// pairs: for a set the key is the element and the cell carries only liveness,
// for a map the key is the map key, for a list a timeuuid. All serialised, so
// both sides are opaque bytes here.
struct collection_element {
    std::string                key;
    std::optional<std::string> value;   // nullopt => the element is dead
    int64_t                    timestamp = 0;
    std::optional<int32_t>     ttl;
    std::optional<int32_t>     local_deletion_time;
    bool operator==(const collection_element&) const = default;
};

struct collection_cell {
    std::optional<deletion_info>    tomb;       // collection-wide tombstone
    std::vector<collection_element> elements;
    bool operator==(const collection_cell&) const = default;
};

struct row {
    std::vector<value>       key;             // partition key then clustering key
    std::map<size_t, cell>   cells;           // index into the regular columns
    std::optional<marker_info>   marker;      // row marker, if the row has one
    std::optional<deletion_info> row_del;     // row tombstone
    // The partition's tombstone, repeated on every row of that partition. It
    // costs nothing -- a column that is constant within a partition and usually
    // absent entirely compresses away -- and it keeps a row self-describing,
    // which is what lets the reader work a window at a time.
    std::optional<deletion_info> part_del;
    // True for a placeholder row standing in for a partition that has no
    // clustering rows -- a static row or a bare partition tombstone. Its
    // clustering-key values are meaningless. Marking the row is cheaper than
    // making every clustering-key column nullable for every table.
    bool no_ck = false;
    // Set when this "row" is really a range tombstone change.
    std::optional<rtc_info> rtc;
    // Non-frozen collections, keyed in the same index space as `cells`.
    std::map<size_t, collection_cell> collections;
};

// ---------------------------------------------------------------- folding
enum class folding_level {
    verbatim,     // L0: five leaves per regular column. The 2020 mapping.
    row_folded,   // L1: one leaf per column, one __ts per row, plus a sparse
                  //     side-channel for cells that disagree with their row.
    uniform,      // L2: as L1 but the whole row group shares one timestamp,
                  //     which then lives in the file's key/value metadata.
    logical,      // L3: the user's CQL schema and nothing else -- no cell
                  //     metadata at all. LOSSY: write times, TTLs and deletions
                  //     are discarded, so this can never be a storage format.
                  //     Export only; reassemble() refuses it.
};

// True for levels that can be read back into the rows they came from. L3 cannot.
inline bool folding_is_lossless(folding_level l) {
    return l != folding_level::logical;
}

// How L1 records cells whose timestamp differs from their row's.
//
//   per_column -- one optional __tsx_<col> leaf per regular column. Simple, but
//                 measured badly: at row-group scale "some row diverges in this
//                 column" is near-certain, so every leaf materialises even at 1%
//                 divergence while carrying almost nothing (43 -> 83 leaves; see
//                 docs/dev/parquet-storage-format.md section 10.3a).
//   sparse     -- two leaves total regardless of table width: a per-row bitmap of
//                 which columns diverge, and a blob of zigzag-varint deltas from
//                 the row timestamp. Both null for rows with no exception, which
//                 costs a definition-level bit and compresses away.
enum class exception_encoding { per_column, sparse };

const char* to_string(folding_level);

// The physical Parquet schema a (cql schema, folding level) pair produces, plus
// the bookkeeping a reader needs to invert it.
struct mapped_schema {
    std::vector<column_spec> columns;         // physical Parquet leaves
    // The schema tree the leaves came from, depth-first with the root at index 0.
    // Flat for a schema without collections; a collection contributes a MAP group.
    // Kept so the writer always takes one path, nested or not.
    std::vector<format::schema_element> tree;
    folding_level            level{};
    size_t                   n_key = 0;       // leading key columns
    size_t                   n_regular = 0;
    // Index of the __ts column, if the level materialises one.
    std::optional<size_t>    ts_index;
    // L1/per_column: index of the exception leaf for each regular column.
    std::vector<std::optional<size_t>> ts_exc_index;
    // L1/sparse: the two side-channel leaves.
    exception_encoding    exc_encoding = exception_encoding::sparse;
    std::optional<size_t> tsx_mask_index;
    std::optional<size_t> tsx_vals_index;
    // For L0: index of the first of the four metadata leaves per column.
    std::vector<std::optional<size_t>> meta_base_index;
    // For L1: the per-column TTL and local-deletion-time leaves. Recorded
    // individually rather than as base + k, because the groups are skipped for
    // collection columns (their per-element metadata lives inside the group) and
    // because writing only one of the two silently produced a ragged row group.
    std::vector<std::optional<size_t>> l1_ttl_index, l1_ldt_index;
    // Row marker, row tombstone and partition tombstone leaves. Each group is
    // materialised only when the data needs it.
    std::optional<size_t> rm_index, rm_ttl_index, rm_ldt_index;
    std::optional<size_t> rt_ts_index, rt_ldt_index;
    std::optional<size_t> pt_ts_index, pt_ldt_index;
    std::optional<size_t> no_ck_index;
    std::optional<size_t> rtc_w_index, rtc_reg_index, rtc_len_index,
                          rtc_ts_index, rtc_ldt_index;
    // Per regular column: where its value lives, and whether that is a scalar
    // leaf or the first of the five leaves a collection group contributes.
    // Recorded rather than computed, because arithmetic over leaf positions is
    // exactly what broke when the metadata groups were added.
    std::vector<size_t> value_leaf;
    std::vector<bool>   value_is_collection;
    // Row-level collection tombstone leaves, per regular column.
    std::vector<std::optional<size_t>> ct_ts_index, ct_ldt_index;
    // For L2: the single timestamp shared by every cell.
    std::optional<int64_t>   uniform_ts;

    size_t leaf_count() const { return columns.size(); }
};

// Decides which optional metadata leaves are actually needed for this batch of
// rows, then builds the schema. Materialising a leaf that is never used is the
// entire cost of the 2020 mapping, so this inspects the data first.
// What map_schema decides by looking at the data: which optional leaf groups a
// batch of rows actually needs. Separated from the schema builder so a reader
// can recover the same answers from a file it did not write.
struct schema_flags {
    bool any_ttl = false;
    bool any_deletion = false;
    bool all_same_ts = true;
    bool any_marker = false;
    bool any_marker_ttl = false;
    bool any_row_del = false;
    bool any_part_del = false;
    bool any_no_ck = false;
    bool any_rtc = false;
    std::vector<bool> col_diverges;      // per regular column
    std::optional<int64_t> single_ts;
};

schema_flags scan_rows(const std::vector<cql_column>& cols, const std::vector<row>& rows);

// The single schema builder. Both map_schema (write side) and
// recover_mapped_schema (read side) go through this, so there is exactly one
// definition of the leaf layout.
mapped_schema build_mapped_schema(const std::vector<cql_column>& cols,
                                  folding_level requested,
                                  const schema_flags&,
                                  exception_encoding = exception_encoding::sparse);

mapped_schema map_schema(const std::vector<cql_column>& cols,
                         folding_level requested,
                         const std::vector<row>& rows,
                         exception_encoding = exception_encoding::sparse);

// Rebuild the mapped_schema of a file we did not write, from its footer. The
// folding level comes from the scylla.folding_level key/value entry; which
// optional leaf groups exist is read off the leaf names. Throws if the file was
// not written by this mapping, or if the recovered layout does not match the
// file's own leaves -- a mismatch means a silent misread, so it must not be
// tolerated.
mapped_schema recover_mapped_schema(const file_metadata&,
                                    const std::vector<cql_column>& cols);

// Shred rows into Parquet columns according to `ms`.
std::vector<column_data> shred(const mapped_schema& ms,
                               const std::vector<cql_column>& cols,
                               const std::vector<row>& rows);

// Rebuild the rows. Must be exactly equal to the input for L0 and L1, and for
// L2 whenever the uniform precondition held.
std::vector<row> reassemble(const mapped_schema& ms,
                            const std::vector<cql_column>& cols,
                            const std::vector<column_data>& colsdata,
                            size_t nrows);

// Convenience: schema + shred + write a complete Parquet file.
std::vector<uint8_t> write_rows(const std::vector<cql_column>& cols,
                                const std::vector<row>& rows,
                                folding_level level,
                                writer_options opt = {},
                                exception_encoding = exception_encoding::sparse);

} // namespace sstables::parquet
