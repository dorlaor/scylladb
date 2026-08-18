/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Parquet file writer: pages -> column chunks -> row groups -> Thrift footer.
//
// Layer 1, so it deals in columns of plain values, not mutation fragments. The
// Scylla-facing shredder that turns a mutation-fragment stream into these
// columns lives one level up, in schema_mapping.hh.
//
// Emits V2 data pages exclusively. V2 keeps definition levels outside the
// compressed body, which is what lets a reader skip nulls and locate rows
// without invoking a codec.

#include "parquet_metadata.hh"

#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace sstables::parquet::format {

// One entry per data page, as parquet.thrift PageLocation. `first_row_index` is
// what makes row-ordinal lookup work: given a row number, binary-searching these
// yields the exact page to decode, which is the mechanism Scylla's index relies
// on (design doc section 5.4, option A).
struct page_location {
    int64_t offset = 0;
    int32_t compressed_page_size = 0;
    int64_t first_row_index = 0;
};

struct writer_options {
    codec   compression = codec::zstd;
    int     zstd_level = 3;
    // Values per data page. Measured trade-off (design doc 10.4): a point read
    // decodes whole pages, so smaller pages cost size and buy latency --
    // 1024 -> +7.8 % bytes / 1728 us, 8192 -> +1.9 % / 2151 us, 20000 -> base /
    // 2836 us. 8192 keeps almost all of the compression for most of the speed.
    size_t  page_values = 8192;
    bool    use_dictionary = true;
    // Dictionary-encode *numeric* columns too, not just byte_array ones.
    //
    // Off by default. Measured on a 20k-partition table over 10 000 random point reads,
    // three runs each and 0.3% spread: it costs +10.5% point-read latency (p50 1 977 ->
    // 2 185 us) and buys 3.9% of the file here, 10.9% on a numeric time-series table.
    // Point-read latency is this format's weakest metric and disk is its strongest, so
    // spending the former to improve the latter is the wrong direction by default.
    // Worth turning on for a bottom tier that is scanned and never point-read.
    bool    numeric_dictionary = false;
    size_t  dictionary_max_bytes = 1u << 20;
    // Minimum average repeats per distinct value before a dictionary is used.
    size_t  dictionary_min_repeat = 8;
    bool    write_statistics = true;
    // Emit the OffsetIndex. Required for row-ordinal lookup, and it also
    // lets scan-side readers skip pages.
    bool    write_page_index = true;
};

// One leaf column of a row group. Exactly one value vector is populated; which
// one must agree with the declared physical type.
struct column_data {
    // Empty means the column is REQUIRED and every value is present.
    std::vector<uint64_t>    def_levels;
    // Dremel repetition levels. Empty for a column that is not inside a
    // repeated group. A zero starts a new row, so the number of zeroes is the
    // row count -- which is not the value count once a column repeats.
    std::vector<uint64_t>    rep_levels;
    std::vector<int32_t>     i32;
    std::vector<int64_t>     i64;
    std::vector<double>      f64;
    std::vector<std::string> str;

    size_t num_values() const {
        if (!i32.empty()) { return i32.size(); }
        if (!i64.empty()) { return i64.size(); }
        if (!f64.empty()) { return f64.size(); }
        if (!str.empty()) { return str.size(); }
        return 0;
    }

    // Rows, as opposed to values. They differ only for a repeated column.
    size_t num_rows() const {
        if (rep_levels.empty()) {
            return def_levels.empty() ? num_values() : def_levels.size();
        }
        size_t n = 0;
        for (auto r : rep_levels) { if (r == 0) { ++n; } }
        return n;
    }
};

struct column_spec {
    std::string            name;
    phys_type              type{};
    repetition             rep = repetition::optional;
    std::optional<int32_t> converted_type;     // parquet ConvertedType, if any
    // Encoding hint. The writer may fall back (e.g. dictionary -> plain when the
    // dictionary grows past dictionary_max_bytes).
    std::optional<encoding> preferred;

    // Dremel levels for this leaf. For a flat schema these follow from `rep`
    // alone; a leaf inside a repeated group needs them stated, because the
    // schema tree they come from is not visible here.
    uint8_t max_def = 0;
    uint8_t max_rep = 0;
    // Full path from the root, for the ColumnMetaData. Empty means just `name`.
    // Explicitly defaulted so that the existing positional initialisers in
    // schema_mapping.cc stay complete under -Wmissing-field-initializers.
    std::vector<std::string> path = {};

    std::vector<std::string> path_or_name() const {
        return path.empty() ? std::vector<std::string>{name} : path;
    }
};

class parquet_file_writer {
    struct chunk_meta {
        column_metadata cm;
        int64_t         first_page_offset = 0;
        std::vector<page_location> pages;      // for the OffsetIndex
        // ColumnChunk.offset_index_offset / _length -- these live on the chunk,
        // not on its meta_data.
        std::optional<int64_t> offset_index_offset;
        std::optional<int32_t> offset_index_length;
    };
    struct rg_meta {
        std::vector<chunk_meta> chunks;
        int64_t num_rows = 0;
        int64_t total_byte_size = 0;
    };

    // Bytes not yet handed to the sink. Without a sink this is the whole file image;
    // with one it holds at most the row group being written plus the footer.
    std::vector<uint8_t>     _buf;
    // Bytes already handed to the sink. Every offset this writer records is a position
    // in the *file*, so it must be _flushed + _buf.size() and never _buf.size() alone --
    // see pos(). An offset that forgets the base yields a file that parses and points at
    // the wrong bytes, which is why there is a single accessor for it.
    uint64_t                 _flushed = 0;
    std::function<void(std::span<const uint8_t>)> _sink;
    // The schema tree exactly as Parquet stores it: flat, depth-first, root at
    // index 0. A flat schema is just a root with one leaf per column; a nested
    // one is supplied by the caller.
    std::vector<schema_element> _tree;
    std::vector<column_spec> _schema;   // leaves, in column order
    writer_options           _opt;
    std::vector<rg_meta>     _rgs;
    int64_t                  _num_rows = 0;
    std::vector<std::pair<std::string, std::string>> _kv;

    // The one way to ask "where are we in the file". Never use _buf.size() for an offset.
    int64_t pos() const { return int64_t(_flushed + _buf.size()); }
    // Hand everything buffered to the sink, if there is one, and advance the base.
    void drain();

    void write_column_chunk(const column_spec&, const column_data&, chunk_meta&);
    // Emitted after all row groups and before the footer, which is where the
    // spec puts it: the footer has to carry the offsets of these blobs.
    void write_page_indexes();
    void write_footer();

public:
    // Flat schema: one leaf per column, no nesting.
    parquet_file_writer(std::vector<column_spec> schema, writer_options opt = {});

    // Nested schema. `tree` is depth-first with the root at index 0, which is how
    // the footer stores it; the leaf specs and their Dremel levels are derived
    // from it by walk_leaves(), so writer and reader agree by construction.
    // A schema given as a tree, for anything with repeated groups in it.
    //
    // `preferred` carries the per-leaf encoding hints, in leaf order -- the order
    // walk_leaves() returns, which is also the order the mapping builds its leaves
    // in. Empty means no hints, which is what a test that only has a tree passes.
    //
    // They cannot travel inside the tree: `schema_element` mirrors the Parquet
    // Thrift SchemaElement, where an encoding is a property of a column chunk, not
    // of the schema. So structure comes from the tree and encoding comes from here,
    // and the two stay separate on purpose. Before this field existed the hints
    // were silently dropped on the nested path and every column was written PLAIN
    // -- including monotonic clustering keys the mapping had explicitly asked to be
    // DELTA_BINARY_PACKED. See docs/dev/parquet-storage-format.md section 10.1g.
    struct nested_schema {
        std::vector<schema_element> tree;
        std::vector<std::optional<encoding>> preferred;
    };
    parquet_file_writer(nested_schema tree, writer_options opt = {});

    // The leaves the tree produced, in column order. add_row_group() wants one
    // column_data per entry.
    const std::vector<column_spec>& leaves() const { return _schema; }

    // Scylla-private metadata (folding level, source table, dictionary id, ...).
    // External readers ignore it; ours uses it to know what was omitted.
    void add_key_value(std::string k, std::string v) { _kv.emplace_back(std::move(k), std::move(v)); }

    // All columns must carry the same number of values.
    void add_row_group(std::span<const column_data> cols);

    // Stream the file out as it is produced instead of accumulating it. Completed row
    // groups are handed to the sink and dropped, so peak memory becomes one row group
    // plus the footer rather than the whole output -- which for a 256 MB bottom-tier
    // sstable is the difference between ~253 MB resident and a few MB (design doc 7.2).
    //
    // The footer still has to be held: it carries one column-chunk entry per leaf per row
    // group and cannot be written until the last row group is known. That cost is
    // inherent to the format, and it is small -- measured at 1 % of the file.
    //
    // Must be set before the first add_row_group(). With a sink, finish() returns an
    // empty vector, because the file has already gone to the sink.
    void set_sink(std::function<void(std::span<const uint8_t>)> sink) {
        _sink = std::move(sink);
    }

    // Finalises the footer and returns the file image, or an empty vector if a sink is set.
    std::vector<uint8_t> finish();

    // Total file bytes produced so far, flushed or not.
    size_t size_so_far() const { return size_t(pos()); }
    // Bytes currently held in memory. Equals size_so_far() without a sink; bounded by one
    // row group plus the footer with one.
    size_t buffered_bytes() const { return _buf.size(); }
};

} // namespace sstables::parquet::format
