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
#include <optional>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace sstables::parquet::format {

struct writer_options {
    codec   compression = codec::zstd;
    int     zstd_level = 3;
    size_t  page_values = 20000;      // values per data page
    bool    use_dictionary = true;
    size_t  dictionary_max_bytes = 1u << 20;
    bool    write_statistics = true;
};

// One leaf column of a row group. Exactly one value vector is populated; which
// one must agree with the declared physical type.
struct column_data {
    // Empty means the column is REQUIRED and every value is present.
    std::vector<uint64_t>    def_levels;
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
};

struct column_spec {
    std::string            name;
    phys_type              type{};
    repetition             rep = repetition::optional;
    std::optional<int32_t> converted_type;     // parquet ConvertedType, if any
    // Encoding hint. The writer may fall back (e.g. dictionary -> plain when the
    // dictionary grows past dictionary_max_bytes).
    std::optional<encoding> preferred;
};

class file_writer {
    struct chunk_meta {
        column_metadata cm;
        int64_t         first_page_offset = 0;
    };
    struct rg_meta {
        std::vector<chunk_meta> chunks;
        int64_t num_rows = 0;
        int64_t total_byte_size = 0;
    };

    std::vector<uint8_t>     _buf;      // whole file image
    std::vector<column_spec> _schema;
    writer_options           _opt;
    std::vector<rg_meta>     _rgs;
    int64_t                  _num_rows = 0;
    std::vector<std::pair<std::string, std::string>> _kv;

    void write_column_chunk(const column_spec&, const column_data&, chunk_meta&);
    void write_footer();

public:
    file_writer(std::vector<column_spec> schema, writer_options opt = {})
        : _schema(std::move(schema)), _opt(opt) {
        _buf.insert(_buf.end(), {'P', 'A', 'R', '1'});
    }

    // Scylla-private metadata (folding level, source table, dictionary id, ...).
    // External readers ignore it; ours uses it to know what was omitted.
    void add_key_value(std::string k, std::string v) { _kv.emplace_back(std::move(k), std::move(v)); }

    // All columns must carry the same number of values.
    void add_row_group(std::span<const column_data> cols);

    // Finalises the footer and returns the file image.
    std::vector<uint8_t> finish();

    size_t size_so_far() const { return _buf.size(); }
};

} // namespace sstables::parquet::format
