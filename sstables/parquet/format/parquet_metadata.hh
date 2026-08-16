/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Parquet FileMetaData, decoded from the Thrift footer.
//
// Field ids follow parquet-format's parquet.thrift. Fields we do not model are
// skipped rather than rejected, so a newer writer's footer still parses.
//
// Layer 1: no Scylla types here. See docs/dev/parquet-storage-format.md 7.8.

#include "thrift_compact.hh"

#include <optional>
#include <span>
#include <string>
#include <vector>

namespace sstables::parquet::format {

enum class phys_type : int32_t {
    boolean = 0, int32 = 1, int64 = 2, int96 = 3,
    flt = 4, dbl = 5, byte_array = 6, flba = 7,
};

enum class repetition : int32_t { required = 0, optional = 1, repeated = 2 };

// parquet.thrift ConvertedType. Only the ones the CQL type mapping needs.
enum class converted : int32_t {
    utf8 = 0, map = 1, list = 3, decimal = 5, date = 6,
    time_millis = 7, time_micros = 8, timestamp_millis = 9, timestamp_micros = 10,
    uint_8 = 11, uint_16 = 12, uint_32 = 13, uint_64 = 14,
    int_8 = 15, int_16 = 16, int_32 = 17, int_64 = 18, json = 19, bson = 20,
};

enum class codec : int32_t {
    uncompressed = 0, snappy = 1, gzip = 2, lzo = 3,
    brotli = 4, lz4 = 5, zstd = 6, lz4_raw = 7,
};

enum class encoding : int32_t {
    plain = 0, plain_dictionary = 2, rle = 3, bit_packed = 4,
    delta_binary_packed = 5, delta_length_byte_array = 6, delta_byte_array = 7,
    rle_dictionary = 8, byte_stream_split = 9,
};

const char* to_string(phys_type);
const char* to_string(codec);
const char* to_string(encoding);
const char* to_string(repetition);

struct schema_element {
    std::optional<phys_type>  type;
    std::optional<int32_t>    type_length;
    std::optional<repetition> repetition_type;
    std::string               name;
    std::optional<int32_t>    num_children;   // absent/0 => leaf
    std::optional<int32_t>    converted_type;
    std::optional<int32_t>    field_id;

    bool is_leaf() const { return !num_children.has_value() || *num_children == 0; }
};

struct key_value { std::string key, value; };

struct statistics {
    std::optional<int64_t> null_count;
    std::optional<int64_t> distinct_count;
    std::optional<std::string> min_value, max_value;
};

struct column_metadata {
    phys_type                type{};
    std::vector<encoding>    encodings;
    std::vector<std::string> path_in_schema;
    codec                    compression{};
    int64_t                  num_values = 0;
    int64_t                  total_uncompressed_size = 0;
    int64_t                  total_compressed_size = 0;
    int64_t                  data_page_offset = 0;
    std::optional<int64_t>   index_page_offset;
    std::optional<int64_t>   dictionary_page_offset;
    std::optional<statistics> stats;
    std::optional<int64_t>   bloom_filter_offset;

    std::string path() const {
        std::string s;
        for (size_t i = 0; i < path_in_schema.size(); ++i) {
            if (i) { s += '.'; }
            s += path_in_schema[i];
        }
        return s;
    }
};

// parquet.thrift PageLocation. `first_row_index` is the row ordinal of the first
// row in the page, relative to the start of the row group.
struct page_loc {
    int64_t offset = 0;
    int32_t compressed_page_size = 0;
    int64_t first_row_index = 0;
};

// The OffsetIndex of one column chunk: enough to turn a row ordinal into the
// exact page that holds it, which is what row-ordinal lookup needs.
struct offset_index {
    std::vector<page_loc> pages;

    // Index of the page containing `row` (relative to the row group), or
    // pages.size() if the row is past the end.
    size_t page_for_row(int64_t row) const {
        size_t lo = 0, hi = pages.size();
        while (lo < hi) {
            const size_t mid = lo + (hi - lo) / 2;
            if (pages[mid].first_row_index <= row) { lo = mid + 1; } else { hi = mid; }
        }
        return lo ? lo - 1 : pages.size();
    }
};

struct column_chunk {
    std::optional<std::string>     file_path;
    int64_t                        file_offset = 0;
    std::optional<column_metadata> meta;
    std::optional<int64_t>         column_index_offset;
    std::optional<int32_t>         column_index_length;
    std::optional<int64_t>         offset_index_offset;
    std::optional<int32_t>         offset_index_length;

    bool has_page_index() const { return column_index_offset.has_value(); }
};

struct row_group {
    std::vector<column_chunk> columns;
    int64_t                   total_byte_size = 0;
    int64_t                   num_rows = 0;
    std::optional<int64_t>    file_offset;
    std::optional<int64_t>    total_compressed_size;
    std::optional<int16_t>    ordinal;
};

struct file_metadata {
    int32_t                     version = 0;
    std::vector<schema_element> schema;      // flat, depth-first per the spec
    int64_t                     num_rows = 0;
    std::vector<row_group>      row_groups;
    std::vector<key_value>      key_value_metadata;
    std::optional<std::string>  created_by;

    size_t leaf_count() const {
        size_t n = 0;
        for (size_t i = 1; i < schema.size(); ++i) {   // schema[0] is the root
            if (schema[i].is_leaf()) { ++n; }
        }
        return n;
    }
    const std::string* kv(std::string_view k) const {
        for (auto& e : key_value_metadata) { if (e.key == k) { return &e.value; } }
        return nullptr;
    }
};

// Structural checks that Thrift itself cannot express. Without these a footer
// that is merely well-formed Thrift -- a bare STOP byte, say -- decodes into an
// empty file_metadata and is indistinguishable from a real empty file. Found by
// the adversarial cases in test_parquet_metadata.cc.
void validate(const file_metadata&);

// Parse a bare FileMetaData Thrift blob (i.e. the footer with the trailing
// 4-byte length and "PAR1" magic already stripped).
// `check` runs validate() on the result; the fuzz driver turns it off so it can
// exercise the decoder alone.
enum class semantic_check { no, yes };
file_metadata parse_file_metadata(std::span<const uint8_t> thrift_blob, limits = {},
                                  semantic_check = semantic_check::yes);

// Validate the footer envelope of a whole file image and parse it. Checks the
// leading and trailing "PAR1" magic and the footer length.
file_metadata parse_footer(std::span<const uint8_t> file_image, limits = {});

// Parse a chunk's OffsetIndex from the file image, using the offsets the footer
// recorded. Returns nullopt when the chunk carries no page index.
std::optional<offset_index> parse_offset_index(std::span<const uint8_t> file_image,
                                               const column_chunk&, limits = {});

// Byte offset and length of the metadata blob within a file image, without parsing.
struct footer_span { size_t offset, length; };
footer_span locate_footer(std::span<const uint8_t> file_image);

} // namespace sstables::parquet::format
