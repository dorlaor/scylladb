/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Lance 2.x container metadata: the 40-byte footer, the column-metadata and
// global-buffer offset tables, ColumnMetadata/Page (protos/file2.proto), and
// the FileDescriptor/Schema/Field carried in global buffer 0
// (protos/file.proto). See docs/dev/lance-storage-format.md 1.
//
// Like the Parquet format layer, this knows nothing about Scylla or Seastar:
// parse functions take memory buffers the caller fetched, and writers return
// byte strings the caller places. Confine to std:: so it stays standalone-
// testable and fuzzable.

#include "pb.hh"

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace sstables::lance::format {

class lance_error : public std::runtime_error {
public:
    explicit lance_error(const std::string& what) : std::runtime_error("lance: " + what) {}
};

inline constexpr size_t footer_size = 40;
inline constexpr char magic[4] = {'L', 'A', 'N', 'C'};

// The version this writer emits. 2.1 is the current stable Lance file
// version; the container is identical across 2.x, only the encoding
// vocabulary differs.
inline constexpr uint16_t version_major = 2;
inline constexpr uint16_t version_minor = 1;

struct footer {
    uint64_t col_meta_start = 0;    // offset of column 0's metadata block
    uint64_t cmo_offset = 0;        // column-metadata offset table
    uint64_t gbo_offset = 0;        // global-buffer offset table
    uint32_t num_global_buffers = 0;
    uint32_t num_columns = 0;
    uint16_t major = 0;
    uint16_t minor = 0;
};

// Parses the last footer_size bytes of a file. Throws on bad magic or an
// unsupported version. Accepts (2,0)/(0,3) [2.0] and (2,1); this reader
// understands only the encodings we write, so anything newer is rejected
// rather than misread.
footer parse_footer(std::span<const uint8_t> tail);

// Serialises the footer, magic included.
std::string write_footer(const footer&);

struct buffer_ref {
    uint64_t offset = 0;
    uint64_t size = 0;
};

// Parses a (u64 position, u64 size) offset table -- the CMO and GBO tables
// share this shape.
std::vector<buffer_ref> parse_offset_table(std::span<const uint8_t> buf, uint32_t entries);
std::string write_offset_table(const std::vector<buffer_ref>&);

// ---------------------------------------------------------------- ColumnMetadata

// lance.file.v2.Encoding: where a page's (or column's) encoding proto lives.
// We only ever write DirectEncoding; DeferredEncoding is accepted on read via
// the buffer it points at, resolved by the caller who has the file bytes.
struct encoding_slot {
    // The serialized google.protobuf.Any payload for a direct encoding, empty
    // for `none`. If `deferred` is set, the Any must be fetched from there.
    std::string any_bytes;
    std::optional<buffer_ref> deferred;
    bool none = false;
};

struct page_info {
    std::vector<buffer_ref> buffers;
    uint64_t rows = 0;        // Page.length: logical row count
    uint64_t priority = 0;    // top-level row ordinal of the first row
    encoding_slot encoding;
};

struct column_meta {
    encoding_slot encoding;   // column-level; `none` for plain value columns
    std::vector<page_info> pages;
    std::vector<buffer_ref> buffers;   // column metadata buffers

    // Locate the page containing top-level row `row`, by priority. Pages are
    // written in row order, so this is a binary search. Throws if `row` is
    // outside the column.
    size_t page_for_row(uint64_t row) const;
};

struct metadata_limits {
    size_t max_pages_per_column = 4u << 20;
    size_t max_buffers_per_page = 64;
    size_t max_encoding_bytes = 1u << 20;
    pb_limits pb{};
};

column_meta parse_column_meta(std::string_view blob, const metadata_limits& = {});
std::string write_column_meta(const column_meta&);

// ---------------------------------------------------------------- schema

// lance.file.Field, the subset we use. Fields form a flat list linked by
// parent_id; all our leaves are top-level (parent_id -1) in v1.
struct field_info {
    enum class kind : uint32_t { parent = 0, repeated = 1, leaf = 2 };
    kind type = kind::leaf;
    std::string name;
    int32_t id = 0;
    int32_t parent_id = -1;
    // Arrow-ish logical type string: "int32", "int64", "double", "string",
    // "binary", "timestamp:ms", ... (see file.proto for the grammar).
    std::string logical_type;
    bool nullable = false;
    std::map<std::string, std::string> metadata;
};

// lance.file.FileDescriptor: Schema + row count. Global buffer 0 by
// convention -- the official reader unconditionally reads it from there.
struct file_descriptor {
    std::vector<field_info> fields;
    std::map<std::string, std::string> schema_metadata;
    uint64_t num_rows = 0;
};

file_descriptor parse_file_descriptor(std::string_view blob, const metadata_limits& = {});
std::string write_file_descriptor(const file_descriptor&);

} // namespace sstables::lance::format
