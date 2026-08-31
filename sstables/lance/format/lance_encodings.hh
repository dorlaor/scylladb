/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Lance 2.1 structural encodings: MiniBlock, FullZip and Constant page
// layouts, with Flat / Variable value encodings and optional General(zstd)
// chunk compression. Byte layouts follow the reference implementation and
// were verified against pylance-written files -- see
// docs/dev/lance-storage-format.md 3.2 and the golden-file conformance suite.
//
// Scope (v1): flat scalar leaves only. bits_rep == 0 everywhere; definition
// is a single 0/1 layer (REPDEF_NULLABLE_ITEM) or absent (ALL_VALID_ITEM).
// That covers every leaf the row-folded CQL mapping produces once non-frozen
// collections are excluded at DDL time. Encodings we do not emit
// (dictionary, FSST, bitpacking, RLE) are rejected on read with a clear
// error, not misread.

#include "lance_metadata.hh"

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace sstables::lance::format {

// Physical type of a leaf. Everything the CQL mapping produces boils down to
// these four.
enum class lphys : uint8_t { i32, i64, f64, bytes };

inline uint32_t bits_of(lphys t) {
    switch (t) {
    case lphys::i32: return 32;
    case lphys::i64: return 64;
    case lphys::f64: return 64;
    case lphys::bytes: return 0;   // variable
    }
    return 0;
}

// One column's values for one page (or one decoded slice of a page).
// Mirrors the shape of the Parquet layer's column_data: a def channel plus
// exactly one populated value vector. Lance def semantics: 0 = valid,
// 1 = null; empty `def` means every value is valid. Null values still occupy
// a slot in the value vector (zero / empty), matching the on-disk "visible
// null" rule for miniblock and fixed-width full-zip.
struct column_values {
    std::vector<uint8_t> def;
    std::vector<int32_t> i32;
    std::vector<int64_t> i64;
    std::vector<double> f64;
    std::vector<std::string> str;

    size_t rows() const {
        if (!i32.empty()) { return i32.size(); }
        if (!i64.empty()) { return i64.size(); }
        if (!f64.empty()) { return f64.size(); }
        if (!str.empty()) { return str.size(); }
        return def.size();   // all-null page: def only
    }
};

struct encode_options {
    // Emit a definition channel. A required leaf never needs one; an optional
    // leaf needs one even for a batch that happens to be fully valid, because
    // pages of one column must agree with the field's nullability.
    bool nullable = false;
    // Structural choice threshold, mirroring the reference writer's
    // `is_narrow`: values under this go to miniblock, at or over it to
    // full-zip. Only meaningful for lphys::bytes; fixed widths always take
    // miniblock.
    size_t fullzip_threshold = 256;
    // Miniblock chunking: target payload bytes per chunk and the cap on
    // values per chunk (power of two, absolute max 4096 in 2.1 defaults).
    size_t chunk_target_bytes = 8u << 10;
    size_t max_chunk_values = 4096;
    // zstd on each miniblock chunk's value sub-buffer via General wrapping.
    // 0 = plain. Full-zip values are never block-compressed (transparency
    // rule: a single value must be independently readable).
    int zstd_level = 0;
};

// The encoded form of one page: buffers in Lance page-buffer order, plus the
// serialized lance.encodings21.PageLayout (not yet Any-wrapped; the metadata
// layer wraps it when it builds ColumnMetadata).
struct encoded_page {
    std::vector<std::string> buffers;
    std::string layout;
    uint64_t rows = 0;
};

encoded_page encode_page(lphys, const column_values&, const encode_options&);

// Serialized PageLayout -> Any bytes with the 2.1 type url, and back.
std::string wrap_page_layout_any(std::string_view layout);
std::string_view unwrap_page_layout_any(std::string_view any_bytes);

// ---------------------------------------------------------------- decoding

// One decoded channel (values or definition levels) of a page: how its
// sub-buffers are encoded. This is the subset of CompressiveEncoding trees
// the reader accepts; dictionary, FSST, RLE and BSS are rejected with a
// clear error.
struct chan_enc {
    enum class kind : uint8_t {
        flat,        // Flat{bits}
        variable,    // Variable{offsets: Flat{bits}}
        inline_bp,   // InlineBitpacking: [T-word width][fastlanes chunk]
        ool_bp,      // OutOfLineBitpacking{T, values: Flat{W}}
    };
    kind k = kind::flat;
    uint32_t bits = 0;            // flat: value width; variable: offset width
    uint32_t logical_bits = 0;    // bitpacking: uncompressed element width T
    uint32_t packed_bits = 0;     // ool_bp: packed width W
    // General wrapping of this channel's sub-buffer: 0 = none, 2 = zstd.
    uint32_t general_scheme = 0;
};

// The parsed subset of PageLayout this reader supports.
struct page_layout {
    enum class kind : uint8_t { miniblock, fullzip, constant };
    kind k = kind::miniblock;

    // Definition channel present (single nullable layer)?
    bool has_def = false;
    chan_enc defs;     // meaningful when has_def
    chan_enc val;

    // miniblock
    uint64_t num_items = 0;

    // fullzip
    uint32_t bits_rep = 0;
    uint32_t bits_def = 0;
    uint64_t num_visible_items = 0;

    // constant
    std::optional<std::string> constant_value;   // absent => all null
};

// Parses the Any-wrapped page encoding from ColumnMetadata. Throws
// lance_error on layouts or compressive encodings outside the supported
// subset (dictionary, FSST, bitpacking, RLE, rep levels, packed structs).
page_layout parse_page_layout(std::string_view any_bytes, const metadata_limits& = {});

// ---------------------------------------------------------------- miniblock

// Parsed chunk-metadata buffer (page buffer 0): where each chunk lives in the
// chunk-data buffer (page buffer 1) and which value range it covers. This is
// the structure a point read consults to fetch exactly one chunk.
struct miniblock_index {
    struct chunk {
        uint64_t byte_offset = 0;   // within the chunk-data buffer
        uint32_t byte_size = 0;
        uint64_t first_value = 0;
        uint32_t values = 0;
    };
    std::vector<chunk> chunks;

    // Index of the chunk holding value ordinal `v` (binary search).
    size_t chunk_for(uint64_t v) const;
};

miniblock_index parse_miniblock_index(std::string_view chunk_meta_buf, uint64_t num_items,
                                      const metadata_limits& = {});

// Decodes chunks [first_chunk, first_chunk + n) from `chunk_bytes`, which
// must hold exactly those chunks' bytes (the caller fetched them using the
// miniblock_index). Values come back in order; the caller slices rows.
column_values decode_miniblock_chunks(lphys, const page_layout&, const miniblock_index&,
                                      size_t first_chunk, size_t n_chunks,
                                      std::string_view chunk_bytes);

// ---------------------------------------------------------------- fullzip

// Fixed-width full-zip: pure arithmetic. Values [lo, hi) of the zipped
// buffer slice; `zipped` must hold bytes [lo*stride, hi*stride).
column_values decode_fullzip_fixed(lphys, const page_layout&, uint64_t lo, uint64_t hi,
                                   std::string_view zipped);

// Variable-width full-zip: the caller reads the rep-index entries for
// [lo, hi] (hi inclusive bound entry) to learn the byte range, fetches it,
// and hands it here.
column_values decode_fullzip_variable(const page_layout&, uint64_t lo, uint64_t hi,
                                      std::string_view zipped);

// The rep-index entry width is inferred from the buffer size: it must be
// exactly (rows + 1) * width with width in {1,2,4,8}.
uint32_t fullzip_rep_index_width(uint64_t rep_index_buffer_size, uint64_t rows);
uint64_t read_rep_index_entry(std::string_view rep_index, uint32_t width, uint64_t i);

// ---------------------------------------------------------------- constant

column_values decode_constant(lphys, const page_layout&, uint64_t lo, uint64_t hi);

// zstd helpers shared by encode/decode (General framing: u64 LE uncompressed
// length, then one zstd frame).
std::string zstd_compress_block(std::string_view raw, int level);
std::string zstd_decompress_block(std::string_view framed, size_t max_size);

} // namespace sstables::lance::format
