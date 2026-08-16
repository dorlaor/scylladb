/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "parquet_reader.hh"
#include "page_header.hh"
#include "decoders.hh"

#include <algorithm>

#include <cstring>
#include <zstd.h>

namespace sstables::parquet::format {

namespace {

std::vector<uint8_t> decompress(std::span<const uint8_t> in, codec c, size_t expected) {
    if (c == codec::uncompressed) { return {in.begin(), in.end()}; }
    if (c != codec::zstd) {
        throw decode_error("only zstd and uncompressed are supported on the read path");
    }
    std::vector<uint8_t> out(expected);
    const size_t n = ZSTD_decompress(out.data(), out.size(), in.data(), in.size());
    if (ZSTD_isError(n)) { throw decode_error(std::string("zstd: ") + ZSTD_getErrorName(n)); }
    out.resize(n);
    return out;
}

// Append `n` decoded values of the chunk's physical type to `cd`.
void append_values(column_data& cd, phys_type pt, encoding enc,
                   std::span<const uint8_t> body, size_t n,
                   const std::vector<std::string>& dict_ba,
                   const std::vector<int32_t>& dict_i32,
                   const std::vector<int64_t>& dict_i64,
                   const std::vector<double>& dict_f64) {
    switch (pt) {
    case phys_type::int32: {
        auto v = (enc == encoding::rle_dictionary || enc == encoding::plain_dictionary)
               ? decode_rle_dictionary<int32_t>(body, dict_i32, n)
               : decode_plain<int32_t>(body, n);
        cd.i32.insert(cd.i32.end(), v.begin(), v.end());
        break;
    }
    case phys_type::int64: {
        std::vector<int64_t> v;
        if (enc == encoding::rle_dictionary || enc == encoding::plain_dictionary) {
            v = decode_rle_dictionary<int64_t>(body, dict_i64, n);
        } else if (enc == encoding::delta_binary_packed) {
            v = decode_delta_binary_packed(body, n);
        } else {
            v = decode_plain<int64_t>(body, n);
        }
        cd.i64.insert(cd.i64.end(), v.begin(), v.end());
        break;
    }
    case phys_type::dbl: {
        std::vector<double> v;
        if (enc == encoding::rle_dictionary || enc == encoding::plain_dictionary) {
            v = decode_rle_dictionary<double>(body, dict_f64, n);
        } else if (enc == encoding::byte_stream_split) {
            v = decode_byte_stream_split<double>(body, n);
        } else {
            v = decode_plain<double>(body, n);
        }
        cd.f64.insert(cd.f64.end(), v.begin(), v.end());
        break;
    }
    case phys_type::byte_array: {
        auto v = (enc == encoding::rle_dictionary || enc == encoding::plain_dictionary)
               ? decode_rle_dictionary<std::string>(body, dict_ba, n)
               : decode_plain_byte_array(body, n);
        cd.str.insert(cd.str.end(), v.begin(), v.end());
        break;
    }
    default:
        throw decode_error("unsupported physical type on the read path");
    }
}

// A null in an optional column occupies a definition-level slot but no value, so
// the value vectors have to be re-expanded to one entry per row for the caller,
// which indexes them positionally alongside def_levels.
void expand_nulls(column_data& cd, phys_type pt, size_t first, size_t count,
                  std::span<const uint64_t> levels) {
    const size_t present = size_t(std::count(levels.begin(), levels.end(), uint64_t(1)));
    if (present == count) { return; }   // dense: nothing to do
    auto expand = [&] (auto& vec, auto zero) {
        std::decay_t<decltype(vec)> out;
        out.reserve(count);
        size_t src = first;
        for (size_t i = 0; i < count; ++i) {
            if (levels[i]) { out.push_back(vec[src++]); }
            else           { out.push_back(zero); }
        }
        vec.resize(first);
        vec.insert(vec.end(), out.begin(), out.end());
    };
    switch (pt) {
    case phys_type::int32:      expand(cd.i32, int32_t(0)); break;
    case phys_type::int64:      expand(cd.i64, int64_t(0)); break;
    case phys_type::dbl:        expand(cd.f64, 0.0); break;
    case phys_type::byte_array: expand(cd.str, std::string()); break;
    default: throw decode_error("unsupported physical type when expanding nulls");
    }
}

} // namespace

std::vector<column_data> read_row_group(std::span<const uint8_t> image,
                                        const file_metadata& md,
                                        size_t rg_index) {
    if (rg_index >= md.row_groups.size()) { throw decode_error("row group index out of range"); }
    const auto& rg = md.row_groups[rg_index];

    // Leaf schema elements, in order, so repetition type is known per column.
    std::vector<const schema_element*> leaves;
    for (size_t i = 1; i < md.schema.size(); ++i) {
        if (md.schema[i].is_leaf()) { leaves.push_back(&md.schema[i]); }
    }
    if (leaves.size() != rg.columns.size()) {
        throw decode_error("row group chunk count does not match the schema");
    }

    std::vector<column_data> out(rg.columns.size());

    for (size_t c = 0; c < rg.columns.size(); ++c) {
        const auto& cc = rg.columns[c];
        if (!cc.meta) { throw decode_error("column chunk without metadata"); }
        const auto& cm = *cc.meta;
        const bool optional = leaves[c]->repetition_type &&
                              *leaves[c]->repetition_type == repetition::optional;

        std::vector<std::string> dict_ba;
        std::vector<int32_t> dict_i32;
        std::vector<int64_t> dict_i64;
        std::vector<double>  dict_f64;

        int64_t off = cm.dictionary_page_offset ? *cm.dictionary_page_offset : cm.data_page_offset;
        const int64_t end = off + cm.total_compressed_size;
        if (off < 0 || size_t(end) > image.size()) { throw decode_error("chunk extends past EOF"); }

        int64_t produced = 0;
        while (off < end && produced < cm.num_values) {
            size_t consumed = 0;
            auto ph = parse_page_header(
                    image.subspan(size_t(off), size_t(std::min<int64_t>(end - off, 64 * 1024))),
                    consumed);
            const int64_t body_at = off + int64_t(consumed);
            if (body_at + ph.compressed_page_size > int64_t(image.size())) {
                throw decode_error("page body extends past EOF");
            }
            auto body = image.subspan(size_t(body_at), size_t(ph.compressed_page_size));

            if (ph.type == page_type::dictionary_page) {
                if (!ph.dict) { throw decode_error("dictionary page without header"); }
                auto raw = decompress(body, cm.compression, size_t(ph.uncompressed_page_size));
                const size_t n = size_t(ph.dict->num_values);
                switch (cm.type) {
                case phys_type::int32:      dict_i32 = decode_plain<int32_t>(raw, n); break;
                case phys_type::int64:      dict_i64 = decode_plain<int64_t>(raw, n); break;
                case phys_type::dbl:        dict_f64 = decode_plain<double>(raw, n); break;
                case phys_type::byte_array: dict_ba  = decode_plain_byte_array(raw, n); break;
                default: throw decode_error("unsupported dictionary type");
                }
            } else if (ph.type == page_type::data_page_v2 && ph.v2) {
                const auto& h = *ph.v2;
                const size_t n = size_t(h.num_values);
                const size_t rl = size_t(h.repetition_levels_byte_length);
                const size_t dl = size_t(h.definition_levels_byte_length);
                if (rl + dl > body.size()) { throw decode_error("level lengths exceed page body"); }

                std::vector<uint64_t> levels;
                if (optional) {
                    rle_decoder ld(body.subspan(rl, dl), 1);
                    levels = ld.decode_all(n);
                    if (levels.size() != n) { throw decode_error("short definition level stream"); }
                    out[c].def_levels.insert(out[c].def_levels.end(), levels.begin(), levels.end());
                }

                // V2 keeps levels outside the compressed region.
                auto vbody = body.subspan(rl + dl);
                const size_t uncompressed_values = size_t(ph.uncompressed_page_size) - rl - dl;
                auto raw = decompress(vbody, cm.compression, uncompressed_values);

                const size_t present = optional
                        ? size_t(std::count(levels.begin(), levels.end(), uint64_t(1))) : n;
                const size_t before = out[c].num_values();
                append_values(out[c], cm.type, h.value_encoding, raw, present,
                              dict_ba, dict_i32, dict_i64, dict_f64);
                if (optional) {
                    expand_nulls(out[c], cm.type, before, n, levels);
                }
                produced += int64_t(n);
            } else {
                throw decode_error("only V2 data pages are supported on the read path");
            }

            if (ph.compressed_page_size <= 0) { throw decode_error("non-positive page size"); }
            off = body_at + ph.compressed_page_size;
        }
        if (produced != cm.num_values) {
            throw decode_error("decoded " + std::to_string(produced) + " values but the chunk "
                               "declares " + std::to_string(cm.num_values));
        }
    }
    return out;
}

std::vector<column_data> read_file(std::span<const uint8_t> image) {
    auto md = parse_footer(image);
    if (md.row_groups.empty()) { return {}; }
    return read_row_group(image, md, 0);
}

} // namespace sstables::parquet::format
