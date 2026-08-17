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
#include <snappy.h>
#include <zstd.h>

namespace sstables::parquet::format {

namespace {

std::vector<uint8_t> decompress(std::span<const uint8_t> in, codec c, size_t expected) {
    switch (c) {
    case codec::uncompressed:
        return {in.begin(), in.end()};
    case codec::zstd: {
        std::vector<uint8_t> out(expected);
        const size_t n = ZSTD_decompress(out.data(), out.size(), in.data(), in.size());
        if (ZSTD_isError(n)) { throw decode_error(std::string("zstd: ") + ZSTD_getErrorName(n)); }
        out.resize(n);
        return out;
    }
    case codec::snappy: {
        // Common in files written by other implementations; cheap to support
        // since Scylla already links snappy.
        size_t n = 0;
        if (!snappy::GetUncompressedLength(reinterpret_cast<const char*>(in.data()), in.size(), &n)) {
            throw decode_error("snappy: bad length header");
        }
        std::vector<uint8_t> out(n);
        if (!snappy::RawUncompress(reinterpret_cast<const char*>(in.data()), in.size(),
                                   reinterpret_cast<char*>(out.data()))) {
            throw decode_error("snappy: corrupt input");
        }
        return out;
    }
    default:
        throw decode_error(std::string("unsupported codec on the read path: ") + to_string(c));
    }
}

// Append `n` decoded values of the chunk's physical type to `cd`.
void append_values(column_data& cd, phys_type pt, encoding enc,
                   std::span<const uint8_t> body, size_t n,
                   std::span<const std::string_view> dict_ba,
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
               ? decode_rle_dictionary_views(body, dict_ba, n)
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
// Keep `keep` rows starting at `drop`. For a flat column values are dense after
// expand_nulls, so levels and values slice identically. For a repeated one a row
// spans several slots, so the row boundaries have to be found in rep_levels and
// the value vectors sliced by how many values those slots actually carried.
void trim(column_data& cd, size_t drop, size_t keep, bool repeated) {
    auto cut = [&] (auto& v, size_t from, size_t count) {
        if (from >= v.size()) { v.clear(); return; }
        v.erase(v.begin(), v.begin() + long(from));
        if (v.size() > count) { v.resize(count); }
    };
    if (!repeated) {
        if (!cd.def_levels.empty()) { cut(cd.def_levels, drop, keep); }
        cut(cd.i32, drop, keep); cut(cd.i64, drop, keep);
        cut(cd.f64, drop, keep); cut(cd.str, drop, keep);
        return;
    }

    // Slot range covering rows [drop, drop + keep).
    const size_t n = cd.rep_levels.size();
    size_t slot_lo = n, slot_hi = n, row = 0;
    for (size_t i = 0; i < n; ++i) {
        if (cd.rep_levels[i] == 0) {
            if (row == drop) { slot_lo = i; }
            if (row == drop + keep) { slot_hi = i; break; }
            ++row;
        }
    }
    if (slot_lo == n) { slot_lo = n; }
    if (row < drop + keep) { slot_hi = n; }

    // Values are present-only, so count how many fall before and inside.
    const bool has_def = !cd.def_levels.empty();
    auto present_in = [&] (size_t from, size_t to) {
        if (!has_def) { return to - from; }
        size_t maxd = 0;
        for (auto d : cd.def_levels) { maxd = std::max<size_t>(maxd, d); }
        size_t k = 0;
        for (size_t i = from; i < to && i < cd.def_levels.size(); ++i) {
            if (cd.def_levels[i] == maxd) { ++k; }
        }
        return k;
    };
    const size_t vdrop = present_in(0, slot_lo);
    const size_t vkeep = present_in(slot_lo, slot_hi);

    cut(cd.rep_levels, slot_lo, slot_hi - slot_lo);
    if (has_def) { cut(cd.def_levels, slot_lo, slot_hi - slot_lo); }
    cut(cd.i32, vdrop, vkeep); cut(cd.i64, vdrop, vkeep);
    cut(cd.f64, vdrop, vkeep); cut(cd.str, vdrop, vkeep);
}

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

// Decodes rows [row_lo, row_hi) of one row group, reading and decompressing only
// the pages that intersect that range. `base_offset` is the file offset that
// image[0] corresponds to, so a caller can pass a slice of the file rather than
// the whole thing -- which is the entire point: this is what lets a point read
// touch one page instead of one file.
std::vector<column_data> decode_columns(std::span<const column_input> in,
                                        const file_metadata& md, size_t rg_index,
                                        int64_t row_lo, int64_t row_hi) {
    if (rg_index >= md.row_groups.size()) { throw decode_error("row group index out of range"); }
    const auto& rg = md.row_groups[rg_index];
    if (row_lo < 0) { row_lo = 0; }
    if (row_hi > rg.num_rows) { row_hi = rg.num_rows; }
    if (row_hi < row_lo) { row_hi = row_lo; }

    // Levels come from the schema *tree*: a leaf's own repetition type does not
    // determine them once it sits inside a repeated group.
    auto leaves = walk_leaves(md);
    if (leaves.size() != rg.columns.size()) {
        throw decode_error("row group chunk count does not match the schema");
    }

    std::vector<column_data> out(rg.columns.size());
    std::vector<int64_t> first_row_decoded(rg.columns.size(), -1);

    for (size_t c = 0; c < rg.columns.size(); ++c) {
        const auto& cc = rg.columns[c];
        if (!cc.meta) { throw decode_error("column chunk without metadata"); }
        const auto& cm = *cc.meta;
        const uint8_t max_def = leaves[c].max_def;
        const uint8_t max_rep = leaves[c].max_rep;
        const bool optional = max_def > 0;
        const bool repeated = max_rep > 0;

        // The dictionary page stays as raw bytes with a view table over it; see
        // index_plain_byte_array() for why materialising it was the single
        // largest cost in a point read.
        std::vector<uint8_t> dict_raw;
        std::vector<std::string_view> dict_ba;
        std::vector<int32_t> dict_i32;
        std::vector<int64_t> dict_i64;
        std::vector<double>  dict_f64;

        if (c >= in.size()) { throw decode_error("missing column input"); }
        const auto& ci = in[c];

        // The dictionary page, when it is supplied separately. A caller that
        // hands over the whole chunk leaves `dict` empty and lets the page walk
        // below find it, which is the same bytes either way.
        if (!ci.dict.empty()) {
            size_t dconsumed = 0;
            auto dph = parse_page_header(ci.dict, dconsumed);
            if (dph.type != page_type::dictionary_page || !dph.dict) {
                throw decode_error("dictionary span does not start with a dictionary page");
            }
            auto dbody = ci.dict.subspan(dconsumed, size_t(dph.compressed_page_size));
            auto raw = decompress(dbody, cm.compression, size_t(dph.uncompressed_page_size));
            const size_t n = size_t(dph.dict->num_values);
            switch (cm.type) {
            case phys_type::int32:      dict_i32 = decode_plain<int32_t>(raw, n); break;
            case phys_type::int64:      dict_i64 = decode_plain<int64_t>(raw, n); break;
            case phys_type::dbl:        dict_f64 = decode_plain<double>(raw, n); break;
            case phys_type::byte_array:
                dict_raw = std::move(raw);
                dict_ba = index_plain_byte_array(dict_raw, n);
                break;
            default: throw decode_error("unsupported dictionary type");
            }
        }

        const std::span<const uint8_t> image = ci.pages;
        int64_t off = 0;
        const int64_t end = int64_t(image.size());

        // Row cursor within this row group, advanced by every data page whether
        // or not it is decoded. It starts wherever the supplied page run starts.
        int64_t row_at = ci.first_row;
        int64_t produced = 0;
        while (off < end && row_at < row_hi) {
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
                case phys_type::byte_array:
                    dict_raw = std::move(raw);
                    dict_ba = index_plain_byte_array(dict_raw, n);
                    break;
                default: throw decode_error("unsupported dictionary type");
                }
            } else if (ph.type == page_type::data_page_v2 && ph.v2) {
                const auto& h = *ph.v2;
                const size_t n = size_t(h.num_values);
                // V2 headers carry num_rows, so a page outside the window can be
                // stepped over without decompressing it. This is the whole
                // mechanism behind bounded point reads.
                const int64_t page_rows = h.num_rows > 0 ? int64_t(h.num_rows) : int64_t(n);
                if (row_at + page_rows <= row_lo || row_at >= row_hi) {
                    row_at += page_rows;
                    produced += int64_t(n);
                    if (ph.compressed_page_size <= 0) { throw decode_error("non-positive page size"); }
                    off = body_at + ph.compressed_page_size;
                    continue;
                }
                if (first_row_decoded[c] < 0) { first_row_decoded[c] = row_at; }
                row_at += page_rows;
                const size_t rl = size_t(h.repetition_levels_byte_length);
                const size_t dl = size_t(h.definition_levels_byte_length);
                if (rl + dl > body.size()) { throw decode_error("level lengths exceed page body"); }

                if (repeated) {
                    rle_decoder rd(body.subspan(0, rl), bit_width_for(max_rep));
                    auto reps = rd.decode_all(n);
                    if (reps.size() != n) { throw decode_error("short repetition level stream"); }
                    out[c].rep_levels.insert(out[c].rep_levels.end(), reps.begin(), reps.end());
                }
                std::vector<uint64_t> levels;
                if (optional) {
                    rle_decoder ld(body.subspan(rl, dl), bit_width_for(max_def));
                    levels = ld.decode_all(n);
                    if (levels.size() != n) { throw decode_error("short definition level stream"); }
                    out[c].def_levels.insert(out[c].def_levels.end(), levels.begin(), levels.end());
                }

                // V2 keeps levels outside the compressed region. The page's own
                // is_compressed flag wins over the chunk codec: parquet-cpp
                // leaves a page raw when compression does not pay, and honouring
                // only the chunk codec makes those pages fail to decode.
                auto vbody = body.subspan(rl + dl);
                const size_t uncompressed_values = size_t(ph.uncompressed_page_size) - rl - dl;
                auto raw = decompress(vbody, h.is_compressed ? cm.compression : codec::uncompressed,
                                      uncompressed_values);

                const size_t present = optional
                        ? size_t(std::count(levels.begin(), levels.end(), uint64_t(max_def))) : n;
                const size_t before = out[c].num_values();
                append_values(out[c], cm.type, h.value_encoding, raw, present,
                              dict_ba, dict_i32, dict_i64, dict_f64);
                // Densifying to one value per *slot* only makes sense when a slot
                // is a row. Under repetition the caller has to walk the levels, so
                // the values stay as the file has them: present only.
                if (optional && !repeated) {
                    expand_nulls(out[c], cm.type, before, n, levels);
                }
                produced += int64_t(n);
            } else {
                throw decode_error("only V2 data pages are supported on the read path");
            }

            if (ph.compressed_page_size <= 0) { throw decode_error("non-positive page size"); }
            off = body_at + ph.compressed_page_size;
        }
        const bool whole_chunk = row_lo == 0 && row_hi == rg.num_rows;
        if (whole_chunk && produced != cm.num_values) {
            throw decode_error("decoded " + std::to_string(produced) + " values but the chunk "
                               "declares " + std::to_string(cm.num_values));
        }
    }

    // Pages are decoded whole, so a column may start before row_lo, and columns
    // need not share page boundaries. Trim every column to exactly [row_lo,
    // row_hi) so the caller gets aligned rows. expand_nulls has already made the
    // value vectors dense, one entry per row, so this is a plain slice.
    for (size_t c = 0; c < out.size(); ++c) {
        const int64_t start = first_row_decoded[c] < 0 ? row_lo : first_row_decoded[c];
        const size_t drop = size_t(row_lo - start);
        const size_t keep = size_t(row_hi - row_lo);
        trim(out[c], drop, keep, leaves[c].max_rep > 0);
    }
    return out;
}

std::vector<column_data> read_row_range(std::span<const uint8_t> image, int64_t base_offset,
                                        const file_metadata& md, size_t rg_index,
                                        int64_t row_lo, int64_t row_hi) {
    if (rg_index >= md.row_groups.size()) { throw decode_error("row group index out of range"); }
    const auto& rg = md.row_groups[rg_index];
    std::vector<column_input> in(rg.columns.size());
    for (size_t c = 0; c < rg.columns.size(); ++c) {
        const auto& cc = rg.columns[c];
        if (!cc.meta) { throw decode_error("column chunk without metadata"); }
        const auto& cm = *cc.meta;
        const int64_t start = (cm.dictionary_page_offset ? *cm.dictionary_page_offset
                                                         : cm.data_page_offset) - base_offset;
        const int64_t end = start + cm.total_compressed_size;
        if (start < 0 || size_t(end) > image.size()) { throw decode_error("chunk extends past EOF"); }
        // Whole chunk in one span: the page walk finds the dictionary itself.
        in[c].pages = image.subspan(size_t(start), size_t(end - start));
        in[c].first_row = 0;
    }
    return decode_columns(in, md, rg_index, row_lo, row_hi);
}

std::vector<column_data> read_row_group(std::span<const uint8_t> image,
                                        const file_metadata& md,
                                        size_t rg_index) {
    if (rg_index >= md.row_groups.size()) { throw decode_error("row group index out of range"); }
    return read_row_range(image, 0, md, rg_index, 0, md.row_groups[rg_index].num_rows);
}

std::vector<column_data> read_file(std::span<const uint8_t> image) {
    auto md = parse_footer(image);
    if (md.row_groups.empty()) { return {}; }
    return read_row_group(image, md, 0);
}

} // namespace sstables::parquet::format
