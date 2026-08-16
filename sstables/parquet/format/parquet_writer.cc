/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "parquet_writer.hh"
#include "page_header.hh"
#include "thrift_compact_writer.hh"
#include "encoders.hh"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <zstd.h>

namespace sstables::parquet::format {

namespace {

std::vector<uint8_t> compress(const std::vector<uint8_t>& in, codec c, int level) {
    if (c == codec::uncompressed || in.empty()) { return in; }
    if (c != codec::zstd) { throw std::runtime_error("writer: only zstd/uncompressed for now"); }
    size_t bound = ZSTD_compressBound(in.size());
    std::vector<uint8_t> out(bound);
    size_t n = ZSTD_compress(out.data(), bound, in.data(), in.size(), level);
    if (ZSTD_isError(n)) { throw std::runtime_error(std::string("zstd: ") + ZSTD_getErrorName(n)); }
    out.resize(n);
    return out;
}

// PageHeader for a V2 data page. Field ids per parquet.thrift.
void write_data_page_v2_header(std::vector<uint8_t>& out,
                               int32_t uncompressed, int32_t compressed,
                               int32_t num_values, int32_t num_nulls, int32_t num_rows,
                               encoding value_enc, int32_t def_len, int32_t rep_len,
                               bool is_compressed) {
    compact_writer w(out);
    compact_writer::struct_scope s(w);
    w.field_i32(1, int32_t(page_type::data_page_v2));
    w.field_i32(2, uncompressed);
    w.field_i32(3, compressed);
    w.field_struct(8);
    {
        compact_writer::elem_scope v2(w);
        w.field_i32(1, num_values);
        w.field_i32(2, num_nulls);
        w.field_i32(3, num_rows);
        w.field_i32(4, int32_t(value_enc));
        w.field_i32(5, def_len);
        w.field_i32(6, rep_len);
        w.field_bool(7, is_compressed);
    }
}

void write_dictionary_page_header(std::vector<uint8_t>& out,
                                  int32_t uncompressed, int32_t compressed,
                                  int32_t num_values) {
    compact_writer w(out);
    compact_writer::struct_scope s(w);
    w.field_i32(1, int32_t(page_type::dictionary_page));
    w.field_i32(2, uncompressed);
    w.field_i32(3, compressed);
    w.field_struct(7);
    {
        compact_writer::elem_scope d(w);
        w.field_i32(1, num_values);
        w.field_i32(2, int32_t(encoding::plain));
    }
}

} // namespace

void file_writer::write_column_chunk(const column_spec& spec, const column_data& col,
                                     chunk_meta& out_meta) {
    const size_t n = col.num_values();
    const bool optional = spec.rep == repetition::optional;
    const uint8_t max_def = optional ? 1 : 0;

    out_meta.cm.type = spec.type;
    out_meta.cm.path_in_schema = {spec.name};
    out_meta.cm.compression = _opt.compression;
    out_meta.cm.num_values = int64_t(n);
    out_meta.first_page_offset = int64_t(_buf.size());

    // ---- decide encoding
    bool use_dict = false;
    dict_result dict;
    if (_opt.use_dictionary && spec.type == phys_type::byte_array && !col.str.empty()) {
        // Only the present values go into the dictionary.
        std::vector<std::string> present;
        present.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            if (!optional || col.def_levels[i]) { present.push_back(col.str[i]); }
        }
        dict = encode_dictionary_byte_array(present);
        // Dictionary only pays if it is small relative to the data.
        if (dict.dictionary_page.size() <= _opt.dictionary_max_bytes &&
            dict.num_distinct * 2 < present.size()) {
            use_dict = true;
        }
    }

    int64_t total_uncompressed = 0, total_compressed = 0;
    int64_t null_count = 0;

    if (use_dict) {
        out_meta.cm.dictionary_page_offset = int64_t(_buf.size());
        auto comp = compress(dict.dictionary_page, _opt.compression, _opt.zstd_level);
        std::vector<uint8_t> hdr;
        write_dictionary_page_header(hdr, int32_t(dict.dictionary_page.size()),
                                     int32_t(comp.size()), int32_t(dict.num_distinct));
        _buf.insert(_buf.end(), hdr.begin(), hdr.end());
        _buf.insert(_buf.end(), comp.begin(), comp.end());
        total_uncompressed += int64_t(dict.dictionary_page.size() + hdr.size());
        total_compressed   += int64_t(comp.size() + hdr.size());
        out_meta.cm.encodings.push_back(encoding::plain);
        out_meta.cm.encodings.push_back(encoding::rle_dictionary);
    }

    // ---- data pages
    // The dictionary index stream was produced for the whole chunk, so when the
    // dictionary is in use the chunk is emitted as a single page. Splitting it
    // would require re-running the index encoder per page; not worth it here,
    // and noted as a follow-up.
    const size_t page_sz = use_dict ? n : _opt.page_values;
    bool first_data_page = true;
    int64_t rows_written = 0;   // first_row_index of the next page

    for (size_t off = 0; off < n || (n == 0 && off == 0); off += page_sz) {
        const size_t cnt = std::min(page_sz, n - off);
        if (n == 0) { break; }

        // definition levels (never compressed in V2)
        std::vector<uint8_t> def_bytes;
        int32_t page_nulls = 0;
        if (optional) {
            std::span<const uint64_t> lv(col.def_levels.data() + off, cnt);
            for (auto x : lv) { if (!x) { ++page_nulls; } }
            def_bytes = encode_levels_v2(lv, max_def);
        }
        null_count += page_nulls;

        // values (present only)
        std::vector<uint8_t> body;
        encoding used = encoding::plain;
        if (use_dict) {
            body = dict.index_page;
            used = encoding::rle_dictionary;
        } else {
            switch (spec.type) {
            case phys_type::int32: {
                std::vector<int32_t> present;
                present.reserve(cnt);
                for (size_t i = 0; i < cnt; ++i) {
                    if (!optional || col.def_levels[off + i]) { present.push_back(col.i32[off + i]); }
                }
                encode_plain<int32_t>(body, present);
                break;
            }
            case phys_type::int64: {
                std::vector<int64_t> present;
                present.reserve(cnt);
                for (size_t i = 0; i < cnt; ++i) {
                    if (!optional || col.def_levels[off + i]) { present.push_back(col.i64[off + i]); }
                }
                if (spec.preferred && *spec.preferred == encoding::delta_binary_packed) {
                    encode_delta_binary_packed(body, present);
                    used = encoding::delta_binary_packed;
                } else {
                    encode_plain<int64_t>(body, present);
                }
                break;
            }
            case phys_type::dbl: {
                std::vector<double> present;
                present.reserve(cnt);
                for (size_t i = 0; i < cnt; ++i) {
                    if (!optional || col.def_levels[off + i]) { present.push_back(col.f64[off + i]); }
                }
                if (spec.preferred && *spec.preferred == encoding::byte_stream_split) {
                    encode_byte_stream_split<double>(body, present);
                    used = encoding::byte_stream_split;
                } else {
                    encode_plain<double>(body, present);
                }
                break;
            }
            case phys_type::byte_array: {
                std::vector<std::string> present;
                present.reserve(cnt);
                for (size_t i = 0; i < cnt; ++i) {
                    if (!optional || col.def_levels[off + i]) { present.push_back(col.str[off + i]); }
                }
                encode_plain_byte_array(body, present);
                break;
            }
            default:
                throw std::runtime_error("writer: unsupported physical type");
            }
        }

        auto comp = compress(body, _opt.compression, _opt.zstd_level);
        // In V2 the level bytes sit before the (compressed) values and are
        // counted in both page sizes but never compressed.
        const int32_t uncompressed = int32_t(def_bytes.size() + body.size());
        const int32_t compressed   = int32_t(def_bytes.size() + comp.size());

        std::vector<uint8_t> hdr;
        write_data_page_v2_header(hdr, uncompressed, compressed,
                                  int32_t(cnt), page_nulls, int32_t(cnt),
                                  used, int32_t(def_bytes.size()), 0,
                                  _opt.compression != codec::uncompressed);
        const int64_t page_start = int64_t(_buf.size());
        if (first_data_page) {
            out_meta.cm.data_page_offset = page_start;
            first_data_page = false;
        }
        // PageLocation.offset points at the page header, and the size covers
        // header + body -- that is what the spec means by "the page".
        out_meta.pages.push_back(page_location{
                page_start,
                int32_t(hdr.size()) + compressed,
                int64_t(rows_written)});
        rows_written += int64_t(cnt);
        _buf.insert(_buf.end(), hdr.begin(), hdr.end());
        _buf.insert(_buf.end(), def_bytes.begin(), def_bytes.end());
        _buf.insert(_buf.end(), comp.begin(), comp.end());

        total_uncompressed += uncompressed + int64_t(hdr.size());
        total_compressed   += compressed   + int64_t(hdr.size());

        if (std::find(out_meta.cm.encodings.begin(), out_meta.cm.encodings.end(), used)
            == out_meta.cm.encodings.end()) {
            out_meta.cm.encodings.push_back(used);
        }
    }

    if (first_data_page) {   // zero-row column: still needs a valid offset
        out_meta.cm.data_page_offset = int64_t(_buf.size());
    }
    out_meta.cm.total_uncompressed_size = total_uncompressed;
    out_meta.cm.total_compressed_size   = total_compressed;
    if (_opt.write_statistics) {
        statistics st;
        st.null_count = null_count;
        out_meta.cm.stats = st;
    }
}

void file_writer::add_row_group(std::span<const column_data> cols) {
    if (cols.size() != _schema.size()) {
        throw std::runtime_error("writer: column count does not match schema");
    }
    rg_meta rg;
    rg.num_rows = cols.empty() ? 0 : int64_t(cols[0].num_values());
    for (size_t i = 0; i < cols.size(); ++i) {
        if (int64_t(cols[i].num_values()) != rg.num_rows) {
            throw std::runtime_error("writer: ragged row group (column '" + _schema[i].name + "')");
        }
        chunk_meta cmeta;
        write_column_chunk(_schema[i], cols[i], cmeta);
        rg.total_byte_size += cmeta.cm.total_uncompressed_size;
        rg.chunks.push_back(std::move(cmeta));
    }
    _num_rows += rg.num_rows;
    _rgs.push_back(std::move(rg));
}

void file_writer::write_page_indexes() {
    if (!_opt.write_page_index) { return; }
    for (auto& rg : _rgs) {
        for (auto& ch : rg.chunks) {
            if (ch.pages.empty()) { continue; }
            std::vector<uint8_t> blob;
            {
                compact_writer w(blob);
                compact_writer::struct_scope oi(w);          // OffsetIndex
                w.field_list(1, ctype::strct, ch.pages.size());
                for (const auto& pl : ch.pages) {
                    compact_writer::elem_scope e(w);          // PageLocation
                    w.field_i64(1, pl.offset);
                    w.field_i32(2, pl.compressed_page_size);
                    w.field_i64(3, pl.first_row_index);
                }
            }
            ch.offset_index_offset = int64_t(_buf.size());
            ch.offset_index_length = int32_t(blob.size());
            _buf.insert(_buf.end(), blob.begin(), blob.end());
        }
    }
}

void file_writer::write_footer() {
    std::vector<uint8_t> meta;
    compact_writer w(meta);
    {
        compact_writer::struct_scope s(w);
        w.field_i32(1, 2);                          // version
        // --- schema: flat, root first
        w.field_list(2, ctype::strct, _schema.size() + 1);
        {
            compact_writer::elem_scope root(w);
            w.field_binary(4, "schema");
            w.field_i32(5, int32_t(_schema.size()));   // num_children
        }
        for (const auto& c : _schema) {
            compact_writer::elem_scope e(w);
            w.field_i32(1, int32_t(c.type));
            w.field_i32(3, int32_t(c.rep));
            w.field_binary(4, c.name);
            if (c.converted_type) { w.field_i32(6, *c.converted_type); }
        }
        w.field_i64(3, _num_rows);
        // --- row groups
        w.field_list(4, ctype::strct, _rgs.size());
        for (const auto& rg : _rgs) {
            compact_writer::elem_scope e(w);
            w.field_list(1, ctype::strct, rg.chunks.size());
            for (const auto& ch : rg.chunks) {
                compact_writer::elem_scope ce(w);
                w.field_i64(2, ch.first_page_offset);       // file_offset
                w.field_struct(3);
                {
                    compact_writer::elem_scope cm(w);
                    const auto& m = ch.cm;
                    w.field_i32(1, int32_t(m.type));
                    w.field_list(2, ctype::i32, m.encodings.size());
                    for (auto en : m.encodings) { w.zigzag(int32_t(en)); }
                    w.field_list(3, ctype::binary, m.path_in_schema.size());
                    for (const auto& p : m.path_in_schema) { w.uvarint(p.size()); w.raw(p.data(), p.size()); }
                    w.field_i32(4, int32_t(m.compression));
                    w.field_i64(5, m.num_values);
                    w.field_i64(6, m.total_uncompressed_size);
                    w.field_i64(7, m.total_compressed_size);
                    w.field_i64(9, m.data_page_offset);
                    if (m.dictionary_page_offset) { w.field_i64(11, *m.dictionary_page_offset); }
                    if (m.stats && m.stats->null_count) {
                        w.field_struct(12);
                        compact_writer::elem_scope st(w);
                        w.field_i64(3, *m.stats->null_count);
                    }
                }
                // ColumnChunk.offset_index_offset / _length come after meta_data
                // because Thrift field ids must be written in ascending order.
                if (ch.offset_index_offset) { w.field_i64(4, *ch.offset_index_offset); }
                if (ch.offset_index_length) { w.field_i32(5, *ch.offset_index_length); }
            }
            w.field_i64(2, rg.total_byte_size);
            w.field_i64(3, rg.num_rows);
        }
        // --- key/value metadata
        if (!_kv.empty()) {
            w.field_list(5, ctype::strct, _kv.size());
            for (const auto& kv : _kv) {
                compact_writer::elem_scope e(w);
                w.field_binary(1, kv.first);
                w.field_binary(2, kv.second);
            }
        }
        w.field_binary(6, "scylladb-parquet (sstables/parquet/format)");
    }
    _buf.insert(_buf.end(), meta.begin(), meta.end());
    uint32_t len = uint32_t(meta.size());
    const uint8_t* lp = reinterpret_cast<const uint8_t*>(&len);
    _buf.insert(_buf.end(), lp, lp + 4);
    _buf.insert(_buf.end(), {'P', 'A', 'R', '1'});
}

std::vector<uint8_t> file_writer::finish() {
    write_page_indexes();
    write_footer();
    return std::move(_buf);
}

} // namespace sstables::parquet::format
