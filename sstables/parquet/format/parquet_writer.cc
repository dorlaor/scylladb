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

parquet_file_writer::parquet_file_writer(std::vector<column_spec> schema, writer_options opt)
        : _schema(std::move(schema)), _opt(opt) {
    _buf.insert(_buf.end(), {'P', 'A', 'R', '1'});
    // Synthesise the tree a flat schema implies, so footer emission has one path.
    schema_element root;
    root.name = "schema";
    root.num_children = int32_t(_schema.size());
    _tree.push_back(root);
    for (const auto& c : _schema) {
        schema_element e;
        e.type = c.type;
        e.repetition_type = c.rep;
        e.name = c.name;
        e.converted_type = c.converted_type;
        _tree.push_back(e);
    }
}

parquet_file_writer::parquet_file_writer(nested_schema ns, writer_options opt)
        : _tree(std::move(ns.tree)), _opt(opt) {
    _buf.insert(_buf.end(), {'P', 'A', 'R', '1'});
    if (_tree.empty()) { throw std::runtime_error("writer: nested schema has no root"); }

    // Derive the leaves and their levels with the same walker the reader uses, so
    // the two cannot disagree about what a level means.
    file_metadata probe;
    probe.schema = _tree;
    for (const auto& li : walk_leaves(probe)) {
        const auto& el = _tree[li.index];
        if (!el.type) { throw std::runtime_error("writer: leaf without a physical type"); }
        column_spec c;
        c.name = el.name;
        c.type = *el.type;
        c.rep = el.repetition_type.value_or(repetition::required);
        c.converted_type = el.converted_type;
        c.max_def = li.max_def;
        c.max_rep = li.max_rep;
        c.path = li.path;
        _schema.push_back(std::move(c));
    }
}

void parquet_file_writer::write_column_chunk(const column_spec& spec, const column_data& col,
                                     chunk_meta& out_meta) {
    // Slots, not values. Parquet's ColumnMetaData.num_values counts level
    // entries, and for a repeated column there are more of those than values --
    // reporting the value count makes readers stop early with no error.
    const size_t n = !col.def_levels.empty() ? col.def_levels.size()
                   : (!col.rep_levels.empty() ? col.rep_levels.size() : col.num_values());
    // A leaf inside a repeated group states its levels explicitly, because the
    // schema tree they come from is not visible here. A flat leaf derives them.
    const uint8_t max_rep = spec.max_rep;
    const uint8_t max_def = spec.max_def ? spec.max_def
                                        : uint8_t(spec.rep == repetition::optional ? 1 : 0);
    const bool has_def = max_def > 0;

    out_meta.cm.type = spec.type;
    out_meta.cm.path_in_schema = spec.path_or_name();
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
            if (!has_def || col.def_levels[i] == max_def) { present.push_back(col.str[i]); }
        }
        dict = encode_dictionary_byte_array(present);
        // Dictionary only pays if it is small relative to the data.
        // Cardinality has to be well below the row count, not merely below it.
        // A dictionary is decompressed in full before a single value can be
        // decoded, so a near-unique dictionary is pure cost on the read path --
        // it dominated point-read latency at the old 2x threshold (design doc
        // 10.4) while saving almost nothing, because zstd already finds those
        // repeats. 8x keeps the low-cardinality columns a dictionary is for.
        if (dict.dictionary_page.size() <= _opt.dictionary_max_bytes &&
            dict.num_distinct * _opt.dictionary_min_repeat < present.size()) {
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
    int64_t rows_written = 0;
    size_t val_cursor = 0;   // next present value, for repeated columns   // first_row_index of the next page

    // A page must never split a row, so with repetition the cut points are the
    // rep_level==0 positions rather than every `page_values`-th value.
    auto page_end = [&] (size_t from) {
        size_t want = std::min(from + page_sz, n);
        if (max_rep == 0 || want >= n) { return want; }
        // Walk back to the start of the row `want` lands inside; never emit an
        // empty page, so if that walk reaches `from`, walk forward instead.
        size_t e = want;
        while (e > from && col.rep_levels[e] != 0) { --e; }
        if (e == from) {
            e = want;
            while (e < n && col.rep_levels[e] != 0) { ++e; }
        }
        return e;
    };

    for (size_t off = 0; off < n || (n == 0 && off == 0); ) {
        if (n == 0) { break; }
        const size_t stop = page_end(off);
        const size_t cnt = stop - off;

        // definition and repetition levels (never compressed in V2)
        std::vector<uint8_t> def_bytes, rep_bytes;
        int32_t page_nulls = 0;
        if (has_def) {
            std::span<const uint64_t> lv(col.def_levels.data() + off, cnt);
            for (auto x : lv) { if (x < max_def) { ++page_nulls; } }
            def_bytes = encode_levels_v2(lv, max_def);
        }
        if (max_rep > 0) {
            std::span<const uint64_t> rv(col.rep_levels.data() + off, cnt);
            rep_bytes = encode_levels_v2(rv, max_rep);
        }
        null_count += page_nulls;

        // Where this page's values start. A flat column supplies one value per
        // slot, so the slot index doubles as the value index; a repeated one
        // supplies present values only, so they have to be counted.
        const size_t vbase = max_rep == 0 ? off : val_cursor;
        size_t page_present = 0;
        for (size_t i = 0; i < cnt; ++i) {
            if (!has_def || col.def_levels[off + i] == max_def) { ++page_present; }
        }

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
                size_t vi = vbase;
                    for (size_t i = 0; i < cnt; ++i) {
                        if (!has_def || col.def_levels[off + i] == max_def) {
                            present.push_back(col.i32[vi]);
                        }
                        if (max_rep == 0 || !has_def || col.def_levels[off + i] == max_def) { ++vi; }
                    }
                encode_plain<int32_t>(body, present);
                break;
            }
            case phys_type::int64: {
                std::vector<int64_t> present;
                present.reserve(cnt);
                size_t vi = vbase;
                    for (size_t i = 0; i < cnt; ++i) {
                        if (!has_def || col.def_levels[off + i] == max_def) {
                            present.push_back(col.i64[vi]);
                        }
                        if (max_rep == 0 || !has_def || col.def_levels[off + i] == max_def) { ++vi; }
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
                size_t vi = vbase;
                    for (size_t i = 0; i < cnt; ++i) {
                        if (!has_def || col.def_levels[off + i] == max_def) {
                            present.push_back(col.f64[vi]);
                        }
                        if (max_rep == 0 || !has_def || col.def_levels[off + i] == max_def) { ++vi; }
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
                size_t vi = vbase;
                    for (size_t i = 0; i < cnt; ++i) {
                        if (!has_def || col.def_levels[off + i] == max_def) {
                            present.push_back(col.str[vi]);
                        }
                        if (max_rep == 0 || !has_def || col.def_levels[off + i] == max_def) { ++vi; }
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
        const size_t lvl_bytes = def_bytes.size() + rep_bytes.size();
        const int32_t uncompressed = int32_t(lvl_bytes + body.size());
        const int32_t compressed   = int32_t(lvl_bytes + comp.size());

        // num_rows counts rows, not values; they differ once a column repeats.
        int32_t page_rows = int32_t(cnt);
        if (max_rep > 0) {
            page_rows = 0;
            for (size_t i = 0; i < cnt; ++i) {
                if (col.rep_levels[off + i] == 0) { ++page_rows; }
            }
        }

        std::vector<uint8_t> hdr;
        write_data_page_v2_header(hdr, uncompressed, compressed,
                                  int32_t(cnt), page_nulls, page_rows,
                                  used, int32_t(def_bytes.size()),
                                  int32_t(rep_bytes.size()),
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
        rows_written += int64_t(page_rows);
        _buf.insert(_buf.end(), hdr.begin(), hdr.end());
        // Spec order: repetition levels first, then definition levels.
        _buf.insert(_buf.end(), rep_bytes.begin(), rep_bytes.end());
        _buf.insert(_buf.end(), def_bytes.begin(), def_bytes.end());
        _buf.insert(_buf.end(), comp.begin(), comp.end());

        total_uncompressed += uncompressed + int64_t(hdr.size());
        total_compressed   += compressed   + int64_t(hdr.size());

        if (std::find(out_meta.cm.encodings.begin(), out_meta.cm.encodings.end(), used)
            == out_meta.cm.encodings.end()) {
            out_meta.cm.encodings.push_back(used);
        }
        val_cursor += page_present;
        off = stop;
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

void parquet_file_writer::add_row_group(std::span<const column_data> cols) {
    if (cols.size() != _schema.size()) {
        throw std::runtime_error("writer: column count does not match schema");
    }
    rg_meta rg;
    // Rows, not values: a repeated column holds more values than rows, so the
    // consistency check has to count rows or it rejects every nested schema.
    rg.num_rows = cols.empty() ? 0 : int64_t(cols[0].num_rows());
    for (size_t i = 0; i < cols.size(); ++i) {
        if (int64_t(cols[i].num_rows()) != rg.num_rows) {
            throw std::runtime_error("writer: ragged row group (column '" + _schema[i].name +
                                     "': " + std::to_string(cols[i].num_rows()) + " rows, expected " +
                                     std::to_string(rg.num_rows) + ")");
        }
        chunk_meta cmeta;
        write_column_chunk(_schema[i], cols[i], cmeta);
        rg.total_byte_size += cmeta.cm.total_uncompressed_size;
        rg.chunks.push_back(std::move(cmeta));
    }
    _num_rows += rg.num_rows;
    _rgs.push_back(std::move(rg));
}

void parquet_file_writer::write_page_indexes() {
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

void parquet_file_writer::write_footer() {
    std::vector<uint8_t> meta;
    compact_writer w(meta);
    {
        compact_writer::struct_scope s(w);
        w.field_i32(1, 2);                          // version
        // --- schema: flat, root first
        w.field_list(2, ctype::strct, _tree.size());
        // Straight out of _tree, so a nested schema needs no special case: a group
        // has num_children and no physical type, a leaf the other way round.
        for (const auto& el : _tree) {
            compact_writer::elem_scope e(w);
            if (el.type)            { w.field_i32(1, int32_t(*el.type)); }
            if (el.repetition_type) { w.field_i32(3, int32_t(*el.repetition_type)); }
            w.field_binary(4, el.name);
            if (el.num_children)    { w.field_i32(5, *el.num_children); }
            if (el.converted_type)  { w.field_i32(6, *el.converted_type); }
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

std::vector<uint8_t> parquet_file_writer::finish() {
    write_page_indexes();
    write_footer();
    return std::move(_buf);
}

} // namespace sstables::parquet::format
