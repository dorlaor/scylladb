/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "lance_reader.hh"

namespace sstables::lance::format {

lance_file_view::lance_file_view(std::span<const uint8_t> image, metadata_limits lim)
    : _image(image)
    , _lim(lim) {
    _footer = parse_footer(image);

    auto table_bytes = [&](uint64_t off, uint32_t entries) {
        if (off > image.size() || image.size() - off < uint64_t(entries) * 16) {
            throw lance_error("offset table out of range");
        }
        return image.subspan(size_t(off), size_t(entries) * 16);
    };
    auto gbo = parse_offset_table(table_bytes(_footer.gbo_offset, _footer.num_global_buffers),
                                  _footer.num_global_buffers);
    if (gbo.empty()) {
        throw lance_error("no global buffers: the schema is missing");
    }
    _fd = parse_file_descriptor(std::string_view(buffer(gbo[0])), _lim);

    auto cmo = parse_offset_table(table_bytes(_footer.cmo_offset, _footer.num_columns),
                                  _footer.num_columns);
    _columns.reserve(cmo.size());
    for (const auto& ref : cmo) {
        _columns.push_back(parse_column_meta(std::string_view(buffer(ref)), _lim));
    }
}

std::string_view lance_file_view::buffer(const buffer_ref& b) const {
    if (b.offset > _image.size() || _image.size() - b.offset < b.size) {
        throw lance_error("buffer out of file range");
    }
    return {reinterpret_cast<const char*>(_image.data()) + b.offset, size_t(b.size)};
}

void slice_values(column_values& v, uint64_t first, uint64_t keep_from, uint64_t keep_to) {
    const uint64_t rows = v.rows();
    if (keep_from < first || keep_to > first + rows || keep_from > keep_to) {
        throw lance_error("slice outside the decoded range");
    }
    const size_t a = size_t(keep_from - first);
    const size_t b = size_t(keep_to - first);
    auto cut = [&](auto& vec) {
        if (vec.empty()) { return; }
        vec.erase(vec.begin() + b, vec.end());
        vec.erase(vec.begin(), vec.begin() + a);
    };
    cut(v.def);
    cut(v.i32);
    cut(v.i64);
    cut(v.f64);
    cut(v.str);
}

static void append_values_dst(column_values& dst, column_values&& src, bool dst_may_need_def) {
    const size_t dst_rows = dst.rows();
    const size_t src_rows = src.rows();
    if (dst_may_need_def && (!dst.def.empty() || !src.def.empty())) {
        dst.def.resize(dst_rows, 0);
        src.def.resize(src_rows, 0);
        dst.def.insert(dst.def.end(), src.def.begin(), src.def.end());
    }
    auto app = [](auto& d, auto& s) {
        d.insert(d.end(), std::make_move_iterator(s.begin()), std::make_move_iterator(s.end()));
    };
    app(dst.i32, src.i32);
    app(dst.i64, src.i64);
    app(dst.f64, src.f64);
    app(dst.str, src.str);
}

column_values lance_file_view::read_rows(size_t c, lphys t, uint64_t lo, uint64_t hi) const {
    const auto& cm = column(c);
    column_values out;
    if (lo >= hi) { return out; }
    size_t p = cm.page_for_row(lo);
    for (; p < cm.pages.size(); ++p) {
        const auto& pg = cm.pages[p];
        if (pg.priority >= hi) { break; }
        const uint64_t page_lo = std::max(lo, pg.priority);
        const uint64_t page_hi = std::min(hi, pg.priority + pg.rows);
        if (pg.encoding.any_bytes.empty()) {
            throw lance_error("page without a direct encoding");
        }
        auto pl = parse_page_layout(pg.encoding.any_bytes, _lim);
        column_values part;
        switch (pl.k) {
        case page_layout::kind::miniblock: {
            if (pg.buffers.size() < 2) { throw lance_error("miniblock page needs two buffers"); }
            auto idx = parse_miniblock_index(buffer(pg.buffers[0]), pl.num_items, _lim);
            const size_t c_lo = idx.chunk_for(page_lo - pg.priority);
            const size_t c_hi = idx.chunk_for(page_hi - 1 - pg.priority);
            const auto& first = idx.chunks[c_lo];
            const auto& last = idx.chunks[c_hi];
            auto chunk_bytes = buffer(pg.buffers[1]).substr(
                    size_t(first.byte_offset),
                    size_t(last.byte_offset + last.byte_size - first.byte_offset));
            part = decode_miniblock_chunks(t, pl, idx, c_lo, c_hi - c_lo + 1, chunk_bytes);
            slice_values(part, pg.priority + first.first_value, page_lo, page_hi);
            break;
        }
        case page_layout::kind::fullzip: {
            if (pg.buffers.empty()) { throw lance_error("full-zip page needs a data buffer"); }
            const uint64_t zlo = page_lo - pg.priority;
            const uint64_t zhi = page_hi - pg.priority;
            if (pl.val.k == chan_enc::kind::variable) {
                if (pg.buffers.size() < 2) {
                    throw lance_error("variable full-zip page needs a repetition index");
                }
                auto rep = buffer(pg.buffers[1]);
                const uint32_t w = fullzip_rep_index_width(rep.size(), pg.rows);
                const uint64_t a = read_rep_index_entry(rep, w, zlo);
                const uint64_t b = read_rep_index_entry(rep, w, zhi);
                auto zipped = buffer(pg.buffers[0]);
                if (a > b || b > zipped.size()) { throw lance_error("rep index out of range"); }
                part = decode_fullzip_variable(pl, zlo, zhi, zipped.substr(size_t(a), size_t(b - a)));
            } else {
                const size_t stride = (pl.has_def ? 1 : 0) + pl.val.bits / 8;
                auto zipped = buffer(pg.buffers[0]);
                if (zhi * stride > zipped.size()) { throw lance_error("full-zip buffer truncated"); }
                part = decode_fullzip_fixed(t, pl, zlo, zhi,
                                            zipped.substr(size_t(zlo * stride), size_t((zhi - zlo) * stride)));
            }
            break;
        }
        case page_layout::kind::constant:
            part = decode_constant(t, pl, page_lo - pg.priority, page_hi - pg.priority);
            break;
        }
        // Keep def positional across pages that disagree about having one
        // (a page with no nulls legitimately drops the channel).
        append_values_dst(out, std::move(part), true);
    }
    if (out.rows() != hi - lo) {
        throw lance_error("row range not fully covered by pages");
    }
    return out;
}

} // namespace sstables::lance::format
