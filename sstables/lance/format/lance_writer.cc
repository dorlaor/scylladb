/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "lance_writer.hh"

namespace sstables::lance::format {

// The reference writer aligns page and global buffers to 64 bytes and fills
// the gap with 'H'; readers use only recorded offsets, so the value is
// cosmetic but keeping it identical makes byte-level diffs against pylance
// output tractable.
static constexpr size_t buffer_alignment = 64;
static constexpr char align_pad = 'H';

static const char* default_logical_type(lphys t) {
    switch (t) {
    case lphys::i32: return "int32";
    case lphys::i64: return "int64";
    case lphys::f64: return "double";
    case lphys::bytes: return "binary";
    }
    return "binary";
}

lance_file_writer::lance_file_writer(std::vector<lance_column_spec> specs, writer_options opt, sink out)
    : _specs(std::move(specs))
    , _opt(opt)
    , _out(std::move(out))
    , _pending(_specs.size())
    , _meta(_specs.size()) {
    if (_specs.empty()) {
        throw lance_error("a file needs at least one column");
    }
    // The official reader requires a column-level encoding description even
    // for plain value columns ("Missing ColumnEncoding encoding description");
    // the value it expects is ColumnEncoding{values: Empty}, wrapped in an Any
    // with the 2.0-namespace type url -- 2.1 kept the column-level message.
    static const std::string column_encoding_any = [] {
        pb_writer ce;      // lance.encodings.ColumnEncoding{ values = 1: Empty }
        pb_writer empty;
        ce.msg(1, empty);
        pb_writer any;
        any.len(1, "/lance.encodings.ColumnEncoding");
        any.len(2, ce.str());
        return std::move(any).str();
    }();
    for (auto& cm : _meta) {
        cm.encoding.any_bytes = column_encoding_any;
    }
}

void lance_file_writer::write_raw(std::string_view s) {
    _out(s);
    _offset += s.size();
}

void lance_file_writer::align_to(size_t alignment) {
    static const std::string pad(buffer_alignment, align_pad);
    if (auto rem = _offset % alignment) {
        write_raw(std::string_view(pad).substr(0, alignment - rem));
    }
}

static size_t batch_bytes(lphys t, const column_values& v) {
    if (t == lphys::bytes) {
        size_t n = 0;
        for (const auto& s : v.str) { n += s.size() + 4; }
        return n;
    }
    return v.rows() * (bits_of(t) / 8);
}

static void append_values(lphys t, column_values& dst, column_values&& src) {
    const size_t dst_rows = dst.rows();
    const size_t src_rows = src.rows();
    // The def channel is positional: once either side has one, both need one.
    if (!dst.def.empty() || !src.def.empty()) {
        dst.def.resize(dst_rows, 0);
        if (src.def.empty()) {
            src.def.resize(src_rows, 0);
        }
        dst.def.insert(dst.def.end(), src.def.begin(), src.def.end());
    }
    switch (t) {
    case lphys::i32:
        if (src.i32.size() != src_rows) { src.i32.resize(src_rows, 0); }
        dst.i32.insert(dst.i32.end(), src.i32.begin(), src.i32.end());
        break;
    case lphys::i64:
        if (src.i64.size() != src_rows) { src.i64.resize(src_rows, 0); }
        dst.i64.insert(dst.i64.end(), src.i64.begin(), src.i64.end());
        break;
    case lphys::f64:
        if (src.f64.size() != src_rows) { src.f64.resize(src_rows, 0); }
        dst.f64.insert(dst.f64.end(), src.f64.begin(), src.f64.end());
        break;
    case lphys::bytes:
        if (src.str.size() != src_rows) { src.str.resize(src_rows); }
        dst.str.insert(dst.str.end(), std::make_move_iterator(src.str.begin()),
                       std::make_move_iterator(src.str.end()));
        break;
    }
}

void lance_file_writer::add_batch(std::vector<column_values> cols) {
    if (_finished) { throw lance_error("writer already finished"); }
    if (cols.size() != _specs.size()) {
        throw lance_error("batch column count disagrees with the schema");
    }
    const size_t rows = cols.empty() ? 0 : cols[0].rows();
    for (size_t c = 0; c < cols.size(); ++c) {
        if (cols[c].rows() != rows) {
            throw lance_error("ragged batch: column row counts differ");
        }
    }
    if (rows == 0) { return; }
    for (size_t c = 0; c < cols.size(); ++c) {
        auto& p = _pending[c];
        p.buffered_bytes += batch_bytes(_specs[c].type, cols[c]);
        append_values(_specs[c].type, p.vals, std::move(cols[c]));
        if (p.buffered_bytes >= _opt.page_target_bytes) {
            flush_column(c);
        }
    }
    _rows += rows;
}

void lance_file_writer::flush_column(size_t c) {
    auto& p = _pending[c];
    const size_t rows = p.vals.rows();
    if (rows == 0) { return; }
    encode_options enc = _opt.enc;
    enc.nullable = _specs[c].nullable;
    auto pg = encode_page(_specs[c].type, p.vals, enc);

    page_info pi;
    pi.rows = rows;
    pi.priority = p.first_row;
    pi.encoding.any_bytes = wrap_page_layout_any(pg.layout);
    for (auto& buf : pg.buffers) {
        align_to(buffer_alignment);
        pi.buffers.push_back({_offset, buf.size()});
        write_raw(buf);
    }
    _meta[c].pages.push_back(std::move(pi));

    p.vals = {};
    p.buffered_bytes = 0;
    p.first_row += rows;
}

void lance_file_writer::finish(const std::map<std::string, std::string>& schema_metadata) {
    if (_finished) { throw lance_error("writer already finished"); }
    _finished = true;
    for (size_t c = 0; c < _specs.size(); ++c) {
        flush_column(c);
    }

    // Global buffer 0: the FileDescriptor. The official reader reads the
    // schema from the first GBO entry unconditionally.
    file_descriptor fd;
    fd.num_rows = _rows;
    fd.schema_metadata = schema_metadata;
    fd.fields.reserve(_specs.size());
    for (size_t c = 0; c < _specs.size(); ++c) {
        field_info fi;
        fi.type = field_info::kind::leaf;
        fi.name = _specs[c].name;
        fi.id = int32_t(c);
        fi.parent_id = -1;
        fi.logical_type = _specs[c].logical_type.empty()
                ? default_logical_type(_specs[c].type)
                : _specs[c].logical_type;
        fi.nullable = _specs[c].nullable;
        fi.metadata = _specs[c].field_metadata;
        fd.fields.push_back(std::move(fi));
    }
    align_to(buffer_alignment);
    buffer_ref schema_buf{_offset, 0};
    {
        auto blob = write_file_descriptor(fd);
        schema_buf.size = blob.size();
        write_raw(blob);
    }

    // Column metadata blocks, back to back (unaligned, like the reference
    // writer), then the two offset tables and the footer.
    std::vector<buffer_ref> cmo;
    cmo.reserve(_specs.size());
    const uint64_t col_meta_start = _offset;
    for (const auto& cm : _meta) {
        auto blob = write_column_meta(cm);
        cmo.push_back({_offset, blob.size()});
        write_raw(blob);
    }
    const uint64_t cmo_offset = _offset;
    write_raw(write_offset_table(cmo));
    const uint64_t gbo_offset = _offset;
    write_raw(write_offset_table({schema_buf}));

    footer ft;
    ft.col_meta_start = col_meta_start;
    ft.cmo_offset = cmo_offset;
    ft.gbo_offset = gbo_offset;
    ft.num_global_buffers = 1;
    ft.num_columns = uint32_t(_specs.size());
    ft.major = version_major;
    ft.minor = version_minor;
    write_raw(write_footer(ft));
}

} // namespace sstables::lance::format
