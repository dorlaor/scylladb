/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "lance_metadata.hh"

#include <algorithm>
#include <cstring>

namespace sstables::lance::format {

static uint64_t rd_u64(const uint8_t* p) {
    uint64_t v;
    std::memcpy(&v, p, 8);
    return v;   // file is little-endian; so are all our targets
}
static uint32_t rd_u32(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, 4);
    return v;
}
static uint16_t rd_u16(const uint8_t* p) {
    uint16_t v;
    std::memcpy(&v, p, 2);
    return v;
}

footer parse_footer(std::span<const uint8_t> tail) {
    if (tail.size() < footer_size) {
        throw lance_error("file too small for a footer");
    }
    const uint8_t* f = tail.data() + tail.size() - footer_size;
    if (std::memcmp(f + 36, magic, 4) != 0) {
        throw lance_error("bad magic: not a Lance file");
    }
    footer ft;
    ft.col_meta_start = rd_u64(f);
    ft.cmo_offset = rd_u64(f + 8);
    ft.gbo_offset = rd_u64(f + 16);
    ft.num_global_buffers = rd_u32(f + 24);
    ft.num_columns = rd_u32(f + 28);
    ft.major = rd_u16(f + 32);
    ft.minor = rd_u16(f + 34);
    // 2.0 files carry (0,3) for historical reasons, or (2,0) from
    // self-described writers; 2.1 carries (2,1). We read what we write plus
    // 2.0's flavors; newer minor versions may hold encodings this reader has
    // never heard of, so refuse them up front.
    const bool v20 = (ft.major == 0 && ft.minor == 3) || (ft.major == 2 && ft.minor == 0);
    const bool v21 = (ft.major == 2 && ft.minor == 1);
    if (!v20 && !v21) {
        throw lance_error("unsupported Lance version " + std::to_string(ft.major) + "." + std::to_string(ft.minor));
    }
    return ft;
}

std::string write_footer(const footer& ft) {
    std::string s(footer_size, '\0');
    auto* p = reinterpret_cast<uint8_t*>(s.data());
    std::memcpy(p, &ft.col_meta_start, 8);
    std::memcpy(p + 8, &ft.cmo_offset, 8);
    std::memcpy(p + 16, &ft.gbo_offset, 8);
    std::memcpy(p + 24, &ft.num_global_buffers, 4);
    std::memcpy(p + 28, &ft.num_columns, 4);
    std::memcpy(p + 32, &ft.major, 2);
    std::memcpy(p + 34, &ft.minor, 2);
    std::memcpy(p + 36, magic, 4);
    return s;
}

std::vector<buffer_ref> parse_offset_table(std::span<const uint8_t> buf, uint32_t entries) {
    if (buf.size() < uint64_t(entries) * 16) {
        throw lance_error("offset table truncated");
    }
    std::vector<buffer_ref> out;
    out.reserve(entries);
    for (uint32_t i = 0; i < entries; ++i) {
        out.push_back({rd_u64(buf.data() + 16 * i), rd_u64(buf.data() + 16 * i + 8)});
    }
    return out;
}

std::string write_offset_table(const std::vector<buffer_ref>& refs) {
    std::string s;
    s.resize(refs.size() * 16);
    auto* p = reinterpret_cast<uint8_t*>(s.data());
    for (const auto& r : refs) {
        std::memcpy(p, &r.offset, 8);
        std::memcpy(p + 8, &r.size, 8);
        p += 16;
    }
    return s;
}

// ---------------------------------------------------------------- Encoding

static encoding_slot parse_encoding(pb_reader parent, std::string_view payload, const metadata_limits& lim) {
    encoding_slot slot;
    auto r = parent.child(payload);
    uint32_t f;
    wire w;
    while (r.next(f, w)) {
        if (f == 1 && w == wire::len) {              // DeferredEncoding
            auto d = r.child(r.len_view());
            buffer_ref ref;
            uint32_t df; wire dw;
            while (d.next(df, dw)) {
                if (df == 1 && dw == wire::varint) { ref.offset = d.uvarint(); }
                else if (df == 2 && dw == wire::varint) { ref.size = d.uvarint(); }
                else { d.skip(dw); }
            }
            slot.deferred = ref;
        } else if (f == 2 && w == wire::len) {       // DirectEncoding{bytes encoding=1}
            auto d = r.child(r.len_view());
            uint32_t df; wire dw;
            while (d.next(df, dw)) {
                if (df == 1 && dw == wire::len) {
                    auto v = d.len_view();
                    if (v.size() > lim.max_encoding_bytes) {
                        throw lance_error("encoding proto over limit");
                    }
                    slot.any_bytes = std::string(v);
                } else {
                    d.skip(dw);
                }
            }
        } else if (f == 3) {                         // google.protobuf.Empty
            if (w == wire::len) { r.len_view(); }
            slot.none = true;
        } else {
            r.skip(w);
        }
    }
    return slot;
}

static void write_encoding(pb_writer& out, uint32_t field, const encoding_slot& slot) {
    pb_writer enc;
    if (slot.none || (slot.any_bytes.empty() && !slot.deferred)) {
        pb_writer empty;
        enc.msg(3, empty);
    } else if (slot.deferred) {
        pb_writer d;
        d.varint_nz(1, slot.deferred->offset);
        d.varint_nz(2, slot.deferred->size);
        enc.msg(1, d);
    } else {
        pb_writer d;
        d.len(1, slot.any_bytes);
        enc.msg(2, d);
    }
    out.msg(field, enc);
}

// ---------------------------------------------------------------- ColumnMetadata

static page_info parse_page(pb_reader parent, std::string_view payload, const metadata_limits& lim) {
    page_info pg;
    std::vector<uint64_t> offsets, sizes;
    auto r = parent.child(payload);
    uint32_t f;
    wire w;
    while (r.next(f, w)) {
        switch (f) {
        case 1:
            if (w == wire::len) { read_packed_varints(r.len_view(), offsets, lim.max_buffers_per_page); }
            else { offsets.push_back(r.uvarint()); }
            break;
        case 2:
            if (w == wire::len) { read_packed_varints(r.len_view(), sizes, lim.max_buffers_per_page); }
            else { sizes.push_back(r.uvarint()); }
            break;
        case 3: pg.rows = r.uvarint(); break;
        case 4: pg.encoding = parse_encoding(r, r.len_view(), lim); break;
        case 5: pg.priority = r.uvarint(); break;
        default: r.skip(w); break;
        }
    }
    if (offsets.size() != sizes.size()) {
        throw lance_error("page buffer offsets/sizes length mismatch");
    }
    if (offsets.size() > lim.max_buffers_per_page) {
        throw lance_error("page buffer count over limit");
    }
    pg.buffers.reserve(offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        pg.buffers.push_back({offsets[i], sizes[i]});
    }
    return pg;
}

column_meta parse_column_meta(std::string_view blob, const metadata_limits& lim) {
    column_meta cm;
    std::vector<uint64_t> offsets, sizes;
    pb_reader r(blob, lim.pb);
    uint32_t f;
    wire w;
    while (r.next(f, w)) {
        switch (f) {
        case 1: cm.encoding = parse_encoding(r, r.len_view(), lim); break;
        case 2:
            if (cm.pages.size() >= lim.max_pages_per_column) {
                throw lance_error("page count over limit");
            }
            cm.pages.push_back(parse_page(r, r.len_view(), lim));
            break;
        case 3:
            if (w == wire::len) { read_packed_varints(r.len_view(), offsets, lim.max_buffers_per_page); }
            else { offsets.push_back(r.uvarint()); }
            break;
        case 4:
            if (w == wire::len) { read_packed_varints(r.len_view(), sizes, lim.max_buffers_per_page); }
            else { sizes.push_back(r.uvarint()); }
            break;
        default: r.skip(w); break;
        }
    }
    if (offsets.size() != sizes.size()) {
        throw lance_error("column buffer offsets/sizes length mismatch");
    }
    for (size_t i = 0; i < offsets.size(); ++i) {
        cm.buffers.push_back({offsets[i], sizes[i]});
    }
    return cm;
}

std::string write_column_meta(const column_meta& cm) {
    pb_writer out;
    write_encoding(out, 1, cm.encoding);
    for (const auto& pg : cm.pages) {
        pb_writer p;
        std::vector<uint64_t> offsets, sizes;
        offsets.reserve(pg.buffers.size());
        sizes.reserve(pg.buffers.size());
        for (const auto& b : pg.buffers) {
            offsets.push_back(b.offset);
            sizes.push_back(b.size);
        }
        p.packed_u64(1, offsets);
        p.packed_u64(2, sizes);
        p.varint_nz(3, pg.rows);
        write_encoding(p, 4, pg.encoding);
        p.varint_nz(5, pg.priority);
        out.msg(2, p);
    }
    std::vector<uint64_t> offsets, sizes;
    for (const auto& b : cm.buffers) {
        offsets.push_back(b.offset);
        sizes.push_back(b.size);
    }
    out.packed_u64(3, offsets);
    out.packed_u64(4, sizes);
    return std::move(out).str();
}

size_t column_meta::page_for_row(uint64_t row) const {
    // pages are in row order; priority is the first row, priority+rows the
    // one-past-last.
    auto it = std::upper_bound(pages.begin(), pages.end(), row,
            [](uint64_t r, const page_info& p) { return r < p.priority; });
    if (it == pages.begin()) {
        throw lance_error("row before first page");
    }
    --it;
    if (row >= it->priority + it->rows) {
        throw lance_error("row past last page row");
    }
    return size_t(it - pages.begin());
}

// ---------------------------------------------------------------- schema

static void parse_metadata_map(pb_reader parent, std::string_view payload, std::map<std::string, std::string>& out) {
    auto r = parent.child(payload);
    std::string key, value;
    uint32_t f;
    wire w;
    while (r.next(f, w)) {
        if (f == 1 && w == wire::len) { key = std::string(r.len_view()); }
        else if (f == 2 && w == wire::len) { value = std::string(r.len_view()); }
        else { r.skip(w); }
    }
    out[std::move(key)] = std::move(value);
}

static field_info parse_field(pb_reader parent, std::string_view payload) {
    field_info fi;
    auto r = parent.child(payload);
    uint32_t f;
    wire w;
    while (r.next(f, w)) {
        switch (f) {
        case 1: fi.type = field_info::kind(r.uvarint()); break;
        case 2: fi.name = std::string(r.len_view()); break;
        case 3: fi.id = int32_t(r.uvarint()); break;
        case 4: fi.parent_id = int32_t(r.uvarint()); break;
        case 5: fi.logical_type = std::string(r.len_view()); break;
        case 6: fi.nullable = r.uvarint() != 0; break;
        case 10: parse_metadata_map(r, r.len_view(), fi.metadata); break;
        default: r.skip(w); break;
        }
    }
    return fi;
}

file_descriptor parse_file_descriptor(std::string_view blob, const metadata_limits& lim) {
    file_descriptor fd;
    pb_reader r(blob, lim.pb);
    uint32_t f;
    wire w;
    while (r.next(f, w)) {
        if (f == 1 && w == wire::len) {           // Schema
            auto s = r.child(r.len_view());
            uint32_t sf;
            wire sw;
            while (s.next(sf, sw)) {
                if (sf == 1 && sw == wire::len) {
                    fd.fields.push_back(parse_field(s, s.len_view()));
                } else if (sf == 5 && sw == wire::len) {
                    parse_metadata_map(s, s.len_view(), fd.schema_metadata);
                } else {
                    s.skip(sw);
                }
            }
        } else if (f == 2 && w == wire::varint) { // length (row count)
            fd.num_rows = r.uvarint();
        } else {
            r.skip(w);
        }
    }
    return fd;
}

std::string write_file_descriptor(const file_descriptor& fd) {
    pb_writer schema;
    for (const auto& fi : fd.fields) {
        pb_writer f;
        f.varint_nz(1, uint64_t(fi.type));
        f.len_nz(2, fi.name);
        f.intfield_nz(3, fi.id);
        // parent_id is an int32; -1 marks a top-level field and must be
        // written explicitly (0 is a real field id).
        f.intfield(4, uint64_t(uint32_t(fi.parent_id)));
        f.len_nz(5, fi.logical_type);
        f.boolean(6, fi.nullable);
        for (const auto& [k, v] : fi.metadata) {
            pb_writer entry;
            entry.len(1, k);
            entry.len(2, v);
            f.msg(10, entry);
        }
        schema.msg(1, f);
    }
    for (const auto& [k, v] : fd.schema_metadata) {
        pb_writer entry;
        entry.len(1, k);
        entry.len(2, v);
        schema.msg(5, entry);
    }
    pb_writer out;
    out.msg(1, schema);
    out.varint_nz(2, fd.num_rows);
    return std::move(out).str();
}

} // namespace sstables::lance::format
