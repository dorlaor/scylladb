/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// A minimal, allocation-bounded protobuf wire-format codec, covering exactly
// the subset the Lance 2.x file format uses (protos/file2.proto,
// protos/encodings_v2_1.proto, protos/file.proto in lancedb/lance).
//
// Hand-written rather than libprotobuf for the same reasons the Parquet layer
// hand-writes TCompactProtocol (docs/dev/parquet-storage-format.md 7.8):
// Scylla carries no protobuf dependency for its storage formats today, Lance
// metadata can arrive from untrusted places (restore, the upload/ directory),
// and a generated parser bounds neither allocation nor recursion. The wire
// format itself is tiny: varints, fixed64/32, and length-delimited fields.
//
// This layer knows nothing about Scylla types. It is deliberately confined to
// std:: so it can be unit-tested and fuzzed standalone.

#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace sstables::lance::format {

class pb_error : public std::runtime_error {
public:
    explicit pb_error(const std::string& what) : std::runtime_error("lance/pb: " + what) {}
};

// Protobuf wire types.
enum class wire : uint8_t {
    varint = 0,
    fixed64 = 1,
    len = 2,
    fixed32 = 5,
};

struct pb_limits {
    // Far above any legitimate Lance metadata block, while bounding what a
    // hostile file can make us do. Column metadata blocks are typically a few
    // KB; the schema FileDescriptor grows with column count.
    size_t max_depth = 32;
    size_t max_len   = 64u << 20;   // 64 MiB for any one length-delimited field
};

class pb_reader {
    const uint8_t* _base;
    const uint8_t* _p;
    const uint8_t* _end;
    pb_limits _lim;
    size_t _depth = 0;

public:
    explicit pb_reader(std::span<const uint8_t> buf, pb_limits l = {})
        : _base(buf.data()), _p(buf.data()), _end(buf.data() + buf.size()), _lim(l) {}
    pb_reader(std::string_view buf, pb_limits l = {})
        : pb_reader(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(buf.data()), buf.size()), l) {}

    size_t remaining() const noexcept { return size_t(_end - _p); }
    size_t position() const noexcept { return size_t(_p - _base); }
    bool eof() const noexcept { return _p >= _end; }

    uint8_t byte() {
        if (_p >= _end) { throw pb_error("truncated: expected a byte"); }
        return *_p++;
    }

    // LEB128, bounded at 10 bytes so a run of 0x80 cannot spin.
    uint64_t uvarint() {
        uint64_t v = 0;
        int shift = 0;
        for (int i = 0; i < 10; ++i) {
            uint8_t b = byte();
            v |= uint64_t(b & 0x7F) << shift;
            if (!(b & 0x80)) { return v; }
            shift += 7;
        }
        throw pb_error("varint longer than 10 bytes");
    }

    int64_t zigzag() {
        uint64_t u = uvarint();
        return int64_t(u >> 1) ^ -int64_t(u & 1);
    }

    uint64_t fixed64() {
        if (remaining() < 8) { throw pb_error("truncated fixed64"); }
        uint64_t v;
        std::memcpy(&v, _p, 8);   // protobuf fixed fields are little-endian
        _p += 8;
        return v;
    }

    uint32_t fixed32() {
        if (remaining() < 4) { throw pb_error("truncated fixed32"); }
        uint32_t v;
        std::memcpy(&v, _p, 4);
        _p += 4;
        return v;
    }

    // A length-delimited field's payload: bytes, string, submessage, packed
    // repeated. Returned as a view into the buffer -- no copy, no allocation.
    std::string_view len_view() {
        uint64_t n = uvarint();
        if (n > _lim.max_len) { throw pb_error("length-delimited field over limit"); }
        if (n > remaining()) { throw pb_error("truncated length-delimited field"); }
        auto v = std::string_view(reinterpret_cast<const char*>(_p), size_t(n));
        _p += n;
        return v;
    }

    // Field tag. Returns false at end of buffer (a proto3 message may simply
    // end); sets field number and wire type otherwise.
    bool next(uint32_t& field, wire& w) {
        if (eof()) { return false; }
        uint64_t tag = uvarint();
        field = uint32_t(tag >> 3);
        w = wire(tag & 0x7);
        if (field == 0) { throw pb_error("field number 0"); }
        switch (w) {
        case wire::varint: case wire::fixed64: case wire::len: case wire::fixed32:
            return true;
        default:
            throw pb_error("unsupported wire type " + std::to_string(unsigned(tag & 0x7)));
        }
    }

    void skip(wire w) {
        switch (w) {
        case wire::varint:  uvarint(); return;
        case wire::fixed64: fixed64(); return;
        case wire::len:     len_view(); return;
        case wire::fixed32: fixed32(); return;
        }
        throw pb_error("skip: unsupported wire type");
    }

    // Recursion guard for nested-message parsing. The caller makes a nested
    // pb_reader over len_view(); this hands the child the parent's depth.
    struct depth_guard {
        pb_reader& r;
        explicit depth_guard(pb_reader& rd) : r(rd) {
            if (++r._depth > r._lim.max_depth) { throw pb_error("message nesting over limit"); }
        }
        ~depth_guard() { --r._depth; }
    };

    // Convenience: a child reader over a submessage payload, inheriting limits
    // and remaining depth budget.
    pb_reader child(std::string_view payload) const {
        pb_reader r(payload, _lim);
        r._depth = _depth + 1;
        if (r._depth > _lim.max_depth) { throw pb_error("message nesting over limit"); }
        return r;
    }
};

// Append-based writer. Nested messages are encoded into their own pb_writer
// and embedded with write_len/write_msg -- Lance metadata blocks are small, so
// the double buffering is noise.
class pb_writer {
    std::string _buf;

    void put_uvarint(uint64_t v) {
        while (v >= 0x80) {
            _buf.push_back(char(uint8_t(v) | 0x80));
            v >>= 7;
        }
        _buf.push_back(char(uint8_t(v)));
    }

    void put_tag(uint32_t field, wire w) {
        put_uvarint((uint64_t(field) << 3) | uint64_t(w));
    }

public:
    const std::string& str() const& { return _buf; }
    std::string str() && { return std::move(_buf); }
    size_t size() const { return _buf.size(); }
    bool empty() const { return _buf.empty(); }

    // proto3 semantics helper: scalar fields equal to their default are not
    // written. Callers use the *_nz variants where that matters.
    void varint(uint32_t field, uint64_t v) {
        put_tag(field, wire::varint);
        put_uvarint(v);
    }
    void varint_nz(uint32_t field, uint64_t v) {
        if (v != 0) { varint(field, v); }
    }
    void svarint(uint32_t field, int64_t v) {   // sint32/sint64 (zigzag)
        varint(field, (uint64_t(v) << 1) ^ uint64_t(v >> 63));
    }
    // int32/int64 fields (non-zigzag): negative values are 10-byte varints.
    void intfield(uint32_t field, int64_t v) {
        varint(field, uint64_t(v));
    }
    void intfield_nz(uint32_t field, int64_t v) {
        if (v != 0) { intfield(field, v); }
    }
    void boolean(uint32_t field, bool v) {
        if (v) { varint(field, 1); }
    }
    void fixed64(uint32_t field, uint64_t v) {
        put_tag(field, wire::fixed64);
        char b[8];
        std::memcpy(b, &v, 8);
        _buf.append(b, 8);
    }
    void len(uint32_t field, std::string_view payload) {
        put_tag(field, wire::len);
        put_uvarint(payload.size());
        _buf.append(payload.data(), payload.size());
    }
    void len_nz(uint32_t field, std::string_view payload) {
        if (!payload.empty()) { len(field, payload); }
    }
    void msg(uint32_t field, const pb_writer& sub) {
        len(field, sub.str());
    }
    // Packed repeated uint64 (proto3 default for repeated scalars).
    void packed_u64(uint32_t field, std::span<const uint64_t> vs) {
        if (vs.empty()) { return; }
        pb_writer tmp;
        for (auto v : vs) { tmp.put_uvarint(v); }
        len(field, tmp.str());
    }
    // Packed repeated enum/uint32.
    void packed_u32(uint32_t field, std::span<const uint32_t> vs) {
        if (vs.empty()) { return; }
        pb_writer tmp;
        for (auto v : vs) { tmp.put_uvarint(v); }
        len(field, tmp.str());
    }
    // Raw bytes appended with no framing -- for building packed payloads.
    void raw_uvarint(uint64_t v) { put_uvarint(v); }
};

// Reads a packed repeated varint payload into `out`. Also tolerates the
// unpacked encoding (one varint per tag) at the caller: proto3 readers must
// accept both, so callers append single values for wire::varint and call this
// for wire::len.
template <typename T>
inline void read_packed_varints(std::string_view payload, std::vector<T>& out, size_t max_elems) {
    pb_reader r(payload);
    while (!r.eof()) {
        if (out.size() >= max_elems) { throw pb_error("packed repeated field over element limit"); }
        out.push_back(T(r.uvarint()));
    }
}

// google.protobuf.Any: field 1 = type_url (string), field 2 = value (bytes).
struct any_view {
    std::string_view type_url;
    std::string_view value;
};

inline any_view parse_any(pb_reader& r) {
    any_view a;
    uint32_t f; wire w;
    while (r.next(f, w)) {
        if (f == 1 && w == wire::len) { a.type_url = r.len_view(); }
        else if (f == 2 && w == wire::len) { a.value = r.len_view(); }
        else { r.skip(w); }
    }
    return a;
}

inline void write_any(pb_writer& out, uint32_t field, std::string_view type_url, std::string_view value) {
    pb_writer a;
    a.len(1, type_url);
    a.len(2, value);
    out.msg(field, a);
}

} // namespace sstables::lance::format
