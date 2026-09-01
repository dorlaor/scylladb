/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "lance_encodings.hh"
#include "fastlanes.hh"

#include <span>

#include <algorithm>
#include <bit>
#include <cstring>

#include <zstd.h>

namespace sstables::lance::format {

// Byte-level constants, verified against pylance-written files (see
// docs/dev/lance-storage-format.md 3.2):
//  - chunks are internally padded to 8 bytes with 0xFE;
//  - the reference writer pads page buffers to 64 bytes with 'H' -- readers
//    never depend on either value.
static constexpr uint8_t chunk_pad = 0xFE;
static constexpr size_t chunk_hard_cap = 32u << 10;   // stored chunk size limit
static constexpr char page_layout_type_url[] = "/lance.encodings21.PageLayout";

// RepDefLayer values (encodings_v2_1.proto).
static constexpr uint32_t repdef_all_valid_item = 1;
static constexpr uint32_t repdef_nullable_item = 3;

// ------------------------------------------------------------ small helpers

static void put_u16(std::string& s, uint16_t v) {
    char b[2];
    std::memcpy(b, &v, 2);
    s.append(b, 2);
}

static uint16_t get_u16(std::string_view s, size_t off) {
    if (off + 2 > s.size()) { throw lance_error("chunk truncated (u16)"); }
    uint16_t v;
    std::memcpy(&v, s.data() + off, 2);
    return v;
}

static void pad_to(std::string& s, size_t align, uint8_t fill) {
    while (s.size() % align) { s.push_back(char(fill)); }
}

std::string zstd_compress_block(std::string_view raw, int level) {
    std::string out(8 + ZSTD_compressBound(raw.size()), '\0');
    uint64_t n = raw.size();
    std::memcpy(out.data(), &n, 8);
    size_t r = ZSTD_compress(out.data() + 8, out.size() - 8, raw.data(), raw.size(), level);
    if (ZSTD_isError(r)) { throw lance_error("zstd compression failed"); }
    out.resize(8 + r);
    return out;
}

std::string zstd_decompress_block(std::string_view framed, size_t max_size) {
    if (framed.size() < 8) { throw lance_error("zstd block truncated"); }
    uint64_t n;
    std::memcpy(&n, framed.data(), 8);
    if (n > max_size) { throw lance_error("zstd block over decompressed-size limit"); }
    std::string out(size_t(n), '\0');
    size_t r = ZSTD_decompress(out.data(), out.size(), framed.data() + 8, framed.size() - 8);
    if (ZSTD_isError(r) || r != n) { throw lance_error("zstd decompression failed"); }
    return out;
}

// ------------------------------------------------------ PageLayout protobufs
//
// Message/field numbers from encodings_v2_1.proto:
//   PageLayout: mini_block_layout=1 constant_layout=2 full_zip_layout=3
//   MiniBlockLayout: rep_compression=1 def_compression=2 value_compression=3
//     dictionary=4 num_dictionary_items=5 layers=6 num_buffers=7
//     repetition_index_depth=8 num_items=9
//   FullZipLayout: bits_rep=1 bits_def=2 bits_per_value=3 bits_per_offset=4
//     num_items=5 num_visible_items=6 value_compression=7 layers=8
//   ConstantLayout: layers=5 inline_value=6 rep_compression=7
//     def_compression=8 num_rep_values=9 num_def_values=10
//   CompressiveEncoding: flat=1 variable=2 constant=3 out_of_line_bitpacking=4
//     inline_bitpacking=5 fsst=6 dictionary=7 rle=8 byte_stream_split=9
//     general=10 fixed_size_list=11 packed_struct=12 variable_packed_struct=13
//   Flat: bits_per_value=1 data=2;  Variable: offsets=1 values=2
//   OutOfLineBitpacking: uncompressed_bits_per_value=1 values=3
//   InlineBitpacking: uncompressed_bits_per_value=1 values=2
//   General: compression=1 values=3;  BufferCompression: scheme=1 level=2

static std::string ce_flat(uint32_t bits) {
    pb_writer flat;
    flat.varint(1, bits);
    pb_writer ce;
    ce.msg(1, flat);
    return std::move(ce).str();
}

static std::string ce_variable(uint32_t offset_bits) {
    pb_writer var;
    var.len(1, ce_flat(offset_bits));   // offsets: CompressiveEncoding{flat}
    pb_writer ce;
    ce.msg(2, var);
    return std::move(ce).str();
}

static std::string ce_general_zstd(std::string inner, int level) {
    pb_writer bc;
    bc.varint(1, 2);   // scheme = ZSTD
    bc.intfield(2, level);
    pb_writer gen;
    gen.msg(1, bc);
    gen.len(3, inner);
    pb_writer ce;
    ce.msg(10, gen);
    return std::move(ce).str();
}

std::string wrap_page_layout_any(std::string_view layout) {
    pb_writer any;
    any.len(1, page_layout_type_url);
    any.len(2, layout);
    return std::move(any).str();
}

std::string_view unwrap_page_layout_any(std::string_view any_bytes) {
    pb_reader r(any_bytes);
    auto a = parse_any(r);
    if (a.type_url != page_layout_type_url) {
        throw lance_error("unsupported page encoding type url '" + std::string(a.type_url) + "'");
    }
    return a.value;
}

// What a CompressiveEncoding tree is describing, which constrains the
// encodings we accept in it.
enum class chan_role { value, offsets, def };

// Parses a CompressiveEncoding tree into the supported subset.
static void parse_compressive(pb_reader parent, std::string_view payload, chan_enc& out, chan_role role) {
    auto r = parent.child(payload);
    uint32_t f;
    wire w;
    while (r.next(f, w)) {
        switch (f) {
        case 1: {   // Flat{bits_per_value=1, data=2}
            auto fr = r.child(r.len_view());
            uint32_t ff; wire fw;
            uint32_t bits = 0;
            while (fr.next(ff, fw)) {
                if (ff == 1 && fw == wire::varint) { bits = uint32_t(fr.uvarint()); }
                else if (ff == 2) { throw lance_error("per-buffer compression on Flat is not supported"); }
                else { fr.skip(fw); }
            }
            if (role == chan_role::def && bits != 16) {
                throw lance_error("unsupported def-level width");
            }
            out.k = chan_enc::kind::flat;
            out.bits = bits;
            return;
        }
        case 2: {   // Variable{offsets=1, values=2}
            if (role != chan_role::value) { throw lance_error("variable encoding outside a value channel"); }
            auto vr = r.child(r.len_view());
            uint32_t vf; wire vw;
            chan_enc off;
            bool got_offsets = false;
            while (vr.next(vf, vw)) {
                if (vf == 1 && vw == wire::len) {
                    parse_compressive(vr, vr.len_view(), off, chan_role::offsets);
                    got_offsets = true;
                } else if (vf == 2) {
                    throw lance_error("per-buffer compression on Variable values is not supported");
                } else {
                    vr.skip(vw);
                }
            }
            if (!got_offsets || off.k != chan_enc::kind::flat || (off.bits != 32 && off.bits != 64)) {
                throw lance_error("unsupported Variable offsets encoding");
            }
            out.k = chan_enc::kind::variable;
            out.bits = off.bits;
            return;
        }
        case 4: {   // OutOfLineBitpacking{uncompressed_bits_per_value=1, values=3}
            auto br = r.child(r.len_view());
            uint32_t bf; wire bw;
            chan_enc inner;
            uint32_t t_bits = 0;
            bool got_inner = false;
            while (br.next(bf, bw)) {
                if (bf == 1 && bw == wire::varint) { t_bits = uint32_t(br.uvarint()); }
                else if (bf == 3 && bw == wire::len) {
                    parse_compressive(br, br.len_view(), inner, chan_role::offsets);
                    got_inner = true;
                } else { br.skip(bw); }
            }
            if (!got_inner || inner.k != chan_enc::kind::flat) {
                throw lance_error("unsupported out-of-line bitpacking inner encoding");
            }
            if (t_bits != 8 && t_bits != 16 && t_bits != 32 && t_bits != 64) {
                throw lance_error("unsupported out-of-line bitpacking element width");
            }
            out.k = chan_enc::kind::ool_bp;
            out.logical_bits = t_bits;
            out.packed_bits = inner.bits;
            return;
        }
        case 5: {   // InlineBitpacking{uncompressed_bits_per_value=1, values=2}
            auto br = r.child(r.len_view());
            uint32_t bf; wire bw;
            uint32_t t_bits = 0;
            while (br.next(bf, bw)) {
                if (bf == 1 && bw == wire::varint) { t_bits = uint32_t(br.uvarint()); }
                else if (bf == 2) { throw lance_error("compressed inline bitpacking not supported"); }
                else { br.skip(bw); }
            }
            if (t_bits != 8 && t_bits != 16 && t_bits != 32 && t_bits != 64) {
                throw lance_error("unsupported inline bitpacking element width");
            }
            out.k = chan_enc::kind::inline_bp;
            out.logical_bits = t_bits;
            return;
        }
        case 10: {  // General{compression=1, values=3}
            if (role == chan_role::offsets) { throw lance_error("compressed offsets unsupported"); }
            auto gr = r.child(r.len_view());
            uint32_t gf; wire gw;
            uint32_t scheme = 0;
            bool got_inner = false;
            while (gr.next(gf, gw)) {
                if (gf == 1 && gw == wire::len) {
                    auto cr = gr.child(gr.len_view());
                    uint32_t cf; wire cw;
                    while (cr.next(cf, cw)) {
                        if (cf == 1 && cw == wire::varint) { scheme = uint32_t(cr.uvarint()); }
                        else { cr.skip(cw); }
                    }
                } else if (gf == 3 && gw == wire::len) {
                    parse_compressive(gr, gr.len_view(), out, role);
                    got_inner = true;
                } else {
                    gr.skip(gw);
                }
            }
            if (!got_inner) { throw lance_error("general compression without an inner encoding"); }
            if (scheme != 2) {
                throw lance_error("unsupported general compression scheme " + std::to_string(scheme));
            }
            out.general_scheme = scheme;
            return;
        }
        case 6: throw lance_error("FSST not supported by this reader");
        case 7: throw lance_error("dictionary encoding not supported by this reader");
        case 8: throw lance_error("RLE not supported by this reader");
        case 9: throw lance_error("byte-stream-split not supported by this reader");
        default: r.skip(w); break;
        }
    }
    throw lance_error("empty compressive encoding");
}

// Reads a `layers` field (repeated enum, packed or not) and reduces it to
// "has a nullable item layer". Anything with list layers is out of scope.
static bool parse_layers(pb_reader& r, wire w) {
    std::vector<uint32_t> layers;
    if (w == wire::len) {
        read_packed_varints(r.len_view(), layers, 16);
    } else {
        layers.push_back(uint32_t(r.uvarint()));
    }
    bool nullable = false;
    for (auto l : layers) {
        switch (l) {
        case repdef_all_valid_item: break;
        case repdef_nullable_item: nullable = true; break;
        default: throw lance_error("unsupported repdef layer " + std::to_string(l));
        }
    }
    return nullable;
}

page_layout parse_page_layout(std::string_view any_bytes, const metadata_limits& lim) {
    auto layout_bytes = unwrap_page_layout_any(any_bytes);
    page_layout pl;
    pb_reader r(layout_bytes, lim.pb);
    uint32_t f;
    wire w;
    bool got = false;
    while (r.next(f, w)) {
        if (f == 1 && w == wire::len) {           // MiniBlockLayout
            pl.k = page_layout::kind::miniblock;
            got = true;
            auto mr = r.child(r.len_view());
            uint32_t mf; wire mw;
            bool def_channel = false;
            bool nullable_layer = false;
            while (mr.next(mf, mw)) {
                switch (mf) {
                case 1: throw lance_error("repetition levels not supported by this reader");
                case 2:
                    def_channel = true;
                    parse_compressive(mr, mr.len_view(), pl.defs, chan_role::def);
                    break;
                case 3: parse_compressive(mr, mr.len_view(), pl.val, chan_role::value); break;
                case 4: throw lance_error("dictionary miniblock pages not supported by this reader");
                case 6: nullable_layer = parse_layers(mr, mw) || nullable_layer; break;
                case 7: {
                    auto n = mr.uvarint();
                    if (n != 1) { throw lance_error("multi-buffer miniblock chunks not supported"); }
                    break;
                }
                case 8:
                    if (mr.uvarint() != 0) { throw lance_error("repetition index not supported"); }
                    break;
                case 9: pl.num_items = mr.uvarint(); break;
                default: mr.skip(mw); break;
                }
            }
            // The def channel and the nullable layer must agree; a mismatch
            // means we would mis-slice every chunk.
            if (def_channel != nullable_layer) {
                throw lance_error("def channel / nullable layer mismatch");
            }
            pl.has_def = def_channel;
        } else if (f == 2 && w == wire::len) {    // ConstantLayout
            pl.k = page_layout::kind::constant;
            got = true;
            auto cr = r.child(r.len_view());
            uint32_t cf; wire cw;
            while (cr.next(cf, cw)) {
                switch (cf) {
                case 5: pl.has_def = parse_layers(cr, cw) || pl.has_def; break;
                case 6: pl.constant_value = std::string(cr.len_view()); break;
                case 7: throw lance_error("constant pages with rep buffers not supported");
                case 8: throw lance_error("constant pages with def buffers not supported");
                default: cr.skip(cw); break;
                }
            }
        } else if (f == 3 && w == wire::len) {    // FullZipLayout
            pl.k = page_layout::kind::fullzip;
            got = true;
            auto zr = r.child(r.len_view());
            uint32_t zf; wire zw;
            uint32_t bits_per_value = 0, bits_per_offset = 0;
            bool got_ce = false;
            while (zr.next(zf, zw)) {
                switch (zf) {
                case 1: pl.bits_rep = uint32_t(zr.uvarint()); break;
                case 2: pl.bits_def = uint32_t(zr.uvarint()); break;
                case 3: bits_per_value = uint32_t(zr.uvarint()); break;
                case 4: bits_per_offset = uint32_t(zr.uvarint()); break;
                case 5: pl.num_items = zr.uvarint(); break;
                case 6: pl.num_visible_items = zr.uvarint(); break;
                case 7:
                    parse_compressive(zr, zr.len_view(), pl.val, chan_role::value);
                    got_ce = true;
                    break;
                case 8: pl.has_def = parse_layers(zr, zw) || pl.has_def; break;
                default: zr.skip(zw); break;
                }
            }
            if (pl.bits_rep != 0) { throw lance_error("full-zip repetition levels not supported"); }
            if (pl.bits_def > 1) { throw lance_error("full-zip def wider than one bit not supported"); }
            pl.has_def = pl.bits_def == 1;
            if (!got_ce) {
                // Derive from the oneof widths when value_compression is absent.
                if (bits_per_offset) {
                    pl.val.k = chan_enc::kind::variable;
                    pl.val.bits = bits_per_offset;
                } else {
                    pl.val.k = chan_enc::kind::flat;
                    pl.val.bits = bits_per_value;
                }
            }
            // General on full-zip means per-VALUE compression (transparent),
            // which decode_fullzip_variable handles; fixed-width full-zip
            // with it would break the stride arithmetic, so that stays out.
            if (pl.val.general_scheme && pl.val.k != chan_enc::kind::variable) {
                throw lance_error("compressed fixed-width full-zip values not supported");
            }
            if (pl.val.k == chan_enc::kind::variable) {
                if (bits_per_offset) { pl.val.bits = bits_per_offset; }
                if (pl.val.bits != 32 && pl.val.bits != 64) {
                    throw lance_error("unsupported full-zip offset width");
                }
            } else if (pl.val.k == chan_enc::kind::flat) {
                if (bits_per_value) { pl.val.bits = bits_per_value; }
                if (pl.val.bits == 0 || pl.val.bits % 8) {
                    throw lance_error("unsupported full-zip value width");
                }
            } else {
                throw lance_error("bitpacked full-zip values not supported");
            }
        } else if (f == 4) {
            throw lance_error("blob layout not supported by this reader");
        } else if (f == 5) {
            throw lance_error("sparse layout (Lance 2.3) not supported by this reader");
        } else {
            r.skip(w);
        }
    }
    if (!got) { throw lance_error("page has no layout"); }
    return pl;
}

// ---------------------------------------------------------------- encoding

static size_t value_byte_size(lphys t, const column_values& v, size_t i) {
    return t == lphys::bytes ? v.str[i].size() : bits_of(t) / 8;
}

static bool any_nulls(const column_values& v) {
    return std::any_of(v.def.begin(), v.def.end(), [](uint8_t d) { return d != 0; });
}

// One chunk's definition levels, out-of-line fastlanes-packed at one bit per
// value (the official writer's own choice at any real size). Full 1024-value
// chunks pack; the tail packs when >= 64 values remain and is stored raw
// below that -- the same cost rule as the reference writer, biased to "pack"
// at exact equality so the reader's size-based inference stays unambiguous.
static std::string encode_def_ool(const uint8_t* def, size_t n) {
    std::string out;
    constexpr size_t words_per_chunk = fastlanes_chunk * 1 / 16;   // W=1, T=16
    size_t at = 0;
    while (at < n) {
        const size_t rem = std::min(n - at, fastlanes_chunk);
        uint16_t vals[fastlanes_chunk] = {};
        for (size_t i = 0; i < rem; ++i) { vals[i] = def[at + i]; }
        if (rem == fastlanes_chunk || rem >= 64) {
            uint16_t packed[words_per_chunk];
            fastlanes_pack<uint16_t>(vals, 1, std::span<uint16_t>(packed, words_per_chunk));
            out.append(reinterpret_cast<const char*>(packed), sizeof(packed));
        } else {
            out.append(reinterpret_cast<const char*>(vals), rem * 2);
        }
        at += rem;
    }
    return out;
}

// Builds one chunk's bytes: header u16s, then def, then the value
// sub-buffer, each 8-byte padded.
static std::string encode_chunk(lphys t, const column_values& v, size_t first, size_t n,
                                bool with_def, int zstd_level) {
    std::string values;
    if (t == lphys::bytes) {
        // Offsets are relative to the sub-buffer start and include the
        // offsets region itself, so offsets[0] is the region's size.
        const uint32_t base = uint32_t((n + 1) * 4);
        uint64_t off = base;
        for (size_t i = 0; i <= n; ++i) {
            if (off > 0xFFFFFFFFu) { throw lance_error("chunk value bytes overflow u32 offsets"); }
            uint32_t o32 = uint32_t(off);
            char b[4];
            std::memcpy(b, &o32, 4);
            values.append(b, 4);
            if (i < n) { off += v.str[first + i].size(); }
        }
        for (size_t i = 0; i < n; ++i) { values.append(v.str[first + i]); }
        // The reference reader requires a variable sub-buffer to be a
        // multiple of the offset width; the reference writer pads with 'H'.
        while (values.size() % 4) { values.push_back('H'); }
    } else {
        const size_t w = bits_of(t) / 8;
        values.resize(n * w);
        if (t == lphys::i32) { std::memcpy(values.data(), v.i32.data() + first, n * w); }
        else if (t == lphys::i64) { std::memcpy(values.data(), v.i64.data() + first, n * w); }
        else { std::memcpy(values.data(), v.f64.data() + first, n * w); }
    }
    if (zstd_level > 0) {
        values = zstd_compress_block(values, zstd_level);
    }
    if (values.size() > 0xFFFF) { throw lance_error("chunk value sub-buffer over 64 KiB"); }

    std::string def_bytes;
    if (with_def) {
        if (v.def.empty()) {
            std::vector<uint8_t> zeros(n, 0);
            def_bytes = encode_def_ool(zeros.data(), n);
        } else {
            def_bytes = encode_def_ool(v.def.data() + first, n);
        }
    }
    std::string chunk;
    put_u16(chunk, uint16_t(with_def ? n : 0));   // num_levels
    if (with_def) { put_u16(chunk, uint16_t(def_bytes.size())); }
    put_u16(chunk, uint16_t(values.size()));
    pad_to(chunk, 8, chunk_pad);
    if (with_def) {
        chunk.append(def_bytes);
        pad_to(chunk, 8, chunk_pad);
    }
    chunk.append(values);
    pad_to(chunk, 8, chunk_pad);
    if (chunk.size() > chunk_hard_cap) { throw lance_error("chunk over the 32 KiB cap"); }
    return chunk;
}

static encoded_page encode_miniblock(lphys t, const column_values& v, const encode_options& opt) {
    const size_t rows = v.rows();
    const bool with_def = opt.nullable && any_nulls(v);
    const size_t per_value_meta = with_def ? 1 : 0;   // packed def ~1/8 B/value; round up

    std::string chunk_meta;
    std::string chunks;
    size_t at = 0;
    while (at < rows) {
        // Largest power-of-two count whose payload fits the target; the tail
        // takes whatever remains (log field 0).
        size_t n = 1;
        {
            size_t best = 1;
            for (size_t c = 2; c <= opt.max_chunk_values && at + c <= rows; c *= 2) {
                size_t bytes = t == lphys::bytes ? (c + 1) * 4 : 0;
                for (size_t i = 0; i < c && bytes <= opt.chunk_target_bytes; ++i) {
                    bytes += value_byte_size(t, v, at + i) + per_value_meta;
                }
                if (bytes > opt.chunk_target_bytes) { break; }
                best = c;
            }
            n = best;
        }
        const bool final_chunk = at + n >= rows || rows - at < 2;
        if (final_chunk) { n = rows - at; }
        auto chunk = encode_chunk(t, v, at, n, with_def, opt.zstd_level);
        const uint16_t log = final_chunk ? 0 : uint16_t(std::countr_zero(n));
        put_u16(chunk_meta, uint16_t(((chunk.size() / 8 - 1) << 4) | log));
        chunks.append(chunk);
        at += n;
    }

    pb_writer mb;
    if (with_def) {
        pb_writer ool;   // OutOfLineBitpacking{uncompressed=16, values: Flat{1}}
        ool.varint(1, 16);
        ool.len(3, ce_flat(1));
        pb_writer ce;
        ce.msg(4, ool);
        mb.len(2, ce.str());
    }
    std::string vce = t == lphys::bytes ? ce_variable(32) : ce_flat(bits_of(t));
    if (opt.zstd_level > 0) { vce = ce_general_zstd(std::move(vce), opt.zstd_level); }
    mb.len(3, vce);
    uint32_t layer = with_def ? repdef_nullable_item : repdef_all_valid_item;
    mb.packed_u32(6, std::span<const uint32_t>(&layer, 1));
    mb.varint(7, 1);          // num_buffers
    mb.varint(9, rows);       // num_items
    pb_writer layout;
    layout.msg(1, mb);

    encoded_page pg;
    pg.buffers = {std::move(chunk_meta), std::move(chunks)};
    pg.layout = std::move(layout).str();
    pg.rows = rows;
    return pg;
}

static encoded_page encode_fullzip_bytes(const column_values& v, const encode_options& opt) {
    const size_t rows = v.rows();
    const bool with_def = opt.nullable && any_nulls(v);

    // Zip the values, optionally with each value zstd-framed. Per-value
    // compression stays transparent -- one value decompresses alone -- so it
    // is the only kind full-zip permits; whether it pays is decided per page
    // by measuring, not modelling.
    auto zip = [&] (bool compress, std::vector<uint64_t>& row_offsets) {
        std::string zipped;
        row_offsets.clear();
        row_offsets.reserve(rows + 1);
        for (size_t i = 0; i < rows; ++i) {
            row_offsets.push_back(zipped.size());
            const bool null = !v.def.empty() && v.def[i] != 0;
            if (with_def) { zipped.push_back(null ? 1 : 0); }
            if (!null) {
                std::string framed;
                std::string_view bytes = v.str[i];
                if (compress) {
                    framed = zstd_compress_block(bytes, opt.zstd_level);
                    bytes = framed;
                }
                uint32_t len = uint32_t(bytes.size());
                char b[4];
                std::memcpy(b, &len, 4);
                zipped.append(b, 4);
                zipped.append(bytes);
            }
        }
        row_offsets.push_back(zipped.size());
        return zipped;
    };

    std::vector<uint64_t> row_offsets;
    std::string zipped = zip(false, row_offsets);
    bool compressed = false;
    if (opt.zstd_level > 0) {
        std::vector<uint64_t> c_offsets;
        std::string c_zipped = zip(true, c_offsets);
        if (double(c_zipped.size()) < double(zipped.size()) * opt.fullzip_zstd_max_ratio) {
            zipped = std::move(c_zipped);
            row_offsets = std::move(c_offsets);
            compressed = true;
        }
    }

    // Rep index: byte-packed to the narrowest of {1,2,4,8} that holds the
    // total; the reader infers the width from buffer size / (rows + 1).
    uint32_t w = row_offsets.back() <= 0xFF ? 1
               : row_offsets.back() <= 0xFFFF ? 2
               : row_offsets.back() <= 0xFFFFFFFFull ? 4 : 8;
    std::string rep_index;
    rep_index.reserve((rows + 1) * w);
    for (auto o : row_offsets) {
        char b[8];
        std::memcpy(b, &o, 8);
        rep_index.append(b, w);
    }

    pb_writer fz;
    if (with_def) { fz.varint(2, 1); }   // bits_def
    fz.varint(4, 32);                    // bits_per_offset
    fz.varint(5, rows);                  // num_items
    fz.varint(6, rows);                  // num_visible_items
    std::string vce = ce_variable(32);
    if (compressed) { vce = ce_general_zstd(std::move(vce), opt.zstd_level); }
    fz.len(7, vce);                      // value_compression
    uint32_t layer = with_def ? repdef_nullable_item : repdef_all_valid_item;
    fz.packed_u32(8, std::span<const uint32_t>(&layer, 1));
    pb_writer layout;
    layout.msg(3, fz);

    encoded_page pg;
    pg.buffers = {std::move(zipped), std::move(rep_index)};
    pg.layout = std::move(layout).str();
    pg.rows = rows;
    return pg;
}

static encoded_page encode_all_null(const column_values& v) {
    pb_writer cl;
    uint32_t layer = repdef_nullable_item;
    cl.packed_u32(5, std::span<const uint32_t>(&layer, 1));
    pb_writer layout;
    layout.msg(2, cl);
    encoded_page pg;
    pg.layout = std::move(layout).str();
    pg.rows = v.rows();
    return pg;
}

encoded_page encode_page(lphys t, const column_values& v, const encode_options& opt) {
    const size_t rows = v.rows();
    if (rows == 0) { throw lance_error("empty page"); }
    if (!v.def.empty() && v.def.size() != rows) {
        throw lance_error("def / value length mismatch");
    }
    if (opt.nullable && !v.def.empty()
            && std::all_of(v.def.begin(), v.def.end(), [](uint8_t d) { return d != 0; })) {
        return encode_all_null(v);
    }
    if (t == lphys::bytes) {
        size_t total = 0, biggest = 0;
        for (const auto& s : v.str) {
            total += s.size();
            biggest = std::max(biggest, s.size());
        }
        // The reference writer's is_narrow heuristic, plus a hard escape for
        // any value a 32 KiB chunk cannot hold.
        if (total / rows >= opt.fullzip_threshold || biggest > opt.chunk_target_bytes) {
            return encode_fullzip_bytes(v, opt);
        }
    }
    return encode_miniblock(t, v, opt);
}

// ---------------------------------------------------------------- decoding

miniblock_index parse_miniblock_index(std::string_view buf, uint64_t num_items,
                                      const metadata_limits& lim) {
    if (buf.size() % 2) { throw lance_error("chunk metadata buffer has odd size"); }
    const size_t n = buf.size() / 2;
    if (n == 0) { throw lance_error("miniblock page with no chunks"); }
    if (n > lim.max_pages_per_column) { throw lance_error("chunk count over limit"); }
    miniblock_index idx;
    idx.chunks.reserve(n);
    uint64_t off = 0, val = 0;
    for (size_t i = 0; i < n; ++i) {
        uint16_t w = get_u16(buf, 2 * i);
        miniblock_index::chunk c;
        c.byte_offset = off;
        c.byte_size = uint32_t(((w >> 4) + 1) * 8);
        c.first_value = val;
        const uint32_t log = w & 0xF;
        if (i + 1 < n) {
            if (log == 0) { throw lance_error("non-final chunk without a power-of-two count"); }
            c.values = 1u << log;
        } else {
            if (val > num_items) { throw lance_error("chunk values overrun num_items"); }
            c.values = uint32_t(num_items - val);
        }
        off += c.byte_size;
        val += c.values;
        idx.chunks.push_back(c);
    }
    if (val != num_items) { throw lance_error("chunk value counts disagree with num_items"); }
    return idx;
}

size_t miniblock_index::chunk_for(uint64_t v) const {
    auto it = std::upper_bound(chunks.begin(), chunks.end(), v,
            [](uint64_t x, const chunk& c) { return x < c.first_value; });
    if (it == chunks.begin()) { throw lance_error("value before first chunk"); }
    --it;
    if (v >= it->first_value + it->values) { throw lance_error("value past last chunk"); }
    return size_t(it - chunks.begin());
}

// Unpacks an out-of-line fastlanes buffer of `n` T-bit values packed at
// `w` bits. Layout: ceil(n/1024) chunks of 1024*w/bits(T) words; when the
// buffer is shorter than that, the final chunk's values are stored raw
// (the writer's pad-vs-raw cost rule; the reader infers which from length).
template <typename T>
static std::vector<T> ool_unpack(std::string_view buf, size_t n, unsigned w) {
    constexpr unsigned t = sizeof(T) * 8;
    if (buf.size() % sizeof(T)) { throw lance_error("bitpacked buffer size not word-aligned"); }
    const size_t num_chunks = (n + fastlanes_chunk - 1) / fastlanes_chunk;
    const size_t words_per_chunk = fastlanes_chunk * w / t;
    const size_t total_words = buf.size() / sizeof(T);
    std::vector<T> words(total_words);
    std::memcpy(words.data(), buf.data(), buf.size());

    std::vector<T> out(num_chunks * fastlanes_chunk);
    const bool tail_raw = total_words != num_chunks * words_per_chunk;
    const size_t packed_chunks = tail_raw ? num_chunks - 1 : num_chunks;
    if (num_chunks == 0) { return out; }
    for (size_t c = 0; c < packed_chunks; ++c) {
        if ((c + 1) * words_per_chunk > total_words) { throw lance_error("bitpacked buffer truncated"); }
        fastlanes_unpack<T>(std::span<const T>(words).subspan(c * words_per_chunk, words_per_chunk),
                            w, out.data() + c * fastlanes_chunk);
    }
    if (tail_raw) {
        const size_t tail_vals = n - packed_chunks * fastlanes_chunk;
        if (packed_chunks * words_per_chunk + tail_vals != total_words) {
            throw lance_error("bitpacked tail size mismatch");
        }
        std::memcpy(out.data() + packed_chunks * fastlanes_chunk,
                    words.data() + packed_chunks * words_per_chunk, tail_vals * sizeof(T));
    }
    out.resize(n);
    return out;
}

// Unpacks an inline fastlanes sub-buffer: one T-bit width word, then a
// single packed 1024-value chunk (values beyond `n` are padding).
template <typename T>
static std::vector<T> inline_unpack(std::string_view buf, size_t n) {
    constexpr unsigned t = sizeof(T) * 8;
    if (n > fastlanes_chunk) { throw lance_error("inline bitpacked chunk over 1024 values"); }
    if (buf.size() < sizeof(T)) { throw lance_error("inline bitpacked buffer truncated"); }
    T w_word;
    std::memcpy(&w_word, buf.data(), sizeof(T));
    const unsigned w = unsigned(w_word);
    if (w > t) { throw lance_error("inline bitpacked width over element width"); }
    const size_t words = fastlanes_chunk * w / t;
    if (buf.size() < sizeof(T) * (1 + words)) { throw lance_error("inline bitpacked buffer truncated"); }
    std::vector<T> packed(words);
    std::memcpy(packed.data(), buf.data() + sizeof(T), words * sizeof(T));
    std::vector<T> out(fastlanes_chunk);
    fastlanes_unpack<T>(packed, w, out.data());
    out.resize(n);
    return out;
}

// Decodes levels [a, b) of a def sub-buffer holding `n` levels, appended to
// out.def. Bitpacked forms unpack whole 1024-value blocks (128 bytes each --
// cheap) and then slice; flat u16 slices directly.
static void decode_def_channel(const chan_enc& e, std::string_view buf, size_t n,
                               size_t a, size_t b, column_values& out) {
    std::vector<uint16_t> levels;
    size_t base = 0;   // index of levels[0] within the chunk's n
    switch (e.k) {
    case chan_enc::kind::flat:
        if (buf.size() < n * 2) { throw lance_error("def sub-buffer truncated"); }
        levels.resize(b - a);
        std::memcpy(levels.data(), buf.data() + a * 2, (b - a) * 2);
        base = a;
        break;
    case chan_enc::kind::ool_bp:
        if (e.logical_bits != 16) { throw lance_error("unsupported def element width"); }
        levels = ool_unpack<uint16_t>(buf, n, e.packed_bits);
        break;
    case chan_enc::kind::inline_bp:
        if (e.logical_bits != 16) { throw lance_error("unsupported def element width"); }
        levels = inline_unpack<uint16_t>(buf, n);
        break;
    default:
        throw lance_error("unsupported def-level encoding");
    }
    for (size_t i = a; i < b; ++i) {
        uint16_t d = levels.at(i - base);
        if (d > 1) { throw lance_error("def level above 1 in a flat page"); }
        out.def.push_back(uint8_t(d));
    }
}

// Appends `n` fixed-width values from a plain little-endian buffer.
static void append_flat(lphys t, std::string_view values, size_t n, column_values& out) {
    const size_t w = bits_of(t) / 8;
    if (values.size() < n * w) { throw lance_error("flat sub-buffer truncated"); }
    auto app = [&](auto& vec) {
        const size_t base = vec.size();
        vec.resize(base + n);
        std::memcpy(vec.data() + base, values.data(), n * w);
    };
    if (t == lphys::i32) { app(out.i32); }
    else if (t == lphys::i64) { app(out.i64); }
    else { app(out.f64); }
}

// Decodes values [a, b) of a value sub-buffer holding `n` values.
static void decode_value_channel(lphys t, const chan_enc& e, std::string_view values, size_t n,
                                 size_t a, size_t b, column_values& out) {
    switch (e.k) {
    case chan_enc::kind::variable: {
        if (t != lphys::bytes) { throw lance_error("variable values on a fixed-width column"); }
        const uint32_t ow = e.bits / 8;
        if (values.size() < (n + 1) * ow) { throw lance_error("variable sub-buffer truncated"); }
        auto offset_at = [&](size_t i) -> uint64_t {
            if (ow == 4) {
                uint32_t o;
                std::memcpy(&o, values.data() + i * 4, 4);
                return o;
            }
            uint64_t o;
            std::memcpy(&o, values.data() + i * 8, 8);
            return o;
        };
        for (size_t i = a; i < b; ++i) {
            uint64_t x = offset_at(i), y = offset_at(i + 1);
            if (x > y || y > values.size()) { throw lance_error("variable offsets out of range"); }
            out.str.emplace_back(values.substr(size_t(x), size_t(y - x)));
        }
        return;
    }
    case chan_enc::kind::flat: {
        if (t == lphys::bytes) { throw lance_error("flat values on a bytes column"); }
        if (e.bits != bits_of(t)) { throw lance_error("value width disagrees with the schema"); }
        const size_t w = e.bits / 8;
        if (values.size() < n * w) { throw lance_error("flat sub-buffer truncated"); }
        append_flat(t, values.substr(a * w), b - a, out);
        return;
    }
    case chan_enc::kind::inline_bp:
    case chan_enc::kind::ool_bp: {
        if (t == lphys::bytes || t == lphys::f64) {
            throw lance_error("bitpacked values on a non-integer column");
        }
        if (e.logical_bits != bits_of(t)) { throw lance_error("bitpacked element width disagrees with the schema"); }
        if (t == lphys::i32) {
            auto vals = e.k == chan_enc::kind::inline_bp
                    ? inline_unpack<uint32_t>(values, n)
                    : ool_unpack<uint32_t>(values, n, e.packed_bits);
            for (size_t i = a; i < b; ++i) { out.i32.push_back(int32_t(vals.at(i))); }
        } else {
            auto vals = e.k == chan_enc::kind::inline_bp
                    ? inline_unpack<uint64_t>(values, n)
                    : ool_unpack<uint64_t>(values, n, e.packed_bits);
            for (size_t i = a; i < b; ++i) { out.i64.push_back(int64_t(vals.at(i))); }
        }
        return;
    }
    }
    throw lance_error("unsupported value encoding");
}

column_values decode_miniblock_chunks(lphys t, const page_layout& pl, const miniblock_index& idx,
                                      size_t first_chunk, size_t n_chunks,
                                      std::string_view chunk_bytes,
                                      uint64_t keep_from, uint64_t keep_to) {
    column_values out;
    if (first_chunk + n_chunks > idx.chunks.size()) { throw lance_error("chunk range out of bounds"); }
    const uint64_t base_off = idx.chunks[first_chunk].byte_offset;
    for (size_t c = first_chunk; c < first_chunk + n_chunks; ++c) {
        const auto& ch = idx.chunks[c];
        // The requested slice, in this chunk's local coordinates.
        const uint64_t lo = std::max(keep_from, ch.first_value);
        const uint64_t hi = std::min<uint64_t>(keep_to, ch.first_value + ch.values);
        if (lo >= hi) { continue; }
        const size_t a = size_t(lo - ch.first_value);
        const size_t b = size_t(hi - ch.first_value);
        const size_t start = size_t(ch.byte_offset - base_off);
        if (start + ch.byte_size > chunk_bytes.size()) { throw lance_error("chunk bytes truncated"); }
        auto chunk = chunk_bytes.substr(start, ch.byte_size);
        size_t at = 0;
        const uint16_t num_levels = get_u16(chunk, at);
        at += 2;
        uint16_t def_len = 0;
        if (pl.has_def) {
            def_len = get_u16(chunk, at);
            at += 2;
        }
        const uint16_t val_len = get_u16(chunk, at);
        at += 2;
        at = (at + 7) & ~size_t(7);
        if (pl.has_def) {
            if (num_levels != ch.values) {
                throw lance_error("def level count disagrees with chunk values");
            }
            if (at + def_len > chunk.size()) { throw lance_error("def sub-buffer truncated"); }
            decode_def_channel(pl.defs, chunk.substr(at, def_len), ch.values, a, b, out);
            at += def_len;
            at = (at + 7) & ~size_t(7);
        }
        if (at + val_len > chunk.size()) { throw lance_error("value sub-buffer truncated"); }
        auto values = chunk.substr(at, val_len);
        std::string plain;
        if (pl.val.general_scheme == 2) {
            plain = zstd_decompress_block(values, chunk_hard_cap * 8);
            values = plain;
        }
        decode_value_channel(t, pl.val, values, ch.values, a, b, out);
    }
    if (out.rows() != keep_to - keep_from) {
        throw lance_error("miniblock slice not fully covered by the fetched chunks");
    }
    // A page without a def channel decodes as all-valid: leave def empty.
    return out;
}

column_values decode_fullzip_fixed(lphys t, const page_layout& pl, uint64_t lo, uint64_t hi,
                                   std::string_view zipped) {
    const size_t ctrl = pl.has_def ? 1 : 0;
    const size_t w = pl.val.bits / 8;
    const size_t stride = ctrl + w;
    if (zipped.size() < (hi - lo) * stride) { throw lance_error("full-zip slice truncated"); }
    column_values out;
    for (uint64_t i = 0; i < hi - lo; ++i) {
        auto rec = zipped.substr(size_t(i * stride), stride);
        if (ctrl) {
            uint8_t d = uint8_t(rec[0]);
            if (d > 1) { throw lance_error("full-zip def above 1 in a flat page"); }
            out.def.push_back(d);
        }
        decode_value_channel(t, pl.val, rec.substr(ctrl), 1, 0, 1, out);
    }
    return out;
}

column_values decode_fullzip_variable(const page_layout& pl, uint64_t lo, uint64_t hi,
                                      std::string_view zipped) {
    const bool ctrl = pl.has_def;
    const uint32_t ow = pl.val.bits / 8;
    column_values out;
    size_t at = 0;
    for (uint64_t i = lo; i < hi; ++i) {
        bool null = false;
        if (ctrl) {
            if (at >= zipped.size()) { throw lance_error("full-zip slice truncated"); }
            uint8_t d = uint8_t(zipped[at++]);
            if (d > 1) { throw lance_error("full-zip def above 1 in a flat page"); }
            null = d != 0;
            out.def.push_back(d);
        }
        if (null) {
            out.str.emplace_back();
            continue;
        }
        if (at + ow > zipped.size()) { throw lance_error("full-zip length truncated"); }
        uint64_t len = 0;
        std::memcpy(&len, zipped.data() + at, ow);
        at += ow;
        if (at + len > zipped.size()) { throw lance_error("full-zip value truncated"); }
        if (pl.val.general_scheme == 2) {
            // Per-value compression: each value is its own framed zstd block,
            // so a single value stays independently readable.
            out.str.push_back(zstd_decompress_block(zipped.substr(at, size_t(len)), 64u << 20));
        } else {
            out.str.emplace_back(zipped.substr(at, size_t(len)));
        }
        at += len;
    }
    return out;
}

std::pair<std::string, std::string> split_miniblock_chunk(const page_layout& pl, uint32_t n_values,
                                                          std::string_view chunk) {
    size_t at = 0;
    const uint16_t num_levels = get_u16(chunk, at);
    at += 2;
    uint16_t def_len = 0;
    if (pl.has_def) {
        def_len = get_u16(chunk, at);
        at += 2;
    }
    const uint16_t val_len = get_u16(chunk, at);
    at += 2;
    at = (at + 7) & ~size_t(7);
    std::string def;
    if (pl.has_def) {
        if (num_levels != n_values) { throw lance_error("def level count disagrees with chunk values"); }
        if (at + def_len > chunk.size()) { throw lance_error("def sub-buffer truncated"); }
        def = std::string(chunk.substr(at, def_len));
        at += def_len;
        at = (at + 7) & ~size_t(7);
    }
    if (at + val_len > chunk.size()) { throw lance_error("value sub-buffer truncated"); }
    std::string values(chunk.substr(at, val_len));
    if (pl.val.general_scheme == 2) {
        values = zstd_decompress_block(values, chunk_hard_cap * 8);
    }
    return {std::move(def), std::move(values)};
}

column_values decode_plain_chunk(lphys t, const page_layout& pl, std::string_view def,
                                 std::string_view values, uint32_t n_values,
                                 size_t a, size_t b) {
    if (a > b || b > n_values) { throw lance_error("plain-chunk slice out of range"); }
    column_values out;
    if (pl.has_def) {
        decode_def_channel(pl.defs, def, n_values, a, b, out);
    }
    // The zstd wrapping was already undone by split_miniblock_chunk.
    chan_enc plain_val = pl.val;
    plain_val.general_scheme = 0;
    decode_value_channel(t, plain_val, values, n_values, a, b, out);
    return out;
}

uint32_t fullzip_rep_index_width(uint64_t buffer_size, uint64_t rows) {
    for (uint32_t w : {1u, 2u, 4u, 8u}) {
        if (buffer_size == (rows + 1) * w) { return w; }
    }
    throw lance_error("full-zip repetition index size does not match any width");
}

uint64_t read_rep_index_entry(std::string_view rep_index, uint32_t width, uint64_t i) {
    if ((i + 1) * width > rep_index.size()) { throw lance_error("rep index entry out of range"); }
    uint64_t v = 0;
    std::memcpy(&v, rep_index.data() + i * width, width);
    return v;
}

column_values decode_constant(lphys t, const page_layout& pl, uint64_t lo, uint64_t hi) {
    column_values out;
    const size_t n = size_t(hi - lo);
    if (!pl.constant_value) {
        // All-null page.
        if (!pl.has_def) { throw lance_error("constant page with no value and no nullable layer"); }
        out.def.assign(n, 1);
        switch (t) {
        case lphys::i32: out.i32.assign(n, 0); break;
        case lphys::i64: out.i64.assign(n, 0); break;
        case lphys::f64: out.f64.assign(n, 0); break;
        case lphys::bytes: out.str.assign(n, {}); break;
        }
        return out;
    }
    const auto& cv = *pl.constant_value;
    switch (t) {
    case lphys::i32: {
        if (cv.size() != 4) { throw lance_error("constant width disagrees with the schema"); }
        int32_t v;
        std::memcpy(&v, cv.data(), 4);
        out.i32.assign(n, v);
        break;
    }
    case lphys::i64: {
        if (cv.size() != 8) { throw lance_error("constant width disagrees with the schema"); }
        int64_t v;
        std::memcpy(&v, cv.data(), 8);
        out.i64.assign(n, v);
        break;
    }
    case lphys::f64: {
        if (cv.size() != 8) { throw lance_error("constant width disagrees with the schema"); }
        double v;
        std::memcpy(&v, cv.data(), 8);
        out.f64.assign(n, v);
        break;
    }
    case lphys::bytes:
        out.str.assign(n, cv);
        break;
    }
    if (pl.has_def) { out.def.assign(n, 0); }
    return out;
}

} // namespace sstables::lance::format
