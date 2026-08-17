/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Value decoders -- the inverse of encoders.hh.
//
// Until now the read side could parse metadata and definition levels but not a
// single value, which meant nothing written could be read back. These close the
// loop for the encodings the writer emits: PLAIN, RLE_DICTIONARY and
// DELTA_BINARY_PACKED, plus BYTE_STREAM_SPLIT for completeness.
//
// Every decoder is bounded: it is told how many values to produce and never
// reads past the buffer it was given, because page bodies can come from
// untrusted files.

#include "rle_bitpack.hh"

#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace sstables::parquet::format {

class decode_error : public std::runtime_error {
public:
    explicit decode_error(const std::string& w) : std::runtime_error("parquet/decode: " + w) {}
};

// ---------------------------------------------------------------- PLAIN
template <typename T>
inline std::vector<T> decode_plain(std::span<const uint8_t> in, size_t count) {
    if (in.size() < count * sizeof(T)) {
        throw decode_error("PLAIN buffer too small for " + std::to_string(count) + " values");
    }
    std::vector<T> out(count);
    // memcpy with a null source or destination is UB even for a zero length, and
    // an all-null column legitimately produces a zero-value page.
    if (count) { std::memcpy(out.data(), in.data(), count * sizeof(T)); }
    return out;
}

inline std::vector<std::string> decode_plain_byte_array(std::span<const uint8_t> in, size_t count) {
    std::vector<std::string> out;
    out.reserve(count);
    size_t p = 0;
    for (size_t i = 0; i < count; ++i) {
        if (p + 4 > in.size()) { throw decode_error("truncated BYTE_ARRAY length"); }
        uint32_t n;
        std::memcpy(&n, in.data() + p, 4);
        p += 4;
        if (p + n > in.size()) { throw decode_error("truncated BYTE_ARRAY value"); }
        out.emplace_back(reinterpret_cast<const char*>(in.data() + p), n);
        p += n;
    }
    return out;
}

// ---------------------------------------------------------------- BYTE_STREAM_SPLIT
template <typename T>
inline std::vector<T> decode_byte_stream_split(std::span<const uint8_t> in, size_t count) {
    constexpr size_t K = sizeof(T);
    if (in.size() < count * K) { throw decode_error("BYTE_STREAM_SPLIT buffer too small"); }
    std::vector<T> out(count);
    for (size_t i = 0; i < count; ++i) {
        uint8_t buf[K];
        for (size_t b = 0; b < K; ++b) { buf[b] = in[b * count + i]; }
        std::memcpy(&out[i], buf, K);
    }
    return out;
}

// A BYTE_ARRAY dictionary page as a table of views into the page's own bytes.
//
// Materialising it as std::string cost more than everything else in a point read
// put together: a column with a near-unique dictionary made every reader allocate
// one string per distinct value before it could decode a single row. Views make
// the table one allocation and copy only the values actually referenced.
// The caller must keep the decompressed page alive for as long as the views.
inline std::vector<std::string_view> index_plain_byte_array(std::span<const uint8_t> in,
                                                            size_t count) {
    std::vector<std::string_view> out;
    out.reserve(count);
    size_t p = 0;
    for (size_t i = 0; i < count; ++i) {
        if (p + 4 > in.size()) { throw decode_error("truncated BYTE_ARRAY length"); }
        uint32_t n;
        std::memcpy(&n, in.data() + p, 4);
        p += 4;
        if (p + n > in.size()) { throw decode_error("truncated BYTE_ARRAY value"); }
        out.emplace_back(reinterpret_cast<const char*>(in.data() + p), n);
        p += n;
    }
    return out;
}

// RLE_DICTIONARY over a view table: only referenced entries become strings.
inline std::vector<std::string> decode_rle_dictionary_views(std::span<const uint8_t> page,
                                                            std::span<const std::string_view> dict,
                                                            size_t count) {
    if (page.empty()) { throw decode_error("empty dictionary index page"); }
    const uint8_t bw = page[0];
    rle_decoder dec(page.subspan(1), bw);
    auto idx = dec.decode_all(count);
    if (idx.size() != count) { throw decode_error("short dictionary index stream"); }
    std::vector<std::string> out;
    out.reserve(count);
    for (uint64_t i : idx) {
        if (i >= dict.size()) { throw decode_error("dictionary index out of range"); }
        out.emplace_back(dict[size_t(i)]);
    }
    return out;
}

// ---------------------------------------------------------------- RLE_DICTIONARY
// The data page is a single bit-width byte followed by an RLE/bit-packed hybrid
// stream of indices into the dictionary page.
template <typename T>
inline std::vector<T> decode_rle_dictionary(std::span<const uint8_t> page,
                                            const std::vector<T>& dict,
                                            size_t count) {
    if (page.empty()) { throw decode_error("empty dictionary index page"); }
    const uint8_t bw = page[0];
    rle_decoder dec(page.subspan(1), bw);
    auto idx = dec.decode_all(count);
    if (idx.size() != count) { throw decode_error("short dictionary index stream"); }
    std::vector<T> out;
    out.reserve(count);
    for (uint64_t i : idx) {
        if (i >= dict.size()) { throw decode_error("dictionary index out of range"); }
        out.push_back(dict[size_t(i)]);
    }
    return out;
}

// ---------------------------------------------------------------- DELTA_BINARY_PACKED
inline std::vector<int64_t> decode_delta_binary_packed(std::span<const uint8_t> in, size_t count) {
    size_t p = 0;
    auto uvarint = [&] () -> uint64_t {
        uint64_t v = 0; int shift = 0;
        for (int i = 0; i < 10; ++i) {
            if (p >= in.size()) { throw decode_error("truncated varint"); }
            uint8_t b = in[p++];
            v |= uint64_t(b & 0x7F) << shift;
            if (!(b & 0x80)) { return v; }
            shift += 7;
        }
        throw decode_error("varint too long");
    };
    auto zigzag = [&] () -> int64_t {
        uint64_t u = uvarint();
        return int64_t(u >> 1) ^ -int64_t(u & 1);
    };

    const uint64_t block = uvarint();
    const uint64_t minis = uvarint();
    const uint64_t total = uvarint();
    if (block == 0 || minis == 0 || block % minis) { throw decode_error("bad delta header"); }
    const uint64_t mini = block / minis;
    if (mini % 32) { throw decode_error("miniblock size not a multiple of 32"); }

    std::vector<int64_t> out;
    out.reserve(count);
    if (total == 0) { return out; }

    int64_t prev = zigzag();
    out.push_back(prev);

    while (out.size() < count && out.size() < total) {
        const int64_t min_delta = zigzag();
        if (p + minis > in.size()) { throw decode_error("truncated miniblock widths"); }
        std::vector<uint8_t> widths(in.begin() + long(p), in.begin() + long(p + minis));
        p += minis;

        for (uint64_t m = 0; m < minis && out.size() < count && out.size() < total; ++m) {
            const uint8_t w = widths[m];
            if (w > 64) { throw decode_error("delta bit width > 64"); }
            if (w == 0) {
                for (uint64_t i = 0; i < mini && out.size() < count && out.size() < total; ++i) {
                    prev = int64_t(uint64_t(prev) + uint64_t(min_delta));
                    out.push_back(prev);
                }
                continue;
            }
            const size_t need = size_t(mini) * w / 8;
            if (p + need > in.size()) { throw decode_error("truncated miniblock body"); }
            // Plain bit-packing, LSB first -- no RLE hybrid header here.
            uint64_t acc = 0; int bits = 0; size_t q = p;
            for (uint64_t i = 0; i < mini; ++i) {
                while (bits < w) { acc |= uint64_t(in[q++]) << bits; bits += 8; }
                const uint64_t v = (w == 64) ? acc : (acc & ((1ull << w) - 1));
                // Shifting a 64-bit value by 64 is undefined, and w == 64 happens
                // whenever a block's deltas wrapped; the accumulator is fully
                // consumed in that case, so clear it instead.
                if (w == 64) { acc = 0; } else { acc >>= w; }
                bits -= w;
                if (out.size() < count && out.size() < total) {
                    // Unsigned throughout, to undo the encoder's wrap exactly rather
                    // than overflow a signed add. See format/encoders.hh.
                    prev = int64_t(uint64_t(prev) + uint64_t(min_delta) + v);
                    out.push_back(prev);
                }
            }
            p += need;
        }
    }
    return out;
}

} // namespace sstables::parquet::format
