/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// FastLanes 1024-value bitpacking, decode side only -- the layout Lance's
// InlineBitpacking and OutOfLineBitpacking encodings store (the reference
// implementation vendors the `fastlanes` crate as lance-bitpacking).
//
// The layout: 1024 values are packed per "lane" of the element type T
// (LANES = 1024 / bits(T)); within a lane, value `row` (row in [0, bits(T)))
// sits at logical index FL_ORDER[row/8]*16 + (row%8)*128 + lane, and the
// packed words for lane L are words[LANES*w + L]. Verified against
// pylance-written buffers before this was written -- do not "fix" the index
// formula from intuition, it is deliberately transposed for SIMD.
//
// Our writer never emits bitpacking (see docs/dev/lance-storage-format.md 4);
// this exists so the reader can decode files the official writer produced,
// which bitpacks definition levels and narrow integers unconditionally.

#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>

namespace sstables::lance::format {

inline constexpr size_t fastlanes_chunk = 1024;
inline constexpr size_t fl_order[8] = {0, 4, 2, 6, 1, 5, 3, 7};

// Unpacks one 1024-value chunk of T-bit elements packed at `w` bits each.
// `words` must hold 1024*w/bits(T) elements; `out` receives 1024 values,
// zero-extended.
template <typename T>
inline void fastlanes_unpack(std::span<const T> words, unsigned w, T* out) {
    constexpr unsigned t = sizeof(T) * 8;
    constexpr size_t lanes = fastlanes_chunk / t;
    if (w > t) {
        throw std::runtime_error("fastlanes: packed width over element width");
    }
    if (words.size() < fastlanes_chunk * w / t) {
        throw std::runtime_error("fastlanes: packed buffer truncated");
    }
    if (w == 0) {
        std::memset(out, 0, fastlanes_chunk * sizeof(T));
        return;
    }
    const T mask = w == t ? T(~T(0)) : T((T(1) << w) - 1);
    for (size_t lane = 0; lane < lanes; ++lane) {
        for (unsigned row = 0; row < t; ++row) {
            const size_t idx = fl_order[row / 8] * 16 + (row % 8) * 128 + lane;
            const size_t bitpos = size_t(row) * w;
            const size_t word = bitpos / t;
            const unsigned off = unsigned(bitpos % t);
            T v = T(words[lanes * word + lane] >> off);
            if (off + w > t) {
                v = T(v | (words[lanes * (word + 1) + lane] << (t - off)));
            }
            out[idx] = T(v & mask);
        }
    }
}

// The inverse: packs 1024 T-bit values at `w` bits each into 1024*w/bits(T)
// words. `out` must hold that many elements, zero-initialised by this
// function.
template <typename T>
inline void fastlanes_pack(const T* values, unsigned w, std::span<T> out) {
    constexpr unsigned t = sizeof(T) * 8;
    constexpr size_t lanes = fastlanes_chunk / t;
    if (w == 0 || w > t) {
        throw std::runtime_error("fastlanes: bad packed width");
    }
    if (out.size() < fastlanes_chunk * w / t) {
        throw std::runtime_error("fastlanes: output buffer too small");
    }
    std::memset(out.data(), 0, out.size() * sizeof(T));
    const T mask = w == t ? T(~T(0)) : T((T(1) << w) - 1);
    for (size_t lane = 0; lane < lanes; ++lane) {
        for (unsigned row = 0; row < t; ++row) {
            const size_t idx = fl_order[row / 8] * 16 + (row % 8) * 128 + lane;
            const T v = T(values[idx] & mask);
            const size_t bitpos = size_t(row) * w;
            const size_t word = bitpos / t;
            const unsigned off = unsigned(bitpos % t);
            out[lanes * word + lane] = T(out[lanes * word + lane] | T(v << off));
            if (off + w > t) {
                out[lanes * (word + 1) + lane] =
                        T(out[lanes * (word + 1) + lane] | T(v >> (t - off)));
            }
        }
    }
}

} // namespace sstables::lance::format
