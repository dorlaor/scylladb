/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstdint>
#include <cstddef>
#include <vector>
#include <algorithm>

namespace utils {

/// A Count-Min Sketch with 4-bit counters for frequency estimation.
///
/// Used by the W-TinyLFU cache admission policy to estimate access frequency.
/// Each counter is 4 bits (max value 15), and counters are packed 16 per
/// uint64_t word. The sketch uses 4 independent hash functions (rows) and
/// returns the minimum count across all rows for frequency estimation.
///
/// The sketch width (number of counters per row) is dynamic and can be changed
/// at runtime via resize(). The total memory used is:
///   depth * (2^width_log2 / 16) * sizeof(uint64_t)  =  depth * 2^width_log2 / 2  bytes
class count_min_sketch {
    static constexpr size_t depth = 4;
    static constexpr uint64_t reset_mask = 0x7777777777777777ULL;

    // Hash seeds from splitmix64 sequence, chosen for low correlation between rows.
    static constexpr uint64_t seeds[depth] = {
        0x9e3779b97f4a7c15ULL,
        0xbf58476d1ce4e5b9ULL,
        0x94d049bb133111ebULL,
        0xd6e8feb86659fd93ULL,
    };

    std::vector<uint64_t> _table;
    size_t _width_log2;
    size_t _width;
    size_t _width_mask;
    size_t _words_per_row;

    static uint64_t mix(uint64_t key, uint64_t seed) noexcept {
        uint64_t h = key * seed;
        h ^= h >> 32;
        h *= 0xd6e8feb86659fd93ULL;
        h ^= h >> 32;
        return h;
    }

    size_t counter_index(size_t row, uint64_t key) const noexcept {
        return mix(key, seeds[row]) & _width_mask;
    }

    static uint8_t get_counter(uint64_t word, size_t pos) noexcept {
        return (word >> (pos * 4)) & 0x0FULL;
    }

    size_t word_index(size_t row, size_t col) const noexcept {
        return row * _words_per_row + col / 16;
    }

public:
    /// Construct a sketch with the given number of counters per row.
    /// \param width_log2 Log base 2 of the number of counters per row.
    ///                   Total memory is approximately depth * 2^width_log2 / 2 bytes.
    explicit count_min_sketch(size_t width_log2 = 16)
        : _width_log2(width_log2)
        , _width(size_t(1) << width_log2)
        , _width_mask(_width - 1)
        , _words_per_row(_width / 16)
    {
        _table.resize(depth * _words_per_row, 0);
    }

    void increment(uint64_t key) noexcept {
        for (size_t row = 0; row < depth; ++row) {
            size_t col = counter_index(row, key);
            size_t wi = word_index(row, col);
            size_t pos = col & 15;
            uint8_t val = get_counter(_table[wi], pos);
            if (val < 15) {
                _table[wi] += (1ULL << (pos * 4));
            }
        }
    }

    uint8_t estimate(uint64_t key) const noexcept {
        uint8_t min_val = 15;
        for (size_t row = 0; row < depth; ++row) {
            size_t col = counter_index(row, key);
            size_t wi = word_index(row, col);
            size_t pos = col & 15;
            min_val = std::min(min_val, get_counter(_table[wi], pos));
        }
        return min_val;
    }

    /// Halve all counters (aging/decay).
    void reset() noexcept {
        for (auto& word : _table) {
            word = (word >> 1) & reset_mask;
        }
    }

    /// Zeroes all counters (full reset, as opposed to aging via reset()).
    void clear() noexcept {
        std::fill(_table.begin(), _table.end(), uint64_t(0));
    }

    size_t width() const noexcept { return _width; }
    size_t width_log2() const noexcept { return _width_log2; }

    /// Resize the sketch to a new width (given as log2).
    ///
    /// Counter values are scaled in-place so that the relative frequency
    /// estimates are approximately preserved.  Because counter positions are
    /// determined by hash(key) % width, a width change reshuffles bucket
    /// assignments entirely; scaling the packed words is therefore a lossy
    /// but O(table) approximation that is consistent with the probabilistic
    /// nature of the sketch.
    ///
    /// Growing (new_width > old_width): each old counter is copied to every
    /// corresponding new bucket (1-to-N fan-out).
    /// Shrinking (new_width < old_width): N old counters that map to the same
    /// new bucket are combined by taking their maximum.
    void resize(size_t new_width_log2) noexcept {
        if (new_width_log2 == _width_log2) {
            return;
        }
        size_t new_width = size_t(1) << new_width_log2;
        size_t new_words_per_row = new_width / 16;
        std::vector<uint64_t> new_table(depth * new_words_per_row, 0);

        if (new_width > _width) {
            // Growing: fan out each old counter to (new_width / _width) new counters.
            size_t factor = new_width / _width;
            for (size_t row = 0; row < depth; ++row) {
                for (size_t old_col = 0; old_col < _width; ++old_col) {
                    size_t old_wi  = row * _words_per_row + old_col / 16;
                    size_t old_pos = old_col & 15;
                    uint8_t val = get_counter(_table[old_wi], old_pos);
                    for (size_t f = 0; f < factor; ++f) {
                        size_t new_col = old_col * factor + f;
                        size_t new_wi  = row * new_words_per_row + new_col / 16;
                        size_t new_pos = new_col & 15;
                        new_table[new_wi] |= (static_cast<uint64_t>(val) << (new_pos * 4));
                    }
                }
            }
        } else {
            // Shrinking: combine (old_width / new_width) old counters per new
            // bucket by taking the maximum.
            size_t factor = _width / new_width;
            for (size_t row = 0; row < depth; ++row) {
                for (size_t new_col = 0; new_col < new_width; ++new_col) {
                    uint8_t best = 0;
                    for (size_t f = 0; f < factor; ++f) {
                        size_t old_col = new_col * factor + f;
                        size_t old_wi  = row * _words_per_row + old_col / 16;
                        size_t old_pos = old_col & 15;
                        best = std::max(best, get_counter(_table[old_wi], old_pos));
                    }
                    size_t new_wi  = row * new_words_per_row + new_col / 16;
                    size_t new_pos = new_col & 15;
                    new_table[new_wi] |= (static_cast<uint64_t>(best) << (new_pos * 4));
                }
            }
        }

        _width_log2    = new_width_log2;
        _width         = new_width;
        _width_mask    = new_width - 1;
        _words_per_row = new_words_per_row;
        _table         = std::move(new_table);
    }
};

} // namespace utils
