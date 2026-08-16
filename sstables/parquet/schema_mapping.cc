/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "schema_mapping.hh"

#include <algorithm>
#include <stdexcept>
#include <unordered_map>

namespace sstables::parquet {

const char* to_string(folding_level l) {
    switch (l) {
    case folding_level::verbatim:   return "L0";
    case folding_level::row_folded: return "L1";
    case folding_level::uniform:    return "L2";
    }
    return "?";
}

namespace {

// The timestamp a row folds to: the one most of its live cells agree on.
// Choosing the mode (rather than, say, the max) minimises how many cells need
// an exception entry.
std::optional<int64_t> modal_timestamp(const row& r) {
    std::unordered_map<int64_t, int> freq;
    for (const auto& [i, c] : r.cells) {
        if (c.v || !c.live) { ++freq[c.timestamp]; }
    }
    if (freq.empty()) { return std::nullopt; }
    int64_t best = freq.begin()->first;
    int bestn = 0;
    for (auto& [ts, n] : freq) {
        if (n > bestn || (n == bestn && ts < best)) { best = ts; bestn = n; }
    }
    return best;
}

void push_value(column_data& cd, phys_type pt, const value& v) {
    switch (pt) {
    case phys_type::int32:      cd.i32.push_back(std::get<int32_t>(v)); break;
    case phys_type::int64:      cd.i64.push_back(std::get<int64_t>(v)); break;
    case phys_type::dbl:        cd.f64.push_back(std::get<double>(v)); break;
    case phys_type::byte_array: cd.str.push_back(std::get<std::string>(v)); break;
    default: throw std::runtime_error("shred: unsupported physical type");
    }
}

void push_absent(column_data& cd, phys_type pt) {
    switch (pt) {
    case phys_type::int32:      cd.i32.push_back(0); break;
    case phys_type::int64:      cd.i64.push_back(0); break;
    case phys_type::dbl:        cd.f64.push_back(0.0); break;
    case phys_type::byte_array: cd.str.emplace_back(); break;
    default: throw std::runtime_error("shred: unsupported physical type");
    }
}

value read_value(const column_data& cd, phys_type pt, size_t i) {
    switch (pt) {
    case phys_type::int32:      return cd.i32[i];
    case phys_type::int64:      return cd.i64[i];
    case phys_type::dbl:        return cd.f64[i];
    case phys_type::byte_array: return cd.str[i];
    default: throw std::runtime_error("reassemble: unsupported physical type");
    }
}

// Zigzag varint, used for the exception deltas.
void put_zigzag(std::string& o, int64_t v) {
    uint64_t u = (uint64_t(v) << 1) ^ uint64_t(v >> 63);
    while (u >= 0x80) { o.push_back(char(uint8_t(u) | 0x80)); u >>= 7; }
    o.push_back(char(uint8_t(u)));
}
int64_t get_zigzag(const std::string& s, size_t& i) {
    uint64_t u = 0; int shift = 0;
    while (i < s.size()) {
        uint8_t b = uint8_t(s[i++]);
        u |= uint64_t(b & 0x7F) << shift;
        if (!(b & 0x80)) { break; }
        shift += 7;
    }
    return int64_t(u >> 1) ^ -int64_t(u & 1);
}

} // namespace

mapped_schema map_schema(const std::vector<cql_column>& cols,
                         folding_level requested,
                         const std::vector<row>& rows,
                         exception_encoding exc) {
    mapped_schema ms;
    ms.level = requested;
    ms.exc_encoding = exc;

    std::vector<size_t> key_idx, reg_idx;
    for (size_t i = 0; i < cols.size(); ++i) {
        (cols[i].kind == column_kind::regular ? reg_idx : key_idx).push_back(i);
    }
    ms.n_key = key_idx.size();
    ms.n_regular = reg_idx.size();
    ms.ts_exc_index.assign(reg_idx.size(), std::nullopt);
    ms.meta_base_index.assign(reg_idx.size(), std::nullopt);

    // ---- inspect the data before deciding what to materialise
    bool any_ttl = false, any_deletion = false;
    std::vector<bool> col_diverges(reg_idx.size(), false);
    std::optional<int64_t> single_ts;
    bool all_same_ts = true;

    for (const auto& r : rows) {
        auto mt = modal_timestamp(r);
        if (mt) {
            if (!single_ts) { single_ts = mt; }
            else if (*single_ts != *mt) { all_same_ts = false; }
        }
        for (const auto& [ci, c] : r.cells) {
            if (c.ttl) { any_ttl = true; }
            if (c.local_deletion_time || !c.live) { any_deletion = true; }
            if (mt && c.timestamp != *mt) {
                col_diverges[ci] = true;
                all_same_ts = false;
            }
        }
    }
    if (requested == folding_level::uniform && !(all_same_ts && !any_ttl && !any_deletion)) {
        // Precondition broken -- fall back rather than lose information.
        ms.level = folding_level::row_folded;
    }

    // ---- key columns, always required and always present
    for (size_t i : key_idx) {
        ms.columns.push_back(column_spec{cols[i].name, phys_of(cols[i].type),
                                         repetition::required, converted_of(cols[i].type),
                                         cols[i].type == cql_type::bigint ||
                                         cols[i].type == cql_type::timestamp
                                             ? std::optional<encoding>(encoding::delta_binary_packed)
                                             : std::nullopt});
    }

    // ---- regular columns
    for (size_t k = 0; k < reg_idx.size(); ++k) {
        const auto& c = cols[reg_idx[k]];
        // Regular columns default to PLAIN and let zstd do the work.
        //
        // Both of the obvious type-based rules were tried on real data and both
        // LOST (docs/dev/parquet-storage-format.md section 10.3f):
        //   BYTE_STREAM_SPLIT on doubles      2 562 753 -> 3 968 805 bytes
        //   DELTA_BINARY_PACKED on bigints    2 562 753 -> 2 569 567 bytes
        // Transposing or delta-ing destroys the whole-value repetition that zstd
        // was already exploiting -- money-shaped doubles and low-cardinality ids
        // repeat exactly, and those repeats compress better than any residual.
        //
        // The key columns and __ts keep DELTA_BINARY_PACKED (set elsewhere)
        // because they are monotonic by construction, where it demonstrably wins.
        // Choosing per column from the data is the real answer; see open
        // question 9.
        ms.columns.push_back(column_spec{c.name, phys_of(c.type), repetition::optional,
                                         converted_of(c.type), std::nullopt});
    }

    if (ms.level == folding_level::verbatim) {
        // Four metadata leaves per regular column, unconditionally. This is the
        // 2020 mapping and it is here to be measured against, not used.
        for (size_t k = 0; k < reg_idx.size(); ++k) {
            ms.meta_base_index[k] = ms.columns.size();
            const auto& nm = cols[reg_idx[k]].name;
            ms.columns.push_back({"__live_" + nm, phys_type::int32, repetition::required, std::nullopt, std::nullopt});
            ms.columns.push_back({"__ts_"   + nm, phys_type::int64, repetition::required, std::nullopt, std::nullopt});
            ms.columns.push_back({"__ttl_"  + nm, phys_type::int32, repetition::optional, std::nullopt, std::nullopt});
            ms.columns.push_back({"__ldt_"  + nm, phys_type::int32, repetition::optional, std::nullopt, std::nullopt});
        }
    } else if (ms.level == folding_level::row_folded) {
        ms.ts_index = ms.columns.size();
        ms.columns.push_back({"__ts", phys_type::int64, repetition::required, std::nullopt,
                              encoding::delta_binary_packed});
        bool any_divergence = false;
        for (bool b : col_diverges) { any_divergence = any_divergence || b; }
        if (any_divergence) {
            if (ms.exc_encoding == exception_encoding::sparse) {
                // Two leaves, independent of table width: a bitmap of which
                // columns diverge in this row, and the packed deltas.
                ms.tsx_mask_index = ms.columns.size();
                ms.columns.push_back({"__tsx_mask", phys_type::byte_array,
                                      repetition::optional, std::nullopt, std::nullopt});
                ms.tsx_vals_index = ms.columns.size();
                ms.columns.push_back({"__tsx_vals", phys_type::byte_array,
                                      repetition::optional, std::nullopt, std::nullopt});
            } else {
                // One leaf per diverging column -- the shape that measured badly.
                for (size_t k = 0; k < reg_idx.size(); ++k) {
                    if (col_diverges[k]) {
                        ms.ts_exc_index[k] = ms.columns.size();
                        ms.columns.push_back({"__tsx_" + cols[reg_idx[k]].name, phys_type::int64,
                                              repetition::optional, std::nullopt, std::nullopt});
                    }
                }
            }
        }
        if (any_ttl) {
            for (size_t k = 0; k < reg_idx.size(); ++k) {
                ms.columns.push_back({"__ttl_" + cols[reg_idx[k]].name, phys_type::int32,
                                      repetition::optional, std::nullopt, std::nullopt});
            }
        }
        if (any_deletion) {
            for (size_t k = 0; k < reg_idx.size(); ++k) {
                ms.columns.push_back({"__ldt_" + cols[reg_idx[k]].name, phys_type::int32,
                                      repetition::optional, std::nullopt, std::nullopt});
            }
        }
        ms.uniform_ts = std::nullopt;
        // Record which optional groups exist so reassemble() can invert.
        ms.meta_base_index.assign(reg_idx.size(), std::nullopt);
        if (any_ttl || any_deletion) {
            size_t base = ms.columns.size();
            if (any_deletion) { base -= reg_idx.size(); }
            if (any_ttl)      { base -= reg_idx.size(); }
            for (size_t k = 0; k < reg_idx.size(); ++k) { ms.meta_base_index[k] = base + k; }
        }
    } else {
        ms.uniform_ts = single_ts.value_or(0);
    }
    return ms;
}

std::vector<column_data> shred(const mapped_schema& ms,
                               const std::vector<cql_column>& cols,
                               const std::vector<row>& rows) {
    std::vector<size_t> key_idx, reg_idx;
    for (size_t i = 0; i < cols.size(); ++i) {
        (cols[i].kind == column_kind::regular ? reg_idx : key_idx).push_back(i);
    }
    std::vector<column_data> out(ms.columns.size());
    const bool has_ttl_block = ms.level == folding_level::row_folded &&
                               ms.meta_base_index[0].has_value();

    for (const auto& r : rows) {
        // key columns
        for (size_t k = 0; k < key_idx.size(); ++k) {
            push_value(out[k], ms.columns[k].type, r.key[k]);
        }
        auto mt = modal_timestamp(r);

        for (size_t k = 0; k < reg_idx.size(); ++k) {
            const size_t vcol = key_idx.size() + k;
            auto it = r.cells.find(k);
            const bool present = it != r.cells.end() && it->second.v.has_value();
            out[vcol].def_levels.push_back(present ? 1 : 0);
            if (present) { push_value(out[vcol], ms.columns[vcol].type, *it->second.v); }
            else         { push_absent(out[vcol], ms.columns[vcol].type); }

            if (ms.level == folding_level::verbatim) {
                const size_t b = *ms.meta_base_index[k];
                const bool have = it != r.cells.end();
                out[b + 0].i32.push_back(have && it->second.live ? 1 : 0);
                out[b + 1].i64.push_back(have ? it->second.timestamp : 0);
                bool has_ttl = have && it->second.ttl.has_value();
                out[b + 2].def_levels.push_back(has_ttl ? 1 : 0);
                out[b + 2].i32.push_back(has_ttl ? *it->second.ttl : 0);
                bool has_ldt = have && it->second.local_deletion_time.has_value();
                out[b + 3].def_levels.push_back(has_ldt ? 1 : 0);
                out[b + 3].i32.push_back(has_ldt ? *it->second.local_deletion_time : 0);
            } else if (ms.level == folding_level::row_folded) {
                if (ms.exc_encoding == exception_encoding::per_column && ms.ts_exc_index[k]) {
                    const size_t x = *ms.ts_exc_index[k];
                    const bool diverges = it != r.cells.end() && mt && it->second.timestamp != *mt;
                    out[x].def_levels.push_back(diverges ? 1 : 0);
                    out[x].i64.push_back(diverges ? it->second.timestamp : 0);
                }
                if (has_ttl_block) {
                    const size_t b = *ms.meta_base_index[k];
                    bool has_ttl = it != r.cells.end() && it->second.ttl.has_value();
                    out[b].def_levels.push_back(has_ttl ? 1 : 0);
                    out[b].i32.push_back(has_ttl ? *it->second.ttl : 0);
                }
            }
        }
        if (ms.tsx_mask_index) {
            // Build the row's exception bitmap and delta blob in column order.
            const size_t nbytes = (reg_idx.size() + 7) / 8;
            std::string mask(nbytes, '\0'), vals;
            bool any = false;
            for (size_t k = 0; k < reg_idx.size(); ++k) {
                auto it = r.cells.find(k);
                if (it == r.cells.end() || !mt || it->second.timestamp == *mt) { continue; }
                mask[k / 8] = char(uint8_t(mask[k / 8]) | uint8_t(1u << (k % 8)));
                put_zigzag(vals, it->second.timestamp - *mt);
                any = true;
            }
            out[*ms.tsx_mask_index].def_levels.push_back(any ? 1 : 0);
            out[*ms.tsx_mask_index].str.push_back(any ? mask : std::string());
            out[*ms.tsx_vals_index].def_levels.push_back(any ? 1 : 0);
            out[*ms.tsx_vals_index].str.push_back(any ? vals : std::string());
        }
        if (ms.ts_index) { out[*ms.ts_index].i64.push_back(mt.value_or(0)); }
    }
    return out;
}

std::vector<row> reassemble(const mapped_schema& ms,
                            const std::vector<cql_column>& cols,
                            const std::vector<column_data>& cd,
                            size_t nrows) {
    std::vector<size_t> key_idx, reg_idx;
    for (size_t i = 0; i < cols.size(); ++i) {
        (cols[i].kind == column_kind::regular ? reg_idx : key_idx).push_back(i);
    }
    const bool has_ttl_block = ms.level == folding_level::row_folded &&
                               !ms.meta_base_index.empty() && ms.meta_base_index[0].has_value();

    std::vector<row> out(nrows);
    for (size_t i = 0; i < nrows; ++i) {
        row& r = out[i];
        for (size_t k = 0; k < key_idx.size(); ++k) {
            r.key.push_back(read_value(cd[k], ms.columns[k].type, i));
        }
        const int64_t row_ts =
            ms.ts_index ? cd[*ms.ts_index].i64[i] : ms.uniform_ts.value_or(0);

        // Decode this row's sparse exceptions once, into column -> timestamp.
        std::map<size_t, int64_t> exc;
        if (ms.tsx_mask_index && cd[*ms.tsx_mask_index].def_levels[i]) {
            const std::string& mask = cd[*ms.tsx_mask_index].str[i];
            const std::string& vals = cd[*ms.tsx_vals_index].str[i];
            size_t pos = 0;
            for (size_t k = 0; k < reg_idx.size(); ++k) {
                if (k / 8 < mask.size() && (uint8_t(mask[k / 8]) & (1u << (k % 8)))) {
                    exc.emplace(k, row_ts + get_zigzag(vals, pos));
                }
            }
        }

        for (size_t k = 0; k < reg_idx.size(); ++k) {
            const size_t vcol = key_idx.size() + k;
            const bool present = cd[vcol].def_levels[i] != 0;

            if (ms.level == folding_level::verbatim) {
                const size_t b = *ms.meta_base_index[k];
                const bool live = cd[b + 0].i32[i] != 0;
                const int64_t ts = cd[b + 1].i64[i];
                // A column with no cell at all was written as absent + dead.
                if (!present && !live && ts == 0) { continue; }
                cell c;
                c.live = live;
                c.timestamp = ts;
                if (present) { c.v = read_value(cd[vcol], ms.columns[vcol].type, i); }
                if (cd[b + 2].def_levels[i]) { c.ttl = cd[b + 2].i32[i]; }
                if (cd[b + 3].def_levels[i]) { c.local_deletion_time = cd[b + 3].i32[i]; }
                r.cells.emplace(k, std::move(c));
            } else {
                if (!present) { continue; }
                cell c;
                c.live = true;
                c.v = read_value(cd[vcol], ms.columns[vcol].type, i);
                c.timestamp = row_ts;
                if (auto e = exc.find(k); e != exc.end()) {
                    c.timestamp = e->second;
                } else if (ms.exc_encoding == exception_encoding::per_column &&
                           ms.ts_exc_index[k] && cd[*ms.ts_exc_index[k]].def_levels[i]) {
                    c.timestamp = cd[*ms.ts_exc_index[k]].i64[i];
                }
                if (has_ttl_block) {
                    const size_t b = *ms.meta_base_index[k];
                    if (cd[b].def_levels[i]) { c.ttl = cd[b].i32[i]; }
                }
                r.cells.emplace(k, std::move(c));
            }
        }
    }
    return out;
}

std::vector<uint8_t> write_rows(const std::vector<cql_column>& cols,
                                const std::vector<row>& rows,
                                folding_level level,
                                writer_options opt,
                                exception_encoding exc) {
    auto ms = map_schema(cols, level, rows, exc);
    auto data = shred(ms, cols, rows);
    file_writer w(ms.columns, opt);
    w.add_key_value("scylla.folding_level", to_string(ms.level));
    if (ms.uniform_ts) {
        w.add_key_value("scylla.uniform_timestamp", std::to_string(*ms.uniform_ts));
    }
    w.add_row_group(data);
    return w.finish();
}

} // namespace sstables::parquet
