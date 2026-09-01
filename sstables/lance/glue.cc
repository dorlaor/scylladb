/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/lance/glue.hh"

#include <stdexcept>

namespace sstables::lance {

using pq::format::phys_type;
using pq::format::repetition;

format::lphys lphys_of(const pq::format::column_spec& spec) {
    switch (spec.type) {
    case phys_type::int32: return format::lphys::i32;
    case phys_type::int64: return format::lphys::i64;
    case phys_type::dbl: return format::lphys::f64;
    case phys_type::byte_array: return format::lphys::bytes;
    default:
        throw std::invalid_argument("lance: unsupported physical type for leaf '" + spec.name + "'");
    }
}

static std::string logical_type_of(const pq::format::column_spec& spec) {
    using pq::format::converted;
    if (spec.converted_type == int32_t(converted::utf8)) { return "string"; }
    if (spec.converted_type == int32_t(converted::timestamp_millis)) { return "timestamp:ms:-"; }
    switch (spec.type) {
    case phys_type::int32: return "int32";
    case phys_type::int64: return "int64";
    case phys_type::dbl: return "double";
    default: return "binary";
    }
}

std::vector<format::lance_column_spec> specs_of(const pq::mapped_schema& ms) {
    std::vector<format::lance_column_spec> specs;
    specs.reserve(ms.columns.size());
    for (const auto& c : ms.columns) {
        if (c.max_rep != 0) {
            // Unreachable when the DDL guard holds: storage_format='lance'
            // refuses tables with non-frozen collections or counters, the
            // only sources of repeated leaves.
            throw std::invalid_argument(
                    "lance: leaf '" + c.name + "' is repeated; non-frozen collections and "
                    "counters are not supported by the lance storage format");
        }
        format::lance_column_spec s;
        s.name = c.name;
        s.type = lphys_of(c);
        s.nullable = c.rep == repetition::optional;
        s.logical_type = logical_type_of(c);
        specs.push_back(std::move(s));
    }
    return specs;
}

std::vector<format::column_values> to_lance_batch(const pq::mapped_schema& ms,
                                                  std::vector<pq::format::column_data> cols) {
    if (cols.size() != ms.columns.size()) {
        throw std::invalid_argument("lance: shredded column count disagrees with the schema");
    }
    // Both value models are DENSE: shred() materialises a slot for absent
    // values (push_absent) exactly as Lance's visible-null rule wants, so the
    // values move over untouched and only the def semantics invert
    // (Parquet: def == 1 means present; Lance: def == 0 means valid).
    std::vector<format::column_values> out;
    out.reserve(cols.size());
    for (size_t i = 0; i < cols.size(); ++i) {
        auto& src = cols[i];
        format::column_values dst;
        dst.def.reserve(src.def_levels.size());
        for (auto d : src.def_levels) {
            dst.def.push_back(d != 0 ? 0 : 1);
        }
        dst.i32 = std::move(src.i32);
        dst.i64 = std::move(src.i64);
        dst.f64 = std::move(src.f64);
        dst.str = std::move(src.str);
        const size_t rows = dst.rows();
        if (!dst.def.empty() && dst.def.size() != rows) {
            throw std::invalid_argument("lance: def levels disagree with value count");
        }
        out.push_back(std::move(dst));
    }
    return out;
}

pq::format::column_data to_column_data(const pq::format::column_spec& spec,
                                       format::column_values vals) {
    pq::format::column_data out;
    const size_t rows = vals.rows();
    if (spec.rep != repetition::required) {
        // Dense on both sides (see to_lance_batch); reassemble() consults the
        // def levels before every value access, so an optional leaf must
        // carry them even for a fully valid page (Lance drops the channel
        // per page when nothing is null).
        out.def_levels.reserve(rows);
        for (size_t r = 0; r < rows; ++r) {
            const bool valid = vals.def.empty() || vals.def[r] == 0;
            out.def_levels.push_back(valid ? 1 : 0);
        }
    }
    out.i32 = std::move(vals.i32);
    out.i64 = std::move(vals.i64);
    out.f64 = std::move(vals.f64);
    out.str = std::move(vals.str);
    return out;
}

pq::mapped_schema recover_mapped_schema(const format::file_descriptor& fd,
                                        const std::vector<pq::cql_column>& cols) {
    std::vector<std::string> leaves;
    leaves.reserve(fd.fields.size());
    for (const auto& f : fd.fields) {
        leaves.push_back(f.name);
    }
    return pq::recover_mapped_schema_from_leaves(leaves,
            [&] (const std::string& k) -> const std::string* {
                auto it = fd.schema_metadata.find(k);
                return it == fd.schema_metadata.end() ? nullptr : &it->second;
            },
            cols);
}

} // namespace sstables::lance
