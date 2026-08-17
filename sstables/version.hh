/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <type_traits>
#include <seastar/core/sstring.hh>
#include <seastar/core/enum.hh>

namespace sstables {

// `pq` encodes the Data component as Parquet. Everything else about the sstable
// -- the component set, the statistics layout, the index -- follows the `m`
// family, because only the row encoding changes. See
// docs/dev/parquet-storage-format.md section 5.2.
enum class sstable_version_types { ka, la, mc, md, me, ms, mt, pq };
enum class sstable_format_types { big };

// NOTE: `pq` is intentionally absent from both arrays below. These mean "versions
// the node can actually read / write", and pq is not there yet.
//
// The writer and reader are wired in: a pq sstable is written with a full
// component set, streams back through sstable::make_reader within bounded
// memory, and its Data component opens in pyarrow
// (test/boost/sstable_parquet_test.cc). Row markers, row tombstones and
// partition tombstones round-trip as of 2026-08-17.
//
// Row markers, row and partition tombstones, and static rows all round-trip.
// What still blocks membership is coverage, not plumbing. Enrolling pq here runs
// it through run_mutation_source_tests, which needs four things it does not have:
// range tombstones, multi-cell collections, counters (excluded by design), and
// intra-partition forwarding -- fast_forward_to(position_range) is a no-op.
// The first three make the writer throw rather than drop data. Adding pq here would
// enrol it in the generic suites -- including
// sstable_conforms_to_mutation_source_test -- which exercise all of those.
// Add it in the change that closes those gaps; see
// docs/dev/parquet-storage-format.md section 11, item 11.
constexpr std::array<sstable_version_types, 7> all_sstable_versions = {
    sstable_version_types::ka,
    sstable_version_types::la,
    sstable_version_types::mc,
    sstable_version_types::md,
    sstable_version_types::me,
    sstable_version_types::ms,
    sstable_version_types::mt,
};

constexpr std::array<sstable_version_types, 5> writable_sstable_versions = {
    sstable_version_types::mc,
    sstable_version_types::md,
    sstable_version_types::me,
    sstable_version_types::ms,
    sstable_version_types::mt,
};

constexpr sstable_version_types oldest_writable_sstable_format = sstable_version_types::mc;

inline auto get_highest_sstable_version() {
    return all_sstable_versions[all_sstable_versions.size() - 1];
}

sstable_version_types version_from_string(std::string_view s);
sstable_format_types format_from_string(std::string_view s);

// `pq` sorts after every mx version, but it is a different format rather than a
// newer generation of the same one. Comparisons of the form "v >= mc" are fine
// where they ask a structural question pq also answers yes to (index shape,
// filter format, serialization header). They are NOT fine where the code infers
// "we already have an sstable at version X, so the cluster must support X" --
// pq's ordinal position implies nothing about mt or ms support. Use this for
// those.
constexpr bool implies_mx_generation(sstable_version_types v, sstable_version_types at_least) {
    return v != sstable_version_types::pq && v >= at_least;
}

bool has_summary_and_index(sstable_version_types v);
bool uses_legacy_dk_order(sstable_version_types v);

extern const std::unordered_map<sstable_version_types, seastar::sstring, seastar::enum_hash<sstable_version_types>> version_string;
extern const std::unordered_map<sstable_format_types, seastar::sstring, seastar::enum_hash<sstable_format_types>> format_string;

}

template <>
struct fmt::formatter<sstables::sstable_version_types> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const sstables::sstable_version_types& version, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", sstables::version_string.at(version));
    }
};

template <>
struct fmt::formatter<sstables::sstable_format_types> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const sstables::sstable_format_types& format, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}", sstables::format_string.at(format));
    }
};
