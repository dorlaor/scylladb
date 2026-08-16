/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Phase 2 correctness and cost test for metadata folding.
//
// Two things are asserted here:
//
//   1. LOSSLESSNESS. shred() -> reassemble() must return exactly the input, for
//      every folding level, across null patterns, TTLs, deletions, and -- most
//      importantly -- rows whose cells carry *different* timestamps. Folding L1
//      is only sound if divergent cells survive via the exception column.
//
//   2. THE DIVERGENCE COST CURVE. The design doc's threat-to-validity #1 was
//      that L1 was only ever measured on data where every cell in a row shares
//      one timestamp (INSERT-written rows), and that UPDATE-assembled rows were
//      unmeasured. This sweeps the divergence rate from 0% to 100% and reports
//      what folding actually costs.
//
//   test_shred roundtrip   -- losslessness
//   test_shred cost        -- divergence sweep, CSV on stdout

#include "schema_mapping.hh"
#include "format/parquet_metadata.hh"

#include <cstdio>
#include <random>
#include <string>
#include <vector>

using namespace sstables::parquet;

namespace {

std::vector<cql_column> make_schema(size_t n_regular) {
    std::vector<cql_column> c;
    c.push_back({"pk", cql_type::bigint, column_kind::partition_key});
    c.push_back({"ck", cql_type::timestamp, column_kind::clustering_key});
    static const cql_type cycle[] = {cql_type::bigint, cql_type::int32,
                                     cql_type::dbl, cql_type::text};
    for (size_t i = 0; i < n_regular; ++i) {
        c.push_back({"c" + std::to_string(i), cycle[i % 4], column_kind::regular});
    }
    return c;
}

struct gen_opts {
    size_t rows = 5000;
    size_t n_regular = 12;
    double null_rate = 0.25;
    double divergence_rate = 0.0;   // fraction of rows with per-cell timestamps
    double ttl_rate = 0.0;
    double delete_rate = 0.0;
    uint64_t seed = 7;
};

std::vector<row> generate(const std::vector<cql_column>& cols, const gen_opts& o) {
    std::mt19937_64 rng(o.seed);
    auto unit = [&] { return double(rng() % 1000000) / 1000000.0; };
    const size_t nreg = cols.size() - 2;
    std::vector<row> rows;
    rows.reserve(o.rows);
    int64_t base_ts = 1700000000000000LL;
    static const char* WORDS[] = {"alpha","beta","gamma","delta","epsilon","zeta","eta"};

    for (size_t i = 0; i < o.rows; ++i) {
        row r;
        r.key.push_back(int64_t(i) * 13 + 1);
        r.key.push_back(base_ts + int64_t(i) * 1000);
        const int64_t row_ts = base_ts + int64_t(rng() % 2000000000);
        const bool diverge = unit() < o.divergence_rate;

        for (size_t k = 0; k < nreg; ++k) {
            if (unit() < o.null_rate) { continue; }   // column absent for this row
            cell c;
            c.live = true;
            // A diverging row gives each cell its own write time, as repeated
            // UPDATEs to different columns would.
            c.timestamp = diverge ? row_ts + int64_t(rng() % 100000) : row_ts;
            switch (cols[2 + k].type) {
            case cql_type::bigint: c.v = int64_t(rng() % 1000000); break;
            case cql_type::int32:  c.v = int32_t(rng() % 1000); break;
            case cql_type::dbl:    c.v = double(rng() % 100000) / 100.0; break;
            default:               c.v = std::string(WORDS[rng() % 7]); break;
            }
            if (unit() < o.ttl_rate)    { c.ttl = int32_t(rng() % 86400); }
            if (unit() < o.delete_rate) { c.live = false; c.v.reset();
                                          c.local_deletion_time = int32_t(rng() % 100000); }
            r.cells.emplace(k, std::move(c));
        }
        rows.push_back(std::move(r));
    }
    return rows;
}

bool same_cell(const cell& a, const cell& b) {
    return a.live == b.live && a.v == b.v && a.timestamp == b.timestamp &&
           a.ttl == b.ttl && a.local_deletion_time == b.local_deletion_time;
}

// L1/L2 do not carry a dead-cell representation, so a tombstoned cell simply is
// not present after a round trip. Compare only what the level claims to keep.
bool compare(const std::vector<row>& in, const std::vector<row>& out,
             folding_level lvl, std::string& why) {
    if (in.size() != out.size()) { why = "row count"; return false; }
    for (size_t i = 0; i < in.size(); ++i) {
        if (in[i].key != out[i].key) { why = "key at row " + std::to_string(i); return false; }
        for (const auto& [k, c] : in[i].cells) {
            const bool keeps = (lvl == folding_level::verbatim) || (c.live && c.v);
            auto it = out[i].cells.find(k);
            if (!keeps) {
                if (it != out[i].cells.end()) {
                    why = "row " + std::to_string(i) + " col " + std::to_string(k) +
                          ": dead cell resurrected";
                    return false;
                }
                continue;
            }
            if (it == out[i].cells.end()) {
                why = "row " + std::to_string(i) + " col " + std::to_string(k) + ": cell lost";
                return false;
            }
            if (lvl == folding_level::verbatim) {
                if (!same_cell(c, it->second)) {
                    why = "row " + std::to_string(i) + " col " + std::to_string(k) + ": cell differs";
                    return false;
                }
            } else {
                if (c.v != it->second.v) {
                    why = "row " + std::to_string(i) + " col " + std::to_string(k) + ": value differs";
                    return false;
                }
                if (c.timestamp != it->second.timestamp) {
                    why = "row " + std::to_string(i) + " col " + std::to_string(k) +
                          ": timestamp " + std::to_string(c.timestamp) + " != " +
                          std::to_string(it->second.timestamp);
                    return false;
                }
                if (c.ttl != it->second.ttl) {
                    why = "row " + std::to_string(i) + " col " + std::to_string(k) + ": ttl differs";
                    return false;
                }
            }
        }
        size_t expect = 0;
        for (const auto& [k, c] : in[i].cells) {
            if (lvl == folding_level::verbatim || (c.live && c.v)) { ++expect; }
        }
        if (out[i].cells.size() != expect) {
            why = "row " + std::to_string(i) + ": cell count " +
                  std::to_string(out[i].cells.size()) + " != " + std::to_string(expect);
            return false;
        }
    }
    return true;
}

int roundtrip() {
    size_t cases = 0, fails = 0;
    const double divs[] = {0.0, 0.05, 0.25, 0.5, 1.0};
    const double nulls[] = {0.0, 0.25, 0.6};
    const double ttls[] = {0.0, 0.3};
    const double dels[] = {0.0, 0.2};
    const size_t widths[] = {1, 5, 40};

    for (size_t w : widths) {
        auto cols = make_schema(w);
        for (double d : divs) for (double nl : nulls) for (double tt : ttls) for (double dl : dels) {
            gen_opts o; o.rows = 800; o.n_regular = w;
            o.divergence_rate = d; o.null_rate = nl; o.ttl_rate = tt; o.delete_rate = dl;
            auto rows = generate(cols, o);
            for (auto lvl : {folding_level::verbatim, folding_level::row_folded,
                             folding_level::uniform})
            for (auto exc : {exception_encoding::sparse, exception_encoding::per_column}) {
                ++cases;
                auto ms = map_schema(cols, lvl, rows, exc);
                auto data = shred(ms, cols, rows);
                auto back = reassemble(ms, cols, data, rows.size());
                std::string why;
                if (!compare(rows, back, ms.level, why)) {
                    ++fails;
                    std::printf("  FAIL w=%zu div=%.2f null=%.2f ttl=%.2f del=%.2f %s(->%s) exc=%s: %s\n",
                                w, d, nl, tt, dl, to_string(lvl), to_string(ms.level),
                                exc == exception_encoding::sparse ? "sparse" : "per-col", why.c_str());
                    if (fails > 8) { std::printf("  (stopping after 8)\n"); goto done; }
                }
            }
        }
    }
done:
    std::printf("round-trip: %zu cases, %zu failures\n", cases, fails);
    std::printf("%s\n", fails ? "SHRED ROUNDTRIP FAIL" : "SHRED ROUNDTRIP PASS");
    return fails ? 1 : 0;
}

int cost() {
    auto cols = make_schema(40);
    std::printf("divergence,variant,bytes,leaf_columns,vs_base\n");
    size_t base = 0;
    for (double d : {0.0, 0.01, 0.05, 0.10, 0.25, 0.50, 1.00}) {
        gen_opts o; o.rows = 20000; o.n_regular = 40; o.divergence_rate = d; o.null_rate = 0.25;
        auto rows = generate(cols, o);
        struct { const char* name; folding_level lvl; exception_encoding exc; } variants[] = {
            {"L0",         folding_level::verbatim,   exception_encoding::sparse},
            {"L1-percol",  folding_level::row_folded, exception_encoding::per_column},
            {"L1-sparse",  folding_level::row_folded, exception_encoding::sparse},
        };
        for (auto& v : variants) {
            auto ms  = map_schema(cols, v.lvl, rows, v.exc);
            auto img = write_rows(cols, rows, v.lvl, {}, v.exc);
            if (d == 0.0 && std::string(v.name) == "L1-sparse") { base = img.size(); }
            std::printf("%.2f,%s,%zu,%zu,%.2fx\n", d, v.name, img.size(),
                        ms.leaf_count(), base ? double(img.size()) / double(base) : 0.0);
        }
    }
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) { std::fprintf(stderr, "usage: %s {roundtrip|cost}\n", argv[0]); return 2; }
    try {
        std::string c = argv[1];
        if (c == "roundtrip") { return roundtrip(); }
        if (c == "cost")      { return cost(); }
        std::fprintf(stderr, "unknown command\n"); return 2;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what()); return 1;
    }
}
