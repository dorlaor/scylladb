/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Writes Parquet files with our own writer, for two purposes:
//   * our reader parses them back (self-consistency);
//   * pyarrow/DuckDB read them and agree on every value (interop --
//     checked by writer_interop.py, which owns the real assertion).
//
//   test_writer emit <dir>   -- write the fixture set and a JSON manifest
//
// The manifest records the exact values written so the Python side can compare
// without trusting anything our code produced.

#include "parquet_writer.hh"
#include "parquet_metadata.hh"

#include <cstdio>
#include <fstream>
#include <random>
#include <string>
#include <vector>

using namespace sstables::parquet::format;

namespace {

struct fixture {
    std::string name;
    writer_options opt;
    size_t rows;
    bool with_nulls;
    bool delta_ts;
};

void jstr(std::ostream& o, const std::string& s) {
    o << '"';
    for (char c : s) {
        if (c == '"' || c == '\\') { o << '\\' << c; }
        else if (uint8_t(c) < 0x20) { char b[8]; std::snprintf(b, sizeof b, "\\u%04x", c); o << b; }
        else { o << c; }
    }
    o << '"';
}

int emit(const std::string& dir) {
    std::vector<fixture> fx = {
        {"w_plain_nonull",  {codec::zstd, 3, 20000, false, 1u<<20, true}, 50000, false, false},
        {"w_plain_nulls",   {codec::zstd, 3, 20000, false, 1u<<20, true}, 50000, true,  false},
        {"w_dict_nulls",    {codec::zstd, 3, 20000, true,  1u<<20, true}, 50000, true,  false},
        {"w_delta_ts",      {codec::zstd, 3, 20000, true,  1u<<20, true}, 50000, true,  true},
        {"w_uncompressed",  {codec::uncompressed, 0, 8192, true, 1u<<20, true}, 20000, true, true},
        {"w_multipage",     {codec::zstd, 3, 1000,  false, 1u<<20, true}, 20000, true,  false},
        {"w_tiny",          {codec::zstd, 3, 20000, true,  1u<<20, true}, 7,     true,  false},
    };

    std::ofstream man(dir + "/manifest.json");
    man << "[\n";
    bool firstfx = true;

    for (const auto& f : fx) {
        std::mt19937_64 rng(1234);
        const size_t n = f.rows;

        std::vector<column_spec> schema = {
            {"id",    phys_type::int64,      repetition::required, std::nullopt, std::nullopt},
            {"grade", phys_type::int32,      repetition::optional, std::nullopt, std::nullopt},
            {"amount",phys_type::dbl,        repetition::optional, std::nullopt, std::nullopt},
            {"status",phys_type::byte_array, repetition::optional,
             int32_t(converted::utf8), std::nullopt},
            {"__ts",  phys_type::int64,      repetition::required, std::nullopt,
             f.delta_ts ? std::optional<encoding>(encoding::delta_binary_packed) : std::nullopt},
        };

        std::vector<column_data> cols(schema.size());
        // id: dense, monotonically increasing
        cols[0].i64.reserve(n);
        // grade / amount / status: optional, ~25% null when with_nulls
        cols[1].i32.reserve(n); cols[1].def_levels.reserve(n);
        cols[2].f64.reserve(n); cols[2].def_levels.reserve(n);
        cols[3].str.reserve(n); cols[3].def_levels.reserve(n);
        cols[4].i64.reserve(n);

        static const char* STATUS[] = {"active", "pending", "closed", "archived", "error"};
        int64_t ts = 1700000000000000LL;

        for (size_t i = 0; i < n; ++i) {
            cols[0].i64.push_back(int64_t(i) * 7 + 3);

            // Every draw below is unconditional so the value stream is trivial
            // to mirror from Python in writer_interop.py. Short-circuiting the
            // draws would make the sequence depend on the null pattern.
            const bool present = !f.with_nulls || (rng() % 4) != 0;
            const int32_t gv = int32_t(rng() % 100);
            cols[1].def_levels.push_back(present ? 1 : 0);
            cols[1].i32.push_back(present ? gv : 0);

            const bool p2 = !f.with_nulls || (rng() % 4) != 0;
            const double av = double(rng() % 1000000) / 100.0;
            cols[2].def_levels.push_back(p2 ? 1 : 0);
            cols[2].f64.push_back(p2 ? av : 0.0);

            const bool p3 = !f.with_nulls || (rng() % 4) != 0;
            const std::string sv = STATUS[rng() % 5];
            cols[3].def_levels.push_back(p3 ? 1 : 0);
            cols[3].str.push_back(p3 ? sv : std::string());

            ts += int64_t(rng() % 1000);        // realistic: monotone with jitter
            cols[4].i64.push_back(ts);
        }

        file_writer w(schema, f.opt);
        w.add_key_value("scylla.folding_level", "L1");
        w.add_key_value("scylla.table", "pqlab.demo");
        w.add_row_group(cols);
        auto img = w.finish();

        std::string path = dir + "/" + f.name + ".parquet";
        std::ofstream(path, std::ios::binary)
            .write(reinterpret_cast<const char*>(img.data()), std::streamsize(img.size()));

        // Self-check with our own reader before handing to pyarrow.
        auto md = parse_footer(img);
        if (md.num_rows != int64_t(n) || md.leaf_count() != schema.size()) {
            std::printf("  SELF-CHECK FAIL %s: rows=%lld leaves=%zu\n",
                        f.name.c_str(), (long long)md.num_rows, md.leaf_count());
            return 1;
        }

        if (!firstfx) { man << ",\n"; }
        firstfx = false;
        man << "  {\"name\": "; jstr(man, f.name);
        man << ", \"path\": "; jstr(man, path);
        man << ", \"rows\": " << n
            << ", \"bytes\": " << img.size()
            << ", \"compression\": \"" << to_string(f.opt.compression) << "\""
            << ", \"dict\": " << (f.opt.use_dictionary ? "true" : "false")
            << ", \"page_values\": " << f.opt.page_values
            << ", \"delta_ts\": " << (f.delta_ts ? "true" : "false")
            << ", \"nulls\": " << (f.with_nulls ? "true" : "false")
            << ", \"seed\": 1234}";
        std::printf("  wrote %-16s %8zu rows  %9zu B  (%s%s%s)\n", f.name.c_str(), n, img.size(),
                    to_string(f.opt.compression),
                    f.opt.use_dictionary ? ", dict" : "",
                    f.delta_ts ? ", delta-ts" : "");
    }
    man << "\n]\n";
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 3) { std::fprintf(stderr, "usage: %s emit <dir>\n", argv[0]); return 2; }
    try {
        if (std::string(argv[1]) == "emit") { return emit(argv[2]); }
        std::fprintf(stderr, "unknown command\n"); return 2;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what()); return 1;
    }
}
