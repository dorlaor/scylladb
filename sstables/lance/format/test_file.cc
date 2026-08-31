/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Whole-file tests for the Lance codec. Three subcommands:
//   roundtrip <outdir>  -- write multi-column, multi-page files and read them
//                          back value-exact, whole and sliced.
//   emit <outdir>       -- write deterministic files for writer_interop.py
//                          (pylance is the validator).
//   dump <file.lance>   -- decode every column to TSV (hex for bytes), used
//                          by crossread.py to compare us against pylance on
//                          pylance-written files.

#include "lance_reader.hh"
#include "lance_writer.hh"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <random>

using namespace sstables::lance::format;

static int failures = 0;

#define CHECK(cond) do { \
    if (!(cond)) { \
        std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        ++failures; \
    } \
} while (0)


struct table {
    std::vector<lance_column_spec> specs;
    // column-major expected values
    std::vector<column_values> cols;
    size_t rows = 0;
};

// Deterministic full-bit-width mixer, reproduced in gen_golden.py.
static uint64_t mix(uint64_t i) {
    return (i + 1) * 0x9E3779B97F4A7C15ull;
}

static table make_table(size_t rows) {
    table t;
    t.rows = rows;
    t.specs = {
        {.name = "pk", .type = lphys::i64, .nullable = false, .logical_type = "int64", .field_metadata = {}},
        {.name = "v32", .type = lphys::i32, .nullable = true, .logical_type = "int32", .field_metadata = {}},
        {.name = "x", .type = lphys::f64, .nullable = true, .logical_type = "double", .field_metadata = {}},
        {.name = "s", .type = lphys::bytes, .nullable = true, .logical_type = "string", .field_metadata = {}},
        {.name = "fat", .type = lphys::bytes, .nullable = true, .logical_type = "string", .field_metadata = {}},
        {.name = "gone", .type = lphys::i64, .nullable = true, .logical_type = "int64", .field_metadata = {}},
    };
    t.cols.resize(t.specs.size());
    for (size_t i = 0; i < rows; ++i) {
        const uint64_t h = mix(i);
        t.cols[0].i64.push_back(int64_t(h >> 1));

        const bool v32_null = i % 7 == 0;
        t.cols[1].def.push_back(v32_null ? 1 : 0);
        t.cols[1].i32.push_back(v32_null ? 0 : int32_t(uint32_t(h)));

        t.cols[2].def.push_back(0);
        t.cols[2].f64.push_back(double(i) * 1.5);

        const bool s_null = i % 11 == 0;
        t.cols[3].def.push_back(s_null ? 1 : 0);
        std::string s;
        if (!s_null) {
            s = "s-" + std::to_string(h % 100000);
        }
        t.cols[3].str.push_back(std::move(s));

        const bool fat_null = i % 13 == 0;
        t.cols[4].def.push_back(fat_null ? 1 : 0);
        std::string fat;
        if (!fat_null) {
            fat.resize(280 + i % 90);
            for (size_t k = 0; k < fat.size(); ++k) {
                fat[k] = char('A' + (mix(i * 1315423911u + k) % 26));
            }
        }
        t.cols[4].str.push_back(std::move(fat));

        t.cols[5].def.push_back(1);
        t.cols[5].i64.push_back(0);
    }
    return t;
}

static std::string write_table(const table& t, size_t batch_rows, writer_options opt,
                               const std::map<std::string, std::string>& meta = {}) {
    std::string image;
    lance_file_writer w(t.specs, opt, [&](std::string_view s) { image.append(s); });
    for (size_t at = 0; at < t.rows; at += batch_rows) {
        const size_t n = std::min(batch_rows, t.rows - at);
        std::vector<column_values> batch(t.specs.size());
        for (size_t c = 0; c < t.specs.size(); ++c) {
            const auto& src = t.cols[c];
            auto& dst = batch[c];
            if (!src.def.empty()) {
                dst.def.assign(src.def.begin() + at, src.def.begin() + at + n);
            }
            auto cp = [&](const auto& s, auto& d) {
                if (!s.empty()) { d.assign(s.begin() + at, s.begin() + at + n); }
            };
            cp(src.i32, dst.i32);
            cp(src.i64, dst.i64);
            cp(src.f64, dst.f64);
            cp(src.str, dst.str);
        }
        w.add_batch(std::move(batch));
    }
    w.finish(meta);
    return image;
}

static std::span<const uint8_t> bytes(const std::string& s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

static void check_range(const table& t, const lance_file_view& v, size_t c, uint64_t lo, uint64_t hi) {
    auto got = v.read_rows(c, t.specs[c].type, lo, hi);
    CHECK(got.rows() == hi - lo);
    const auto& want = t.cols[c];
    for (uint64_t i = lo; i < hi; ++i) {
        const size_t k = size_t(i - lo);
        const bool want_null = !want.def.empty() && want.def[size_t(i)];
        const bool got_null = !got.def.empty() && got.def.at(k);
        if (want_null != got_null) {
            std::fprintf(stderr, "  col %zu row %llu: null mismatch\n", c, (unsigned long long)i);
            ++failures;
            return;
        }
        if (want_null) { continue; }
        bool same = true;
        switch (t.specs[c].type) {
        case lphys::i32: same = got.i32.at(k) == want.i32[size_t(i)]; break;
        case lphys::i64: same = got.i64.at(k) == want.i64[size_t(i)]; break;
        case lphys::f64: same = got.f64.at(k) == want.f64[size_t(i)]; break;
        case lphys::bytes: same = got.str.at(k) == want.str[size_t(i)]; break;
        }
        if (!same) {
            std::fprintf(stderr, "  col %zu row %llu: value mismatch\n", c, (unsigned long long)i);
            ++failures;
            return;
        }
    }
}

static int cmd_roundtrip(const char*) {
    const size_t rows = 20'000;
    auto t = make_table(rows);
    // Small page target so every column spills into several pages, exercising
    // page_for_row and cross-page reads; zstd on to exercise the chunk codec.
    writer_options opt;
    opt.page_target_bytes = 64u << 10;
    opt.enc.zstd_level = 3;
    auto image = write_table(t, 3'000, opt, {{"scylla.folding_level", "L1"}});

    lance_file_view v(bytes(image));
    CHECK(v.num_rows() == rows);
    CHECK(v.num_columns() == t.specs.size());
    CHECK(v.schema().fields.size() == t.specs.size());
    CHECK(v.schema().schema_metadata.at("scylla.folding_level") == "L1");
    CHECK(v.schema().fields[0].name == "pk");
    CHECK(!v.schema().fields[0].nullable);
    // Multi-page: the fat column at ~300 B/row and a 64 KiB target must cut.
    CHECK(v.column(4).pages.size() > 3);

    std::mt19937_64 rng(3);
    for (size_t c = 0; c < t.specs.size(); ++c) {
        check_range(t, v, c, 0, rows);                       // full scan
        for (int k = 0; k < 200; ++k) {                      // random points
            uint64_t r = rng() % rows;
            check_range(t, v, c, r, r + 1);
        }
        for (int k = 0; k < 50; ++k) {                       // random slices
            uint64_t a = rng() % rows;
            uint64_t b = std::min<uint64_t>(rows, a + 1 + rng() % 700);
            check_range(t, v, c, a, b);
        }
    }

    // Plain (no zstd) variant must round-trip too.
    writer_options plain;
    plain.page_target_bytes = 64u << 10;
    auto image2 = write_table(t, 7'000, plain);
    lance_file_view v2(bytes(image2));
    for (size_t c = 0; c < t.specs.size(); ++c) {
        check_range(t, v2, c, 0, rows);
    }
    if (!failures) { std::puts("test_file roundtrip: all ok"); }
    return failures ? 1 : 0;
}

static int cmd_emit(const char* outdir) {
    const size_t rows = 20'000;
    auto t = make_table(rows);
    struct variant {
        const char* name;
        int zstd;
    } variants[] = {{"ours_plain.lance", 0}, {"ours_zstd.lance", 3}};
    for (auto& va : variants) {
        writer_options opt;
        opt.page_target_bytes = 256u << 10;
        opt.enc.zstd_level = va.zstd;
        auto image = write_table(t, 4'096, opt, {{"scylla.folding_level", "L1"}});
        std::ofstream f(std::string(outdir) + "/" + va.name, std::ios::binary);
        f.write(image.data(), std::streamsize(image.size()));
        if (!f) {
            std::fprintf(stderr, "cannot write %s/%s\n", outdir, va.name);
            return 1;
        }
    }
    std::printf("emitted 2 files, %zu rows\n", rows);
    return 0;
}

static int cmd_dump(const char* path) {
    std::ifstream f(path, std::ios::binary);
    std::string image((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    if (image.empty()) {
        std::fprintf(stderr, "cannot read %s\n", path);
        return 1;
    }
    lance_file_view v(bytes(image));
    const auto& fields = v.schema().fields;
    for (size_t c = 0; c < v.num_columns(); ++c) {
        lphys t;
        const auto& lt = fields.at(c).logical_type;
        if (lt == "int32" || lt == "uint32") { t = lphys::i32; }
        else if (lt == "int64" || lt == "uint64" || lt.starts_with("timestamp")) { t = lphys::i64; }
        else if (lt == "double") { t = lphys::f64; }
        else if (lt == "string" || lt == "binary" || lt == "large_string" || lt == "large_binary") { t = lphys::bytes; }
        else {
            std::fprintf(stderr, "unsupported logical type %s\n", lt.c_str());
            return 1;
        }
        auto vals = v.read_rows(c, t, 0, v.num_rows());
        for (uint64_t i = 0; i < v.num_rows(); ++i) {
            std::printf("%zu\t%llu\t", c, (unsigned long long)i);
            if (!vals.def.empty() && vals.def[size_t(i)]) {
                std::printf("NULL\n");
                continue;
            }
            switch (t) {
            case lphys::i32: std::printf("%d\n", vals.i32[size_t(i)]); break;
            case lphys::i64: std::printf("%lld\n", (long long)vals.i64[size_t(i)]); break;
            case lphys::f64: std::printf("%.17g\n", vals.f64[size_t(i)]); break;
            case lphys::bytes: {
                const auto& s = vals.str[size_t(i)];
                for (unsigned char ch : s) { std::printf("%02x", ch); }
                std::printf("\n");
                break;
            }
            }
        }
    }
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "usage: %s roundtrip|emit|dump <path>\n", argv[0]);
        return 2;
    }
    if (!std::strcmp(argv[1], "roundtrip")) { return cmd_roundtrip(argv[2]); }
    if (!std::strcmp(argv[1], "emit")) { return cmd_emit(argv[2]); }
    if (!std::strcmp(argv[1], "dump")) { return cmd_dump(argv[2]); }
    std::fprintf(stderr, "unknown subcommand %s\n", argv[1]);
    return 2;
}
