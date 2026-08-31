/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Round-trip and random-access tests for the Lance 2.1 structural encodings.
// Standalone (plain g++, -lzstd); interop with the official reader is suite 4
// of run_tests.sh, not this file.

#include "lance_encodings.hh"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <random>

using namespace sstables::lance::format;

static int failures = 0;

#define CHECK(cond) do { \
    if (!(cond)) { \
        std::fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        ++failures; \
    } \
} while (0)

#define CHECK_THROWS(expr) do { \
    bool threw = false; \
    try { (void)(expr); } catch (const std::exception&) { threw = true; } \
    if (!threw) { \
        std::fprintf(stderr, "FAIL %s:%d: expected throw: %s\n", __FILE__, __LINE__, #expr); \
        ++failures; \
    } \
} while (0)

// Decodes a whole miniblock page from its encoded form.
static column_values decode_all(lphys t, const encoded_page& pg) {
    auto pl = parse_page_layout(wrap_page_layout_any(pg.layout));
    switch (pl.k) {
    case page_layout::kind::miniblock: {
        auto idx = parse_miniblock_index(pg.buffers.at(0), pl.num_items);
        return decode_miniblock_chunks(t, pl, idx, 0, idx.chunks.size(), pg.buffers.at(1));
    }
    case page_layout::kind::fullzip: {
        if (pl.val.k == chan_enc::kind::variable) {
            return decode_fullzip_variable(pl, 0, pg.rows, pg.buffers.at(0));
        }
        return decode_fullzip_fixed(t, pl, 0, pg.rows, pg.buffers.at(0));
    }
    case page_layout::kind::constant:
        return decode_constant(t, pl, 0, pg.rows);
    }
    throw lance_error("unreachable");
}

static void expect_i64(const column_values& got, const std::vector<int64_t>& vals,
                       const std::vector<uint8_t>& def) {
    CHECK(got.i64.size() == vals.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        const bool null = !def.empty() && def[i];
        if (!null && got.i64.at(i) != vals[i]) {
            std::fprintf(stderr, "  i64[%zu]: %lld != %lld\n", i,
                         (long long)got.i64[i], (long long)vals[i]);
            ++failures;
            return;
        }
    }
    if (def.empty()) {
        CHECK(got.def.empty() || std::all_of(got.def.begin(), got.def.end(),
                                             [](uint8_t d) { return d == 0; }));
    } else {
        CHECK(got.def == def);
    }
}

static void test_miniblock_i64_required() {
    column_values v;
    for (int64_t i = 0; i < 10'000; ++i) { v.i64.push_back(i * 977); }
    encode_options opt;
    auto pg = encode_page(lphys::i64, v, opt);
    CHECK(pg.buffers.size() == 2);
    CHECK(pg.rows == 10'000);
    // 8 KiB target and 8-byte values: expect 512-value chunks, so > 15 chunks.
    auto pl = parse_page_layout(wrap_page_layout_any(pg.layout));
    auto idx = parse_miniblock_index(pg.buffers[0], pl.num_items);
    CHECK(idx.chunks.size() >= 10);
    // Non-final chunks must be powers of two.
    for (size_t i = 0; i + 1 < idx.chunks.size(); ++i) {
        CHECK(std::popcount(idx.chunks[i].values) == 1);
    }
    expect_i64(decode_all(lphys::i64, pg), v.i64, {});
}

static void test_miniblock_i64_nullable() {
    column_values v;
    std::mt19937_64 rng(42);
    for (int64_t i = 0; i < 5'000; ++i) {
        bool null = rng() % 5 == 0;
        v.def.push_back(null ? 1 : 0);
        v.i64.push_back(null ? 0 : int64_t(rng()));
    }
    encode_options opt;
    opt.nullable = true;
    auto pg = encode_page(lphys::i64, v, opt);
    expect_i64(decode_all(lphys::i64, pg), v.i64, v.def);

    // Random access: decode just the chunk holding value 3210.
    auto pl = parse_page_layout(wrap_page_layout_any(pg.layout));
    auto idx = parse_miniblock_index(pg.buffers[0], pl.num_items);
    size_t c = idx.chunk_for(3210);
    const auto& ch = idx.chunks[c];
    auto slice = std::string_view(pg.buffers[1]).substr(size_t(ch.byte_offset), ch.byte_size);
    auto got = decode_miniblock_chunks(lphys::i64, pl, idx, c, 1, slice);
    CHECK(got.i64.size() == ch.values);
    const size_t rel = size_t(3210 - ch.first_value);
    CHECK(got.def.at(rel) == v.def[3210]);
    if (!v.def[3210]) { CHECK(got.i64.at(rel) == v.i64[3210]); }
}

static void test_nullable_all_valid_drops_def() {
    column_values v;
    v.def.assign(100, 0);
    for (int64_t i = 0; i < 100; ++i) { v.i64.push_back(i); }
    encode_options opt;
    opt.nullable = true;
    auto pg = encode_page(lphys::i64, v, opt);
    auto pl = parse_page_layout(wrap_page_layout_any(pg.layout));
    CHECK(!pl.has_def);
    expect_i64(decode_all(lphys::i64, pg), v.i64, {});
}

static void test_miniblock_strings() {
    column_values v;
    std::mt19937_64 rng(7);
    for (int i = 0; i < 4'000; ++i) {
        bool null = rng() % 7 == 0;
        v.def.push_back(null ? 1 : 0);
        std::string s;
        if (!null) {
            size_t len = rng() % 40;
            for (size_t k = 0; k < len; ++k) { s.push_back(char('a' + rng() % 26)); }
        }
        v.str.push_back(std::move(s));
    }
    encode_options opt;
    opt.nullable = true;
    auto pg = encode_page(lphys::bytes, v, opt);
    auto pl = parse_page_layout(wrap_page_layout_any(pg.layout));
    CHECK(pl.k == page_layout::kind::miniblock);
    CHECK(pl.val.k == chan_enc::kind::variable);
    auto got = decode_all(lphys::bytes, pg);
    CHECK(got.str == v.str);
    CHECK(got.def == v.def);
}

static void test_miniblock_zstd() {
    column_values v;
    for (int i = 0; i < 3'000; ++i) {
        v.str.push_back("prefix-" + std::to_string(i % 50) + "-suffix");
    }
    encode_options opt;
    opt.zstd_level = 3;
    auto pg = encode_page(lphys::bytes, v, opt);
    auto pl = parse_page_layout(wrap_page_layout_any(pg.layout));
    CHECK(pl.val.general_scheme == 2);
    auto got = decode_all(lphys::bytes, pg);
    CHECK(got.str == v.str);
    // zstd must actually shrink this repetitive data.
    size_t plain = 0;
    for (const auto& s : v.str) { plain += s.size(); }
    CHECK(pg.buffers[1].size() < plain);
}

static void test_fullzip_strings() {
    column_values v;
    std::mt19937_64 rng(11);
    for (int i = 0; i < 600; ++i) {
        bool null = rng() % 6 == 0;
        v.def.push_back(null ? 1 : 0);
        std::string s;
        if (!null) {
            size_t len = 200 + rng() % 400;
            for (size_t k = 0; k < len; ++k) { s.push_back(char('A' + rng() % 26)); }
        }
        v.str.push_back(std::move(s));
    }
    encode_options opt;
    opt.nullable = true;
    auto pg = encode_page(lphys::bytes, v, opt);
    auto pl = parse_page_layout(wrap_page_layout_any(pg.layout));
    CHECK(pl.k == page_layout::kind::fullzip);
    CHECK(pl.val.k == chan_enc::kind::variable);
    CHECK(pg.buffers.size() == 2);
    auto got = decode_all(lphys::bytes, pg);
    CHECK(got.str == v.str);
    CHECK(got.def == v.def);

    // Random access through the rep index: rows [200, 203).
    const auto& rep = pg.buffers[1];
    uint32_t w = fullzip_rep_index_width(rep.size(), pg.rows);
    uint64_t a = read_rep_index_entry(rep, w, 200);
    uint64_t b = read_rep_index_entry(rep, w, 203);
    auto slice = std::string_view(pg.buffers[0]).substr(size_t(a), size_t(b - a));
    auto part = decode_fullzip_variable(pl, 200, 203, slice);
    CHECK(part.str.size() == 3);
    for (int i = 0; i < 3; ++i) {
        CHECK(part.str[i] == v.str[200 + i]);
        CHECK(part.def[i] == v.def[200 + i]);
    }
}

static void test_all_null_constant() {
    column_values v;
    v.def.assign(1'000, 1);
    v.i64.assign(1'000, 0);
    encode_options opt;
    opt.nullable = true;
    auto pg = encode_page(lphys::i64, v, opt);
    auto pl = parse_page_layout(wrap_page_layout_any(pg.layout));
    CHECK(pl.k == page_layout::kind::constant);
    CHECK(pg.buffers.empty());
    auto got = decode_constant(lphys::i64, pl, 100, 200);
    CHECK(got.def.size() == 100);
    CHECK(std::all_of(got.def.begin(), got.def.end(), [](uint8_t d) { return d == 1; }));
}

static void test_hostile() {
    column_values v;
    for (int64_t i = 0; i < 100; ++i) { v.i64.push_back(i); }
    auto pg = encode_page(lphys::i64, v, {});
    auto pl = parse_page_layout(wrap_page_layout_any(pg.layout));
    auto idx = parse_miniblock_index(pg.buffers[0], pl.num_items);
    // Truncated chunk bytes must throw, not crash.
    for (size_t cut = 0; cut < pg.buffers[1].size(); cut += 7) {
        try {
            (void)decode_miniblock_chunks(lphys::i64, pl, idx, 0, idx.chunks.size(),
                                          std::string_view(pg.buffers[1]).substr(0, cut));
        } catch (const std::exception&) {
        }
    }
    // Wrong physical type must be refused.
    CHECK_THROWS(decode_miniblock_chunks(lphys::i32, pl, idx, 0, idx.chunks.size(), pg.buffers[1]));
    // A rep-index buffer of impossible size must be refused.
    CHECK_THROWS(fullzip_rep_index_width(13, 5));
}

int main() {
    test_miniblock_i64_required();
    test_miniblock_i64_nullable();
    test_nullable_all_valid_drops_def();
    test_miniblock_strings();
    test_miniblock_zstd();
    test_fullzip_strings();
    test_all_null_constant();
    test_hostile();
    if (failures) {
        std::fprintf(stderr, "%d FAILURES\n", failures);
        return 1;
    }
    std::puts("test_encodings: all ok");
    return 0;
}
