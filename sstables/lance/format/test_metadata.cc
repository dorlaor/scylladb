/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Standalone round-trip tests for the Lance container metadata: footer,
// offset tables, ColumnMetadata, FileDescriptor. Compiled with plain g++ by
// run_tests.sh -- no Seastar, no Scylla headers.

#include "lance_metadata.hh"

#include <cassert>
#include <cstdio>
#include <span>

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

static std::span<const uint8_t> bytes(const std::string& s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

static void test_footer_roundtrip() {
    footer ft;
    ft.col_meta_start = 12345;
    ft.cmo_offset = 23456;
    ft.gbo_offset = 34567;
    ft.num_global_buffers = 2;
    ft.num_columns = 17;
    ft.major = version_major;
    ft.minor = version_minor;
    auto s = write_footer(ft);
    CHECK(s.size() == footer_size);
    auto back = parse_footer(bytes(s));
    CHECK(back.col_meta_start == ft.col_meta_start);
    CHECK(back.cmo_offset == ft.cmo_offset);
    CHECK(back.gbo_offset == ft.gbo_offset);
    CHECK(back.num_global_buffers == 2);
    CHECK(back.num_columns == 17);
    CHECK(back.major == version_major && back.minor == version_minor);

    // Bad magic and truncation are refused.
    auto broken = s;
    broken[39] = 'X';
    CHECK_THROWS(parse_footer(bytes(broken)));
    CHECK_THROWS(parse_footer(bytes(s).subspan(0, 39)));

    // Unknown versions are refused, both flavors of 2.0 are accepted.
    auto v = [&](uint16_t maj, uint16_t min) {
        footer f2 = ft;
        f2.major = maj;
        f2.minor = min;
        return write_footer(f2);
    };
    CHECK_THROWS(parse_footer(bytes(v(2, 2))));
    CHECK_THROWS(parse_footer(bytes(v(0, 1))));
    CHECK(parse_footer(bytes(v(0, 3))).minor == 3);
    CHECK(parse_footer(bytes(v(2, 0))).major == 2);
}

static void test_offset_table_roundtrip() {
    std::vector<buffer_ref> refs = {{0, 80}, {80, 800}, {880, 8}};
    auto s = write_offset_table(refs);
    CHECK(s.size() == 48);
    auto back = parse_offset_table(bytes(s), 3);
    CHECK(back.size() == 3);
    CHECK(back[1].offset == 80 && back[1].size == 800);
    CHECK_THROWS(parse_offset_table(bytes(s), 4));
}

static void test_column_meta_roundtrip() {
    column_meta cm;
    cm.encoding.none = true;
    page_info p1;
    p1.buffers = {{0, 4096}, {4096, 128}};
    p1.rows = 1000;
    p1.priority = 0;
    p1.encoding.any_bytes = "fake-any-payload";
    page_info p2;
    p2.buffers = {{8192, 65536}};
    p2.rows = 2500;
    p2.priority = 1000;
    p2.encoding.any_bytes = "other";
    cm.pages = {p1, p2};
    cm.buffers = {{99999, 55}};

    auto blob = write_column_meta(cm);
    auto back = parse_column_meta(blob);
    CHECK(back.encoding.none || back.encoding.any_bytes.empty());
    CHECK(back.pages.size() == 2);
    CHECK(back.pages[0].buffers.size() == 2);
    CHECK(back.pages[0].buffers[1].offset == 4096 && back.pages[0].buffers[1].size == 128);
    CHECK(back.pages[0].rows == 1000);
    CHECK(back.pages[0].priority == 0);
    CHECK(back.pages[0].encoding.any_bytes == "fake-any-payload");
    CHECK(back.pages[1].rows == 2500 && back.pages[1].priority == 1000);
    CHECK(back.buffers.size() == 1 && back.buffers[0].offset == 99999);

    // page_for_row: [0,1000) -> 0, [1000,3500) -> 1, else throws.
    CHECK(back.page_for_row(0) == 0);
    CHECK(back.page_for_row(999) == 0);
    CHECK(back.page_for_row(1000) == 1);
    CHECK(back.page_for_row(3499) == 1);
    CHECK_THROWS(back.page_for_row(3500));
}

static void test_file_descriptor_roundtrip() {
    file_descriptor fd;
    fd.num_rows = 424242;
    fd.schema_metadata["scylla.folding_level"] = "row";
    fd.schema_metadata["scylla.version"] = "1";
    field_info f0;
    f0.type = field_info::kind::leaf;
    f0.name = "pk";
    f0.id = 0;
    f0.parent_id = -1;
    f0.logical_type = "int64";
    f0.nullable = false;
    field_info f1;
    f1.type = field_info::kind::leaf;
    f1.name = "payload";
    f1.id = 1;
    f1.parent_id = -1;
    f1.logical_type = "binary";
    f1.nullable = true;
    f1.metadata["scylla.cql_type"] = "blob";
    fd.fields = {f0, f1};

    auto blob = write_file_descriptor(fd);
    auto back = parse_file_descriptor(blob);
    CHECK(back.num_rows == 424242);
    CHECK(back.fields.size() == 2);
    CHECK(back.fields[0].name == "pk");
    CHECK(back.fields[0].logical_type == "int64");
    CHECK(back.fields[0].parent_id == -1);
    CHECK(!back.fields[0].nullable);
    CHECK(back.fields[1].nullable);
    CHECK(back.fields[1].metadata.at("scylla.cql_type") == "blob");
    CHECK(back.schema_metadata.at("scylla.folding_level") == "row");
}

static void test_hostile_input() {
    // Random truncations of a valid ColumnMetadata must throw or parse, never
    // crash or hang.
    column_meta cm;
    page_info p;
    p.buffers = {{0, 10}};
    p.rows = 5;
    p.encoding.any_bytes = std::string(300, 'x');
    cm.pages = {p};
    auto blob = write_column_meta(cm);
    for (size_t cut = 0; cut < blob.size(); ++cut) {
        try {
            (void)parse_column_meta(blob.substr(0, cut));
        } catch (const std::exception&) {
            // fine
        }
    }
    // A varint of ten 0x80 bytes must not spin.
    std::string evil(64, char(0x80));
    CHECK_THROWS(parse_column_meta(evil));
}

int main() {
    test_footer_roundtrip();
    test_offset_table_roundtrip();
    test_column_meta_roundtrip();
    test_file_descriptor_roundtrip();
    test_hostile_input();
    if (failures) {
        std::fprintf(stderr, "%d FAILURES\n", failures);
        return 1;
    }
    std::puts("test_metadata: all ok");
    return 0;
}
