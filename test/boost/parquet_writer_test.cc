/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Drives the Parquet shredder with a real Scylla schema and real
// mutation_fragment types, then reads the resulting file back with our own
// footer parser.
//
// The standalone suite in sstables/parquet/ already proves the folding logic on
// a local row/cell model. What this test adds is the part that model cannot
// reach: that Scylla's own clustering_row, atomic_cell, decorated_key and
// column_definition are translated correctly.

#include <algorithm>
#include <map>
#include <boost/test/unit_test.hpp>

#include "test/lib/scylla_test_case.hh"
#include "test/lib/simple_schema.hh"

#include "sstables/parquet/writer_impl.hh"
#include "sstables/parquet/format/parquet_metadata.hh"

#include "mutation/mutation.hh"
#include "schema/schema_builder.hh"

namespace pq = sstables::parquet;

namespace {

schema_ptr make_test_schema() {
    return schema_builder(1u, "pqks", "pqcf")
        .with_column("pk", long_type, ::column_kind::partition_key)
        .with_column("ck", long_type, ::column_kind::clustering_key)
        .with_column("v_int", int32_type)
        .with_column("v_long", long_type)
        .with_column("v_dbl", double_type)
        .with_column("v_txt", utf8_type)
        .build();
}

} // namespace

// The type mapping must classify every column and keep them in
// partition / clustering / regular order, which is the order the shredder and
// the reader both rely on.
SEASTAR_THREAD_TEST_CASE(test_parquet_columns_of_schema) {
    auto s = make_test_schema();
    auto cols = pq::columns_of(*s);

    BOOST_REQUIRE_EQUAL(cols.size(), 6u);
    BOOST_REQUIRE_EQUAL(cols[0].name, "pk");
    BOOST_REQUIRE(cols[0].kind == pq::column_kind::partition_key);
    BOOST_REQUIRE(cols[1].kind == pq::column_kind::clustering_key);
    for (size_t i = 2; i < cols.size(); ++i) {
        BOOST_REQUIRE(cols[i].kind == pq::column_kind::regular);
    }
    // Regular columns come back in schema order, which is sorted by name and is
    // also the column_id order the shredder indexes cells by -- not declaration
    // order. Assert by name so the test states the real contract.
    std::map<std::string, pq::cql_type> by_name;
    for (const auto& c : cols) { by_name.emplace(c.name, c.type); }
    BOOST_REQUIRE(by_name.at("v_int")  == pq::cql_type::int32);
    BOOST_REQUIRE(by_name.at("v_long") == pq::cql_type::bigint);
    BOOST_REQUIRE(by_name.at("v_dbl")  == pq::cql_type::dbl);
    BOOST_REQUIRE(by_name.at("v_txt")  == pq::cql_type::text);

    // The regular block must be in ascending name order, because column_id
    // indexes it and the reader will rely on that to invert the mapping.
    std::vector<std::string> reg;
    for (size_t i = 2; i < cols.size(); ++i) { reg.push_back(cols[i].name); }
    BOOST_REQUIRE(std::is_sorted(reg.begin(), reg.end()));
}

// End to end: real fragments in, valid Parquet out.
SEASTAR_THREAD_TEST_CASE(test_parquet_shred_real_fragments) {
    auto s = make_test_schema();
    pq::fragment_shredder shredder(*s);

    constexpr int N = 500;
    const auto pk = partition_key::from_single_value(*s, long_type->decompose(int64_t(42)));
    shredder.new_partition(dht::decorate_key(*s, pk));

    for (int i = 0; i < N; ++i) {
        auto ck = clustering_key::from_single_value(*s, long_type->decompose(int64_t(i)));
        ::row cells;
        auto put = [&] (const char* name, bytes val, api::timestamp_type ts) {
            const column_definition& cdef = *s->get_column_definition(to_bytes(name));
            cells.apply(cdef, atomic_cell::make_live(*cdef.type, ts, std::move(val)));
        };
        // Every third row gets a divergent timestamp on one column, which is
        // what exercises the sparse exception channel.
        const api::timestamp_type base = 1700000000000000 + i;
        put("v_int",  int32_type->decompose(int32_t(i % 97)), base);
        put("v_long", long_type->decompose(int64_t(i) * 7), base);
        put("v_dbl",  double_type->decompose(double(i) / 4.0), base);
        put("v_txt",  utf8_type->decompose(sstring(i % 5 == 0 ? "alpha" : "beta")),
            (i % 3 == 0) ? base + 500 : base);

        clustering_row cr(std::move(ck), row_tombstone{}, row_marker(base), std::move(cells));
        shredder.add_clustering_row(cr);
    }

    BOOST_REQUIRE_EQUAL(shredder.size(), size_t(N));

    pq::pq_writer_config cfg;
    auto img = shredder.to_parquet(cfg);
    BOOST_REQUIRE_GT(img.size(), 0u);

    // Parse it back with our own reader and check the shape.
    auto md = pq::format::parse_footer(img);
    BOOST_REQUIRE_EQUAL(md.num_rows, int64_t(N));
    BOOST_REQUIRE_EQUAL(md.row_groups.size(), 1u);

    // pk, ck, four regular columns, the folded __ts, and the two sparse
    // exception leaves that the divergent v_txt timestamps force into existence.
    BOOST_REQUIRE_EQUAL(md.leaf_count(), 9u);

    std::vector<std::string> names;
    for (size_t i = 1; i < md.schema.size(); ++i) {
        if (md.schema[i].is_leaf()) { names.push_back(md.schema[i].name); }
    }
    BOOST_REQUIRE_EQUAL(names[0], "pk");
    BOOST_REQUIRE_EQUAL(names[1], "ck");
    BOOST_REQUIRE(std::find(names.begin(), names.end(), "__ts") != names.end());
    BOOST_REQUIRE(std::find(names.begin(), names.end(), "__tsx_mask") != names.end());
    BOOST_REQUIRE(std::find(names.begin(), names.end(), "__tsx_vals") != names.end());

    // The folding level travels in the file's key/value metadata so a reader can
    // tell how to invert the mapping.
    const std::string* lvl = md.kv("scylla.folding_level");
    BOOST_REQUIRE(lvl != nullptr);
    BOOST_REQUIRE_EQUAL(*lvl, "L1");
}

// Without divergence the sparse channel must not be materialised at all --
// that is the whole point of deciding the schema from the data.
SEASTAR_THREAD_TEST_CASE(test_parquet_no_exception_leaves_when_uniform) {
    auto s = make_test_schema();
    pq::fragment_shredder shredder(*s);
    const auto pk = partition_key::from_single_value(*s, long_type->decompose(int64_t(1)));
    shredder.new_partition(dht::decorate_key(*s, pk));

    for (int i = 0; i < 100; ++i) {
        auto ck = clustering_key::from_single_value(*s, long_type->decompose(int64_t(i)));
        ::row cells;
        const api::timestamp_type ts = 1700000000000000;   // identical everywhere
        const column_definition& cdef = *s->get_column_definition(to_bytes("v_long"));
        cells.apply(cdef, atomic_cell::make_live(*cdef.type, ts, long_type->decompose(int64_t(i))));
        clustering_row cr(std::move(ck), row_tombstone{}, row_marker(ts), std::move(cells));
        shredder.add_clustering_row(cr);
    }

    auto img = shredder.to_parquet(pq::pq_writer_config{});
    auto md = pq::format::parse_footer(img);
    for (size_t i = 1; i < md.schema.size(); ++i) {
        BOOST_REQUIRE(md.schema[i].name != "__tsx_mask");
        BOOST_REQUIRE(md.schema[i].name != "__tsx_vals");
    }
}
