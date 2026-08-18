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
#include "sstables/parquet/tiering_context.hh"

#include "mutation/mutation.hh"
#include "schema/schema_builder.hh"
#include "exceptions/exceptions.hh"

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

    std::vector<std::string> names;
    for (size_t i = 1; i < md.schema.size(); ++i) {
        if (md.schema[i].is_leaf()) { names.push_back(md.schema[i].name); }
    }

    // The exact set, not just the count: a count catches a leaf appearing or
    // vanishing but not one being renamed or swapped, and it goes stale silently
    // when a feature adds a leaf -- which is how this assertion came to expect 9
    // after row markers introduced __rm.
    //
    // pk and ck, the four regular columns, the folded __ts, the two sparse
    // exception leaves that the divergent v_txt timestamps force into existence,
    // and __rm for the row markers every row here carries.
    const std::vector<std::string> want_leaves = {
        "pk", "ck", "v_dbl", "v_int", "v_long", "v_txt",
        "__ts", "__tsx_mask", "__tsx_vals", "__rm",
    };
    auto sorted_names = names;
    std::sort(sorted_names.begin(), sorted_names.end());
    auto sorted_want = want_leaves;
    std::sort(sorted_want.begin(), sorted_want.end());
    BOOST_REQUIRE_EQUAL(fmt::format("{}", fmt::join(sorted_names, ",")),
                        fmt::format("{}", fmt::join(sorted_want, ",")));
    BOOST_REQUIRE_EQUAL(md.leaf_count(), want_leaves.size());
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

// ---------------------------------------------------------------- tiering
// The policy itself is exhaustively tested standalone; what needs a real schema
// is the eligibility gate and the storage_format check in front of it.

// Encoding hints must survive the trip to the writer, and a `timestamp` column
// must be annotated in the unit it is actually written in.
//
// Both of these were broken and both were invisible to a round-trip suite, which is
// why they get an explicit test rather than being left to the size numbers:
//
//  - `write_rows()` builds the writer from the schema *tree*, while the per-leaf
//    encoding hints live in `mapped_schema::columns`. They were simply not passed,
//    so every column was written PLAIN -- including monotonic clustering keys the
//    mapping had explicitly asked to be DELTA_BINARY_PACKED. Nothing noticed,
//    because an encoding is self-describing: the reader honours whatever the page
//    header says, so the file still round-tripped, just much larger. On a
//    time-series table the clustering key alone was 912 882 bytes instead of
//    37 445.
//  - a CQL `timestamp` *column value* is milliseconds since epoch, while a cell's
//    *write* timestamp is microseconds. The mapping annotated columns MICROS while
//    writing millisecond values, so every external reader saw dates in 1970. Our
//    own reader inverts the mapping from `cql_type` and never reads the annotation,
//    so only a foreign decoder could catch it.
SEASTAR_THREAD_TEST_CASE(test_parquet_key_encoding_and_timestamp_unit) {
    auto s = schema_builder(1u, "pqks", "pqts")
        .with_column("pk", utf8_type, ::column_kind::partition_key)
        .with_column("ck", timestamp_type, ::column_kind::clustering_key)
        .with_column("v", int32_type)
        .build();

    pq::fragment_shredder shredder(*s);
    const auto pk = partition_key::from_single_value(*s, utf8_type->decompose(sstring("s1")));
    shredder.new_partition(dht::decorate_key(*s, pk));

    // A regular hourly stride, which is what delta encoding is for, and a value
    // whose wall-clock reading is unambiguous: 2023-01-01T00:00:00Z.
    constexpr int64_t base_ms = 1672531200000;
    constexpr int N = 2000;
    for (int i = 0; i < N; ++i) {
        auto ck = clustering_key::from_single_value(
                *s, timestamp_type->decompose(db_clock::time_point(
                        db_clock::duration(base_ms + int64_t(i) * 3600'000))));
        ::row cells;
        const column_definition& cdef = *s->get_column_definition(to_bytes("v"));
        cells.apply(cdef, atomic_cell::make_live(*cdef.type, 1700000000000000,
                                                 int32_type->decompose(int32_t(i))));
        clustering_row cr(std::move(ck), row_tombstone{}, row_marker(1700000000000000),
                          std::move(cells));
        shredder.add_clustering_row(cr);
    }

    pq::pq_writer_config cfg;
    auto img = shredder.to_parquet(cfg);
    auto md = pq::format::parse_footer(img);
    BOOST_REQUIRE_EQUAL(md.num_rows, int64_t(N));
    BOOST_REQUIRE_EQUAL(md.row_groups.size(), 1u);

    // The clustering key must actually be delta-encoded. Asserting the *encoding*
    // and not just the size is the point: a size assertion would drift with any
    // unrelated change, and this is the thing that was silently lost.
    bool saw_ck = false;
    for (const auto& cc : md.row_groups[0].columns) {
        if (!cc.meta || cc.meta->path_in_schema.empty()) { continue; }
        if (cc.meta->path_in_schema.back() != "ck") { continue; }
        saw_ck = true;
        const auto& enc = cc.meta->encodings;
        BOOST_REQUIRE_MESSAGE(
                std::find(enc.begin(), enc.end(),
                          pq::format::encoding::delta_binary_packed) != enc.end(),
                "clustering key was not DELTA_BINARY_PACKED -- the encoding hint was dropped");
    }
    BOOST_REQUIRE(saw_ck);

    // And the annotation must say MILLIS, matching the values written.
    bool saw_ts_col = false;
    for (size_t i = 1; i < md.schema.size(); ++i) {
        if (md.schema[i].name != "ck") { continue; }
        saw_ts_col = true;
        BOOST_REQUIRE(md.schema[i].converted_type.has_value());
        BOOST_REQUIRE_EQUAL(*md.schema[i].converted_type,
                            int32_t(pq::format::converted::timestamp_millis));
    }
    BOOST_REQUIRE(saw_ts_col);
}

// The write-side memory budget's accounting (R-13, design doc 5.5a).
//
// The writer currently buffers every row of an sstable before encoding anything, which
// costs about 1.8 kB per row -- 17 GiB at ten million rows. Cutting row groups needs a
// number to cut on, and that number has to err *high*: under-counting is what OOMs a
// shard, over-counting only cuts a row group slightly early.
//
// Calibrated against measured RSS on a 10-column time-series table: the estimator says
// 1 887 B/row where the real slope is 1 814 B/row, i.e. 4 % conservative. This test does
// not re-derive that -- RSS is not available here -- it pins the properties the budget
// relies on, so the accounting cannot silently stop tracking the buffer.
SEASTAR_THREAD_TEST_CASE(test_parquet_buffered_bytes_accounting) {
    auto s = make_test_schema();
    pq::fragment_shredder shredder(*s);
    BOOST_REQUIRE_EQUAL(shredder.buffered_bytes(), 0u);

    const auto pk = partition_key::from_single_value(*s, long_type->decompose(int64_t(1)));
    shredder.new_partition(dht::decorate_key(*s, pk));

    auto add = [&] (int i) {
        auto ck = clustering_key::from_single_value(*s, long_type->decompose(int64_t(i)));
        ::row cells;
        for (const char* nm : {"v_int", "v_long", "v_dbl", "v_txt"}) {
            const column_definition& cdef = *s->get_column_definition(to_bytes(nm));
            bytes val = cdef.type == utf8_type
                      ? utf8_type->decompose(sstring(format("value-{}", i)))
                      : (cdef.type == int32_type ? int32_type->decompose(int32_t(i))
                      : (cdef.type == double_type ? double_type->decompose(double(i))
                                                  : long_type->decompose(int64_t(i))));
            cells.apply(cdef, atomic_cell::make_live(*cdef.type, 1700000000000000 + i,
                                                     std::move(val)));
        }
        clustering_row cr(std::move(ck), row_tombstone{}, row_marker(1700000000000000 + i),
                          std::move(cells));
        shredder.add_clustering_row(cr);
    };

    add(0);
    const size_t one = shredder.buffered_bytes();
    BOOST_REQUIRE_GT(one, sizeof(pq::row));       // it counts the heap, not just the struct

    for (int i = 1; i < 100; ++i) { add(i); }
    const size_t hundred = shredder.buffered_bytes();

    // Monotonic, and roughly proportional -- the rows are alike, so 100 of them should
    // cost far more than one and not wildly more than 100x.
    BOOST_REQUIRE_GT(hundred, one * 50);
    BOOST_REQUIRE_LT(hundred, one * 200);

    // Errs high: a row of six columns cannot really occupy less than its own struct plus
    // a cell per column, and must not be estimated at some implausible size either.
    const size_t per_row = hundred / 100;
    BOOST_REQUIRE_GT(per_row, sizeof(pq::row) + 4 * sizeof(pq::cell));
    BOOST_REQUIRE_LT(per_row, 8u * 1024u);

    // Every row path must feed the accounting, not just clustering rows: a range
    // tombstone change is buffered as a row too.
    const size_t before_rtc = shredder.buffered_bytes();
    auto bound = clustering_key_prefix::from_single_value(*s, long_type->decompose(int64_t(500)));
    shredder.add_range_tombstone_change(range_tombstone_change(
            position_in_partition::before_key(bound),
            tombstone(1700000000000001, gc_clock::time_point(gc_clock::duration(7)))));
    BOOST_REQUIRE_GT(shredder.buffered_bytes(), before_rtc);

    shredder.clear();
    BOOST_REQUIRE_EQUAL(shredder.buffered_bytes(), 0u);
}

// The `parquet = {...}` table property: parsing, validation, and round-tripping.
//
// Validation is the point. A storage-format option that silently ignores what it cannot
// honour is worse than one that refuses: a user who writes `compression: 'gzip'` and
// gets zstd has been told something untrue about their data. So every case below that
// *should* fail is asserted to fail, not just the ones that should succeed.
SEASTAR_THREAD_TEST_CASE(test_parquet_parameters) {
    using pp = pq::parquet_parameters;

    // Defaults when nothing is given.
    {
        pp p{};
        BOOST_REQUIRE(p.to_map().empty());          // DESCRIBE stays terse
        const pq::pq_writer_config def;
        BOOST_REQUIRE_EQUAL(p.config().row_group_rows, def.row_group_rows);
    }

    // Accepted values reach the config.
    {
        // 7500 rather than the default, so the round-trip below actually proves
        // something: to_map() records only what differs from the default, so a test
        // that sets the default value would assert against an absent key. It used to
        // set 5 000, which stopped being a non-default when 10.4c's sweep moved the
        // default there.
        pp p{{{pp::ROW_GROUP_ROWS, "7500"},
              {pp::ROW_GROUP_BUFFER_BYTES, "32MiB"},
              {pp::PAGE_ROWS, "4096"},
              {pp::COMPRESSION, "none"},
              {pp::METADATA_FOLDING, "verbatim"}}};
        BOOST_REQUIRE_EQUAL(p.config().row_group_rows, 7500u);
        BOOST_REQUIRE_EQUAL(p.config().row_group_buffer_bytes, 32u * 1024 * 1024);
        BOOST_REQUIRE_EQUAL(p.config().wopt.page_values, 4096u);
        BOOST_REQUIRE(p.config().wopt.compression == pq::format::codec::uncompressed);
        BOOST_REQUIRE(p.config().level == pq::folding_level::verbatim);
        // Numeric dictionaries are off by default and reachable via 'all'.
        BOOST_REQUIRE(!p.config().wopt.numeric_dictionary);
        pp all{{{pp::DICTIONARY, "all"}}};
        BOOST_REQUIRE(all.config().wopt.numeric_dictionary);
        BOOST_REQUIRE_EQUAL(all.to_map().at(pp::DICTIONARY), "all");
        pp none{{{pp::DICTIONARY, "none"}}};
        BOOST_REQUIRE(!none.config().wopt.use_dictionary);
        BOOST_REQUIRE_EQUAL(none.to_map().at(pp::DICTIONARY), "none");

        // Round-trips through the map form, which is how it is persisted.
        auto m = p.to_map();
        BOOST_REQUIRE_EQUAL(m[pp::ROW_GROUP_ROWS], "7500");
        pp again{m};
        BOOST_REQUIRE_EQUAL(again.config().row_group_rows, 7500u);
        BOOST_REQUIRE_EQUAL(again.config().wopt.page_values, 4096u);
        BOOST_REQUIRE(again.config().level == pq::folding_level::verbatim);
    }

    auto rejects = [] (std::map<sstring, sstring> opts) {
        BOOST_CHECK_THROW(pp{opts}, exceptions::configuration_exception);
    };

    rejects({{"row_groop_rows", "5000"}});                  // typo, not silently ignored
    rejects({{pp::ROW_GROUP_ROWS, "not-a-number"}});
    rejects({{pp::ROW_GROUP_ROWS, "5000rows"}});            // trailing junk
    rejects({{pp::ROW_GROUP_ROWS, "0"}});
    // Below the floor the fixed per-row-group metadata dominates: at 100 rows on a
    // 20-leaf table it is 45 B/row against a 5.2 B/row total (design doc 10.4c).
    rejects({{pp::ROW_GROUP_ROWS, "100"}});
    rejects({{pp::ROW_GROUP_BUFFER_BYTES, "16"}});          // under the 1 MiB floor
    rejects({{pp::ROW_GROUP_BUFFER_BYTES, "8GiB"}});        // over the 1 GiB ceiling
    rejects({{pp::COMPRESSION, "gzip"}});                   // plausible, unsupported
    rejects({{pp::COMPRESSION, "lz4"}});
    rejects({{pp::COMPRESSION_LEVEL, "99"}});
    // L3 discards write times and TTLs: it is export-only and must not be reachable
    // as a storage setting.
    rejects({{pp::DICTIONARY, "yes"}});
    rejects({{pp::METADATA_FOLDING, "logical"}});
    rejects({{pp::METADATA_FOLDING, "yes"}});
}

SEASTAR_THREAD_TEST_CASE(test_parquet_schema_eligibility) {
    BOOST_REQUIRE(pq::schema_is_parquet_eligible(*make_test_schema()));

    // Non-frozen collections became a repeated group and counters one element per
    // shard, so both are eligible now. They are still worth asserting: these two
    // were the gate's only rejections, and a regression in either encoding should
    // show up as a refusal here rather than as mangled data.
    auto with_set = schema_builder(1u, "pqks", "withset")
        .with_column("pk", long_type, ::column_kind::partition_key)
        .with_column("s", set_type_impl::get_instance(utf8_type, true))
        .build();
    BOOST_REQUIRE(pq::schema_is_parquet_eligible(*with_set));

    // A frozen collection is an opaque blob and is fine.
    auto with_frozen = schema_builder(1u, "pqks", "withfrozen")
        .with_column("pk", long_type, ::column_kind::partition_key)
        .with_column("s", set_type_impl::get_instance(utf8_type, false))
        .build();
    BOOST_REQUIRE(pq::schema_is_parquet_eligible(*with_frozen));

    auto with_counter = schema_builder(1u, "pqks", "withcounter")
        .with_column("pk", long_type, ::column_kind::partition_key)
        .with_column("c", counter_type)
        .build();
    BOOST_REQUIRE(pq::schema_is_parquet_eligible(*with_counter));

    // pk + ck + 4 regular. Exactly the schema's own count -- the Parquet leaf count is
    // data-dependent and deliberately not what C5 bounds (see tiering_policy.hh).
    BOOST_REQUIRE_EQUAL(pq::column_count(*make_test_schema()), 6u);
}

SEASTAR_THREAD_TEST_CASE(test_parquet_storage_format_gates_conversion) {
    pq::compaction_context ctx;
    ctx.bottom_tier = true;
    ctx.predicted_gain = 0.9;                  // everything else says yes

    // Default table: never converted, however good the numbers look.
    auto plain = make_test_schema();
    auto d = pq::decide_output_format({}, *plain, ctx);
    BOOST_REQUIRE(!d.parquet());
    BOOST_REQUIRE(d.reason.find("storage_format") != std::string::npos);

    // Opted into hybrid, and every criterion satisfied: converts.
    auto opted = schema_builder(schema_ptr(make_test_schema()))
        .set_storage_format(storage_format_type::hybrid)
        .build();
    auto d2 = pq::decide_output_format({}, *opted, ctx);
    BOOST_REQUIRE(d2.parquet());

    // Fail closed. This used to be asserted via the minimum-size criterion, which no longer
    // exists -- C6 subsumed it (see tiering_policy.hh). What guards an unmeasurable candidate
    // now is C6 itself: no gain means no conversion, which matters because the estimator
    // returns nullopt whenever it cannot sample, and "could not measure" must never read as
    // "go ahead".
    auto unmeasured = ctx;
    unmeasured.predicted_gain.reset();
    auto d3 = pq::decide_output_format({}, *opted, unmeasured);
    BOOST_REQUIRE(!d3.parquet());
    BOOST_REQUIRE(d3.reason.find("no measured gain") != std::string::npos);

    // And the width bound, which is the only thing standing in for C7.
    auto wide = ctx;
    auto d4 = pq::evaluate_tiering([&] {
        auto in = pq::make_tiering_inputs({}, *opted, wide);
        in.column_count = 500;
        return in;
    }());
    BOOST_REQUIRE(!d4.parquet());
    BOOST_REQUIRE(d4.reason.find("columns") != std::string::npos);
}
