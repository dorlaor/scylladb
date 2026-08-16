/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/writer_impl.hh"

#include "mutation/mutation_fragment.hh"
#include "schema/schema.hh"
#include "types/types.hh"

#include <cstring>

namespace sstables::parquet {

namespace {

// Scylla serialises fixed-width scalars big-endian, so the physical mapping can
// read them straight out of the cell without going through deserialize().
// Anything not handled here keeps its serialised form and travels as an opaque
// BYTE_ARRAY, which is lossless but gives up type-specific encoding.
cql_type cql_type_of(const abstract_type& t) {
    if (&t == int32_type.get())                            { return cql_type::int32; }
    if (&t == long_type.get())                             { return cql_type::bigint; }
    if (&t == timestamp_type.get())                        { return cql_type::timestamp; }
    if (&t == double_type.get())                           { return cql_type::dbl; }
    if (&t == utf8_type.get() || &t == ascii_type.get())   { return cql_type::text; }
    return cql_type::blob;
}

int64_t be64(const bytes_view& b) {
    uint64_t v = 0;
    const size_t n = std::min<size_t>(8, b.size());
    for (size_t i = 0; i < n; ++i) { v = (v << 8) | uint8_t(b[i]); }
    return int64_t(v);
}
int32_t be32(const bytes_view& b) {
    uint32_t v = 0;
    const size_t n = std::min<size_t>(4, b.size());
    for (size_t i = 0; i < n; ++i) { v = (v << 8) | uint8_t(b[i]); }
    return int32_t(v);
}

value decode(cql_type t, bytes_view b) {
    switch (t) {
    case cql_type::int32:     return be32(b);
    case cql_type::bigint:
    case cql_type::timestamp: return be64(b);
    case cql_type::dbl: {
        uint64_t bits = uint64_t(be64(b));
        double d;
        std::memcpy(&d, &bits, 8);
        return d;
    }
    default:
        return std::string(reinterpret_cast<const char*>(b.data()), b.size());
    }
}

// Key components arrive already serialised, one per key column.
void explode_key(const std::vector<bytes>& parts,
                 const std::vector<cql_type>& types,
                 std::vector<value>& out) {
    for (size_t i = 0; i < types.size(); ++i) {
        out.push_back(i < parts.size() ? decode(types[i], bytes_view(parts[i]))
                                       : decode(types[i], bytes_view()));
    }
}

} // namespace

std::vector<cql_column> columns_of(const ::schema& s) {
    std::vector<cql_column> cols;
    for (const auto& c : s.partition_key_columns()) {
        cols.push_back({c.name_as_text(), cql_type_of(*c.type), column_kind::partition_key});
    }
    for (const auto& c : s.clustering_key_columns()) {
        cols.push_back({c.name_as_text(), cql_type_of(*c.type), column_kind::clustering_key});
    }
    for (const auto& c : s.regular_columns()) {
        cols.push_back({c.name_as_text(), cql_type_of(*c.type), column_kind::regular});
    }
    return cols;
}

// ---------------------------------------------------------------- shredder
fragment_shredder::fragment_shredder(const ::schema& s)
    : _schema(s), _cols(columns_of(s)) {
    _n_pk = s.partition_key_size();
    _n_ck = s.clustering_key_size();
}

void fragment_shredder::new_partition(const dht::decorated_key& dk) {
    _pk.clear();
    std::vector<cql_type> types;
    for (const auto& c : _schema.partition_key_columns()) { types.push_back(cql_type_of(*c.type)); }
    std::vector<bytes> parts;
    for (auto&& v : dk.key().components(_schema)) { parts.push_back(linearized(v)); }
    explode_key(parts, types, _pk);
}

void fragment_shredder::add_clustering_row(const clustering_row& cr) {
    row r;
    r.key = _pk;
    std::vector<cql_type> ck_types;
    for (const auto& c : _schema.clustering_key_columns()) { ck_types.push_back(cql_type_of(*c.type)); }
    std::vector<bytes> parts;
    for (auto&& v : cr.key().components(_schema)) { parts.push_back(linearized(v)); }
    explode_key(parts, ck_types, r.key);

    // Regular cells. column_id indexes the regular columns, which is exactly the
    // index space schema_mapping uses for cells.
    cr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& acoc) {
        const column_definition& cdef = _schema.regular_column_at(id);
        if (cdef.is_atomic()) {
            auto av = acoc.as_atomic_cell(cdef);
            cell c;
            c.timestamp = av.timestamp();
            c.live = av.is_live();
            if (c.live) {
                auto lv = av.value().linearize();
                c.v = decode(cql_type_of(*cdef.type), bytes_view(lv));
                if (av.is_live_and_has_ttl()) {
                    c.ttl = int32_t(av.ttl().count());
                    c.local_deletion_time = int32_t(av.expiry().time_since_epoch().count());
                }
            } else {
                c.local_deletion_time = int32_t(av.deletion_time().time_since_epoch().count());
            }
            r.cells.emplace(size_t(id), std::move(c));
        }
        // Collections travel opaquely for now; see design doc section 5.3.
    });
    _rows.push_back(std::move(r));
}

void fragment_shredder::add_static_row(const static_row&) {
    // Static rows need their own row-group section; not part of this step.
}

std::vector<uint8_t> fragment_shredder::to_parquet(const pq_writer_config& cfg) const {
    return write_rows(_cols, _rows, cfg.level, cfg.wopt, cfg.exc);
}

// ---------------------------------------------------------------- writer_impl
pq_writer_impl::pq_writer_impl(sstables::sstable& sst, const ::schema& s,
                               const sstables::sstable_writer_config& cfg,
                               pq_writer_config pcfg, sink_type sink)
    : sstables::sstable_writer::writer_impl(sst, s, cfg)
    , _shredder(s)
    , _pcfg(std::move(pcfg))
    , _sink(std::move(sink)) {}

void pq_writer_impl::consume_new_partition(const dht::decorated_key& dk) {
    _shredder.new_partition(dk);
}

void pq_writer_impl::consume(tombstone) {
    // Partition tombstones need a header section of their own; next step.
}

stop_iteration pq_writer_impl::consume(static_row&& sr) {
    _shredder.add_static_row(sr);
    return stop_iteration::no;
}

stop_iteration pq_writer_impl::consume(clustering_row&& cr) {
    _shredder.add_clustering_row(cr);
    return stop_iteration::no;
}

stop_iteration pq_writer_impl::consume(range_tombstone_change&&) {
    return stop_iteration::no;
}

stop_iteration pq_writer_impl::consume_end_of_partition() {
    return stop_iteration::no;
}

void pq_writer_impl::consume_end_of_stream() {
    auto img = _shredder.to_parquet(_pcfg);
    _pos = img.size();
    if (_sink) { _sink(std::move(img)); }
}

} // namespace sstables::parquet
