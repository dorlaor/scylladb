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
#include "sstables/sstables.hh"
#include "sstables/storage.hh"

#include <seastar/core/fstream.hh>

#include "sstables/writer.hh"
#include "sstables/storage.hh"
#include "keys/keys.hh"
#include "dht/i_partitioner.hh"

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

std::vector<uint8_t> fragment_shredder::to_parquet_for_storage(const pq_writer_config& cfg) const {
    if (!folding_is_lossless(cfg.level)) {
        throw std::invalid_argument(
                std::string("folding level ") + to_string(cfg.level) +
                " discards cell metadata and cannot be used as a storage format; "
                "it is available for export only");
    }
    return to_parquet(cfg);
}

// ---------------------------------------------------------------- writer_impl
pq_writer_impl::pq_writer_impl(sstables::sstable& sst, const ::schema& s,
                               uint64_t estimated_partitions,
                               const sstables::sstable_writer_config& cfg,
                               pq_writer_config pcfg, shard_id shard, sink_type sink)
    : sstables::sstable_writer::writer_impl(sst, s, cfg)
    , _shredder(s)
    , _pcfg(std::move(pcfg))
    , _sink(std::move(sink)) {
    if (_sink) {
        return;   // unit-test path: no sstable components at all
    }
    // Zero is benign here but not further down; mx clamps for the same reason.
    estimated_partitions = std::max(uint64_t(1), estimated_partitions);

    // create_data() is what actually opens the Data and Index files. Until it
    // runs, sst._data_file is null and make_data_or_index_sink dereferences it.
    sst.open_sstable(cfg.origin);
    sst.create_data().get();
    sst._shards = { shard };

    _index_sampling_state.summary_byte_cost = cfg.summary_byte_cost;
    _index_sampling_state.max_partitions_per_page = cfg.summary_max_partitions_per_page;

    {
        auto out = sst._storage->make_data_or_index_sink(sst, component_type::Data).get();
        _data_writer = std::make_unique<crc32_checksummed_file_writer>(
                std::move(out), sst.sstable_buffer_size, sst.get_filename());
    }
    if (sst.has_component(component_type::Index)) {
        auto out = sst._storage->make_data_or_index_sink(sst, component_type::Index).get();
        _index_writer = std::make_unique<crc32_digest_file_writer>(
                std::move(out), sst.sstable_buffer_size, sst.index_filename());
        sstables::prepare_summary(sst._components->summary, estimated_partitions,
                                  s.min_index_interval());
    }

    _cfg.monitor->on_write_started(_data_writer->offset_tracker());
    if (sst.has_component(component_type::Filter)) {
        sst._components->filter = utils::i_filter::get_filter(
                estimated_partitions, s.bloom_filter_fp_chance(), utils::filter_format::m_format);
    }
}

// The mc index entry is: key, then a vint position, then a vint promoted-index
// size. For pq the position is the row ordinal, and the promoted index is always
// absent because Parquet's ColumnIndex already provides intra-partition seeking
// (design doc open question 2).
void pq_writer_impl::finish_open_partition() {
    if (!_in_partition || !_index_writer) {
        _in_partition = false;
        return;
    }
    write_vint(*_index_writer, uint64_t(0));   // no promoted index
    _in_partition = false;
}

void pq_writer_impl::consume_new_partition(const dht::decorated_key& dk) {
    finish_open_partition();
    _shredder.new_partition(dk);
    _partition_first_row = _shredder.size();

    if (_index_writer) {
        auto pk = key::from_partition_key(_schema, dk.key());
        // The filter and the min/max key statistics are fed per partition, as
        // mx does. Without the filter every point read on this sstable misses.
        if (_sst._components->filter) {
            _sst._components->filter->add(utils::make_hashed_key(bytes_view(pk)));
        }
        _collector.add_key(bytes_view(pk));
        sstables::maybe_add_summary_entry(_sst._components->summary, dk.token(),
                bytes_view(pk), _index_writer->offset(), _index_writer->offset(),
                _index_sampling_state);
        // Same on-disk shape as mc: a uint16-prefixed key, then a vint. Only the
        // meaning of the vint differs.
        auto p_key = disk_string_view<uint16_t>();
        p_key.value = bytes_view(pk);
        write(_sst.get_version(), *_index_writer, p_key);
        // Option A: the row ordinal of this partition's first row, not a byte
        // offset. The reader maps it to a page through the OffsetIndex.
        write_vint(*_index_writer, _partition_first_row);
        _in_partition = true;

        // After the write: p_key views into pk.
        if (!_first_key) { _first_key = pk; }
        _last_key = std::move(pk);
    }
    ++_num_partitions;
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
    auto img = _shredder.to_parquet_for_storage(_pcfg);
    _pos = img.size();

    // A sink is the unit-test path: it lets the whole fragment -> Parquet route be
    // driven without constructing an sstable.
    if (_sink) {
        _sink(std::move(img));
        return;
    }

    // Otherwise the image becomes the Data component. consume_end_of_stream runs
    // in a seastar thread (mx::writer relies on the same), so blocking here is
    // allowed.
    finish_open_partition();
    _data_writer->write(reinterpret_cast<const char*>(img.data()), img.size());
    _data_writer->close();
    _sst.write_digest(_data_writer->full_checksum());
    _sst.write_crc(_data_writer->finalize_checksum());
    _data_writer.reset();
    _cfg.monitor->on_data_write_completed();
    write_components();
}

void pq_writer_impl::write_components() {
    if (_index_writer) {
        sstables::seal_summary(_sst._components->summary, std::move(_first_key),
                               std::move(_last_key), _index_sampling_state).get();
        _index_writer->close();
        _index_writer.reset();
    }
    _sst.set_first_and_last_keys();

    sstables::seal_statistics(_sst.get_version(), _sst._components->statistics, _collector,
            _schema.get_partitioner().name(), _schema.bloom_filter_fp_chance(),
            _sst.get_schema(), _sst.get_first_decorated_key(), _sst.get_last_decorated_key(),
            encoding_stats{});

    _sst.maybe_rebuild_filter_from_index(_num_partitions);
    _sst.write_summary();
    _sst.write_filter();
    _sst.write_statistics();
    _sst.write_scylla_metadata(this_shard_id(), run_identifier{_cfg.run_identifier},
                               std::nullopt, std::nullopt, std::nullopt);
    if (!_cfg.leave_unsealed) {
        _sst.seal_sstable(_cfg.backup).get();
    }
}

std::unique_ptr<sstables::sstable_writer::writer_impl> make_writer(
        sstables::sstable& sst,
        const ::schema& s,
        uint64_t estimated_partitions,
        const sstables::sstable_writer_config& cfg,
        encoding_stats /*enc_stats*/,
        shard_id shard) {
    return std::make_unique<pq_writer_impl>(sst, s, estimated_partitions, cfg,
                                            pq_writer_config{}, shard, nullptr);
}

} // namespace sstables::parquet
