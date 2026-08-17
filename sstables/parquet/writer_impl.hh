/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The bridge between Scylla's mutation-fragment stream and the Parquet
// shredder: an sstable_writer::writer_impl that accumulates rows and emits a
// Parquet file image at end of stream.
//
// This is the Phase 3 gate. Everything below it -- folding, shredding, encoding,
// file assembly -- already exists and is tested in schema_mapping.hh and
// format/. This class only translates types.
//
// Scope of this step: the fragment stream is consumed for real, and a valid
// Parquet image is produced. Writing that image into the sstable's Data
// component (and the matching reader and index components) is the next step, so
// the image is handed to a caller-supplied sink rather than to sstable storage.
// That also lets the whole path be driven from a unit test without an sstable.

#include "sstables/parquet/schema_mapping.hh"
#include "sstables/writer_impl.hh"
#include "sstables/writer.hh"

#include <functional>
#include <memory>
#include <optional>
#include <vector>

namespace sstables::parquet {

struct pq_writer_config {
    folding_level      level = folding_level::row_folded;
    exception_encoding exc   = exception_encoding::sparse;
    format::writer_options wopt{};
    // Rows buffered before a row group is cut. The real writer will bound this
    // by bytes as well -- see design doc section 5.5 on the memory budget.
    size_t row_group_rows = 1'000'000;
};

// Builds the layer-2 column description for a Scylla schema. Exposed because
// the reader will need exactly the same mapping to invert it.
std::vector<cql_column> columns_of(const ::schema& s);

// Index, within the value columns produced by columns_of(), of the first static
// column. Everything at or after it is static; everything before is regular.
inline size_t static_base(const ::schema& s) { return s.regular_columns_count(); }

// Converts a mutation-fragment stream into rows. Split out from the
// writer_impl so it can be unit-tested without constructing an sstable.
class fragment_shredder {
    const ::schema& _schema;
    std::vector<cql_column> _cols;
    std::vector<row> _rows;
    std::vector<value> _pk;      // current partition's key components
    std::optional<deletion_info> _part_del;
    std::map<size_t, cell> _static_cells;     // indexed as value columns
    size_t _static_base = 0;
    bool _saw_clustering_row = false;
    size_t _n_pk = 0, _n_ck = 0;

public:
    explicit fragment_shredder(const ::schema& s);

    void new_partition(const dht::decorated_key& dk);
    // Applies to every row of the current partition until the next one.
    void set_partition_tombstone(tombstone);
    // Closes the open partition. A partition whose only content is a static row
    // has no clustering row to attach it to, so one placeholder row is emitted
    // and marked with __no_ck.
    void end_partition();
    void add_clustering_row(const clustering_row& cr);
    void add_static_row(const static_row& sr);
    void add_range_tombstone_change(const range_tombstone_change& rtc);

    const std::vector<cql_column>& columns() const { return _cols; }
    const std::vector<row>& rows() const { return _rows; }
    size_t size() const { return _rows.size(); }
    void clear() { _rows.clear(); }

    // Schema + shred + encode the accumulated rows into a Parquet file image.
    // Accepts any folding level, including the lossy export-only L3.
    std::vector<uint8_t> to_parquet(const pq_writer_config&) const;

    // Same, but refuses a lossy folding level. Everything on the storage path
    // must go through this: writing L3 into an sstable would silently discard
    // write times, TTLs and deletions.
    std::vector<uint8_t> to_parquet_for_storage(const pq_writer_config&) const;
};

class pq_writer_impl : public sstables::sstable_writer::writer_impl {
public:
    using sink_type = std::function<void(std::vector<uint8_t>)>;

private:
    fragment_shredder _shredder;
    pq_writer_config  _pcfg;
    encoding_stats    _enc_stats;
    sink_type         _sink;
    uint64_t          _pos = 0;

    // Partition index. Entries carry a *row ordinal*, not a byte offset -- see
    // design doc 5.4 option A. The reader turns that ordinal into a page via the
    // Parquet OffsetIndex.
    // Held as the base type: the write() overload for keys takes file_writer&.
    std::unique_ptr<file_writer> _index_writer;
    // Opened up front, like mx does: create_data() hands the sstable's data and
    // index files to the storage layer, and nothing can be written before it.
    std::unique_ptr<crc32_checksummed_file_writer> _data_writer;
    sstables::index_sampling_state _index_sampling_state;
    std::optional<key> _first_key, _last_key;
    uint64_t _num_partitions = 0;
    // Ordinal of the first row of the partition currently being consumed.
    uint64_t _partition_first_row = 0;
    bool _in_partition = false;

    void finish_open_partition();
    void write_components();

public:
    pq_writer_impl(sstables::sstable& sst, const ::schema& s,
                   uint64_t estimated_partitions,
                   const sstables::sstable_writer_config& cfg,
                   pq_writer_config pcfg, encoding_stats enc_stats,
                   shard_id shard, sink_type sink);

    void consume_new_partition(const dht::decorated_key& dk) override;
    void consume(tombstone t) override;
    stop_iteration consume(static_row&& sr) override;
    stop_iteration consume(clustering_row&& cr) override;
    stop_iteration consume(range_tombstone_change&& rtc) override;
    stop_iteration consume_end_of_partition() override;
    void consume_end_of_stream() override;
    uint64_t data_file_position_for_tests() const override { return _pos; }
};

// Factory used by sstable_writer when the sstable's version is `pq`. Mirrors
// mc::make_writer so the dispatch in writer.cc stays a one-line choice.
//
// The pq_writer_config is defaulted here (L1, sparse exceptions). Deriving it
// from the table's own storage-format properties -- rows per row group and so
// on, per design doc section 6 -- is the next step; nothing about that changes
// this signature.
std::unique_ptr<sstables::sstable_writer::writer_impl> make_writer(
        sstables::sstable& sst,
        const ::schema& s,
        uint64_t estimated_partitions,
        const sstables::sstable_writer_config& cfg,
        encoding_stats enc_stats,
        shard_id shard);

} // namespace sstables::parquet
