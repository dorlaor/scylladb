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
#include <map>
#include <seastar/core/sstring.hh>
#include "sstables/metadata_collector.hh"
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
    // Per-column encodings an operator asked for, already translated from the CQL enum to the
    // Parquet one. Empty for every table that does not set them, which is the common case.
    std::map<std::string, format::encoding> column_encodings{};
    // Empty key id means no encryption. Kept as an id rather than resolved bytes so that a
    // config round-trip (to_map) cannot leak key material.
    seastar::sstring encryption_key_id{};
    format::cipher   encryption_algo = format::cipher::aes_gcm_v1;
    // A row group is cut when either limit trips.
    //
    // `row_group_buffer_bytes` is **buffered shredder memory**, not encoded output, and
    // the two differ by about 343x -- 1 887 B/row held in memory against 5.2 B/row
    // written. It is named for what it measures because the obvious name invites a 343x
    // misconfiguration: someone setting "64 MiB" expecting 64 MiB of Parquet gets about
    // 35 600 rows, which is roughly 185 kB of actual output on a narrow table. Its job is
    // to stop a shard running out of memory (R-13), so it is charged against
    // fragment_shredder::buffered_bytes(), which errs ~4% high on purpose.
    //
    // `row_group_rows` is the read-granularity knob, and at 5 000 it is the one that
    // actually cuts: a row group is then about 9 MB of shredder buffer, far under the
    // byte budget, which reverts to being purely a safety net against a pathological
    // partition. That is the right division of labour -- bytes protects the shard, rows
    // tunes read cost -- and it is the opposite of the original default, where the byte
    // budget did all the cutting at an incidental ~35 600 rows.
    //
    // 5 000 comes from the sweep in design doc 10.4c (20 000 partitions x 5 rows, 2 000
    // random point reads): against one row group per file it is **2.1x lower point-read
    // latency and 3.9x less scan memory for +10 % size**, with write and scan throughput
    // flat across the whole sweep. Point-read latency and resident memory are this
    // format's two weakest metrics and disk is its strongest, so spending disk on both is
    // the right direction -- and it buys more than a cache would, without adding a cached
    // component or a line of new state.
    //
    // Going further costs disproportionately: 1 000 rows buys another 14 % of latency for
    // another 26 % of size. Per-table override via the `parquet` property (5.5a, 8.2).
    size_t row_group_rows         = 5'000;
    size_t row_group_buffer_bytes = 64u << 20;
};

// The user-facing `parquet = {...}` table property (design doc 8.2), parsed and
// validated into a pq_writer_config.
//
// Mirrors how `compression = {...}` becomes compression_parameters: a map of strings
// from CQL, validated once at ALTER/CREATE time so a bad value is a configuration
// error rather than a broken sstable discovered later.
//
// Only the knobs that are actually implemented are accepted. Silently ignoring a
// recognised-looking option is worse than rejecting it -- a user who sets
// `compression: 'gzip'` and gets zstd has been lied to -- so anything the writer
// cannot honour is an error with the supported set named.
// The CQL type as the writer sees it. Anything it does not special-case travels as an opaque
// BYTE_ARRAY, which is why this returns `blob` rather than failing: a blob column genuinely supports
// the byte_array encodings, so an override naming one is legitimate.
cql_type cql_type_of(const abstract_type&);

class parquet_parameters {
public:
    static constexpr const char* ROW_GROUP_ROWS         = "row_group_rows";
    static constexpr const char* ROW_GROUP_BUFFER_BYTES = "row_group_buffer_bytes";
    static constexpr const char* PAGE_ROWS              = "page_rows";
    static constexpr const char* COMPRESSION            = "compression";
    static constexpr const char* COMPRESSION_LEVEL      = "compression_level";
    static constexpr const char* METADATA_FOLDING       = "metadata_folding";
    static constexpr const char* DICTIONARY            = "dictionary";
    // Per-column encoding override: `parquet = {'encoding.<column>': '<enum>'}`.
    //
    // The writer's own choice is two-stage -- the schema proposes a hint from the column's kind and
    // type, and the data decides whether a dictionary beats it -- and both stages are deliberately
    // conservative. This is the escape hatch for the cases they get wrong, of which there are known
    // ones: a *wide scan-only* table wants encodings tuned for size at the cost of point-read
    // latency, and a text partition key that happens to be sorted in token order would benefit from
    // front coding that the structural rule refuses to apply (§10.13).
    //
    // A prefix rather than a nested map because a CQL table property is map<text,text>; there is no
    // nesting to be had. The column name is taken verbatim, so a quoted CQL identifier keeps its
    // case.
    static constexpr const char* ENCODING_PREFIX        = "encoding.";
    // Parquet Modular Encryption. The algorithm is an enum; the key is named by *id* and
    // resolved locally from parquet_encryption_key_file, so no key material ever enters the
    // schema (which is replicated, stored in system tables and printed by DESCRIBE).
    static constexpr const char* ENCRYPTION            = "encryption";
    static constexpr const char* ENCRYPTION_KEY        = "encryption_key";

    // Guard rails. The lower bound on rows is not arbitrary: below ~1 000 rows the
    // fixed per-row-group metadata (~225 B per leaf) starts to dominate the file --
    // at 100 rows on a 20-leaf table it is 45 B/row against a 5.2 B/row total, so the
    // file grows about ninefold (design doc 10.4c).
    static constexpr size_t min_row_group_rows = 1'000;
    static constexpr size_t max_row_group_rows = 100'000'000;
    static constexpr size_t min_buffer_bytes   = 1u << 20;    // 1 MiB
    static constexpr size_t max_buffer_bytes   = 1024ull << 20; // 1 GiB

    parquet_parameters() = default;
    explicit parquet_parameters(const std::map<sstring, sstring>& opts);

    // Only entries that differ from the defaults, so DESCRIBE stays terse.
    std::map<sstring, sstring> to_map() const;

    const pq_writer_config& config() const { return _cfg; }

    // Every value the enum accepts, in the order the error message lists them. `auto` means "let the
    // writer decide", which is the default and the only way to spell "undo an override" in an ALTER.
    enum class column_encoding {
        automatic, plain, dictionary, delta_binary_packed, delta_byte_array,
        delta_length_byte_array, byte_stream_split,
    };
    static std::optional<column_encoding> parse_column_encoding(std::string_view);
    static const char* to_string(column_encoding);
    // Which encodings are legal for a physical type. Checked at DDL time rather than at write time,
    // because a write-time rejection would take the table down on a setting that looked accepted.
    static bool applies_to(column_encoding, cql_type);

    const std::map<sstring, column_encoding>& column_encodings() const { return _column_encodings; }

private:
    pq_writer_config _cfg;
    std::map<sstring, column_encoding> _column_encodings;
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
    size_t _buffered_bytes = 0;
    std::vector<value> _pk;      // current partition's key components
    std::optional<deletion_info> _part_del;
    std::map<size_t, cell> _static_cells;     // indexed as value columns
    std::map<size_t, collection_cell> _static_collections;
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
    void clear() { _rows.clear(); _buffered_bytes = 0; }

    // Estimated heap held by the buffered rows, for the write-side memory budget
    // (R-13, design doc 5.5a). Errs high on purpose -- see heap_bytes(). Accumulated
    // as rows are appended rather than walked on demand, because the writer needs to
    // consult it after every row.
    size_t buffered_bytes() const { return _buffered_bytes; }

    // Schema + shred + encode the accumulated rows into a Parquet file image.
    // Accepts any folding level, including the lossy export-only L3.
    std::vector<uint8_t> to_parquet(const pq_writer_config&) const;

    // Same, but refuses a lossy folding level. Everything on the storage path
    // must go through this: writing L3 into an sstable would silently discard
    // write times, TTLs and deletions.
    std::vector<uint8_t> to_parquet_for_storage(const pq_writer_config&) const;

private:
    // Static content rides on every row of the partition, because the reader
    // rebuilds the static row from whichever row it sees first -- and that may be
    // a range tombstone change or a placeholder, not a clustering row. Both the
    // atomic cells and the collections have to go on, together: replaying only
    // the cells drops every static collection in any partition whose first row is
    // not a clustering row.
    void replay_statics(row& r) const;

    // The one place a row enters the buffer, so the memory accounting cannot drift
    // away from the buffer the way the static-cell replay once did.
    void push_row(row&& r);
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
    // True once the file writer is streaming into _data_writer, in which case
    // consume_end_of_stream must not write the image again -- finish() returns nothing.
    bool              _streaming = false;

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

    // Statistics metadata, collected with the same semantics as mx -- see
    // sstables/mx/writer.cc write_cell() and write_liveness_info().
    //
    // This is correctness, not reporting. The min/max timestamps, the
    // local-deletion-time range and the tombstone drop-time histogram are what
    // compaction and tombstone garbage collection read; an sstable that
    // under-reports them can have a tombstone dropped while data it shadows is
    // still live. Before this existed the pq writer fed its metadata_collector
    // only add_key().
    sstables::column_stats _c_stats;

    void collect_atomic_cell(const atomic_cell_view& cell);
    void collect_cell(const column_definition& cdef, const atomic_cell_or_collection& acoc);
    void collect_cells(const ::row& cells, ::column_kind kind);
    void collect_marker(const row_marker& marker);

    // Streaming row groups. Both stay empty for an sstable that fits inside the
    // budget, which then takes the single-shot path in consume_end_of_stream() and is
    // byte-for-byte what it was before -- so every size measured in design doc 10 is
    // unaffected.
    //
    // Once a cut happens the leaf set has to be fixed, and it cannot be derived from
    // rows not yet seen, so it becomes the conservative one (design doc 5.5a).
    std::optional<mapped_schema> _ms;
    std::unique_ptr<format::parquet_file_writer> _pq;
    // Rows already flushed into earlier row groups. The index entry is a file-global
    // row ordinal (option A), so it cannot come from the shredder's own size once the
    // shredder is being cleared at each cut.
    uint64_t _rows_flushed = 0;

    void cut_row_group();
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
