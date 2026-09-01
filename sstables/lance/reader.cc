/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/lance/reader.hh"
#include "sstables/lance/glue.hh"
#include "sstables/parquet/writer_impl.hh"
#include "sstables/parquet/schema_mapping.hh"
#include "sstables/parquet/footer_cache.hh"
#include "sstables/sstables.hh"
#include "sstables/index_reader.hh"
#include "sstables/mutation_fragment_filter.hh"
#include "readers/forwardable.hh"
#include "mutation/collection_mutation.hh"
#include "mutation/counters.hh"
#include "types/collection.hh"
#include "mutation/mutation_fragment.hh"
#include "keys/keys.hh"
#include "schema/schema.hh"
#include "types/types.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/log.hh>

#include <bit>
#include <cstring>
#include <tuple>

namespace sstables::lance {

using namespace sstables::parquet;   // the shared value model: row, cell, cql_column, ...

static seastar::logger lclog("lc_reader");

namespace {

// Same window sizes as the pq reader, for the same reasons.
constexpr int64_t scan_window_rows = 16384;
constexpr int64_t point_window_rows = 512;

// Identical to the pq reader's helpers: rebuild the serialised forms the
// shredder took apart. See sstables/parquet/reader.cc for the commentary.
bytes lc_encode(cql_type t, const value& v) {
    auto be = [] (uint64_t x, size_t n) {
        bytes b(bytes::initialized_later(), n);
        for (size_t i = 0; i < n; ++i) { b[i] = int8_t(uint8_t(x >> (8 * (n - 1 - i)))); }
        return b;
    };
    switch (t) {
    case cql_type::int32:
        return be(uint32_t(std::get<int32_t>(v)), 4);
    case cql_type::bigint:
    case cql_type::timestamp:
        return be(uint64_t(std::get<int64_t>(v)), 8);
    case cql_type::dbl:
        return be(std::bit_cast<uint64_t>(std::get<double>(v)), 8);
    default: {
        const auto& s = std::get<std::string>(v);
        return bytes(reinterpret_cast<const int8_t*>(s.data()), s.size());
    }
    }
}

atomic_cell_or_collection build_counter(const collection_cell& cc) {
    if (cc.elements.empty()) {
        const auto ts = cc.tomb ? cc.tomb->timestamp : api::missing_timestamp;
        const auto ldt = gc_clock::time_point(
                gc_clock::duration(cc.tomb ? cc.tomb->local_deletion_time : 0));
        return atomic_cell_or_collection(atomic_cell::make_dead(ts, ldt));
    }
    counter_cell_builder b(cc.elements.size());
    for (const auto& e : cc.elements) {
        int64_t msb = 0, lsb = 0, value = 0, clock = 0;
        if (!unpack_i64_pair(e.key, msb, lsb) ||
            !e.value || !unpack_i64_pair(*e.value, value, clock)) {
            throw std::runtime_error("lc: malformed counter shard");
        }
        b.add_maybe_unsorted_shard(counter_shard(
                counter_id(utils::UUID(msb, lsb)), value, clock));
    }
    b.sort_and_remove_duplicates();
    return atomic_cell_or_collection(b.build(cc.elements.front().timestamp));
}

atomic_cell_or_collection build_collection(const column_definition& cdef,
                                           const collection_cell& cc) {
    const auto* ctype = dynamic_cast<const collection_type_impl*>(cdef.type.get());
    auto vtype = ctype ? ctype->value_comparator() : cdef.type;
    tombstone t;
    if (cc.tomb) {
        t = tombstone(cc.tomb->timestamp,
                      gc_clock::time_point(gc_clock::duration(cc.tomb->local_deletion_time)));
    }
    collection_mutation_writer w(t);
    for (const auto& e : cc.elements) {
        auto key = managed_bytes(reinterpret_cast<const int8_t*>(e.key.data()), e.key.size());
        if (!e.value) {
            auto ldt = gc_clock::time_point(
                    gc_clock::duration(e.local_deletion_time.value_or(0)));
            w.push_back(managed_bytes_view(key), atomic_cell::make_dead(e.timestamp, ldt));
            continue;
        }
        auto vb = bytes(reinterpret_cast<const int8_t*>(e.value->data()), e.value->size());
        if (e.ttl && e.local_deletion_time) {
            w.push_back(managed_bytes_view(key),
                    atomic_cell::make_live(*vtype, e.timestamp, bytes_view(vb),
                            gc_clock::time_point(gc_clock::duration(*e.local_deletion_time)),
                            gc_clock::duration(*e.ttl)));
        } else {
            w.push_back(managed_bytes_view(key),
                    atomic_cell::make_live(*vtype, e.timestamp, bytes_view(vb)));
        }
    }
    return atomic_cell_or_collection(std::move(w).finish());
}

size_t heap_size(const std::string& s) {
    return s.capacity() > 15 ? s.capacity() + 1 : 0;
}

} // namespace

// The retained form of one lc sstable's metadata: the parsed footer, schema
// and every column's ColumnMetadata, plus a lazily-filled cache of parsed
// miniblock chunk indexes -- the piece a point read consults to turn a row
// ordinal into one chunk-sized extent, and small enough to keep (a u16 per
// chunk on disk; tens of KB per sstable). Published into the same reclaimable
// slot the pq footer uses (sstable::_pq_footer): an sstable is `lc` or `pq`,
// never both, and the reclaim machinery neither knows nor cares which format
// the bytes describe.
class cached_lance_meta final : public parquet::cached_footer_base {
public:
    format::footer ft;
    format::file_descriptor fd;
    std::vector<format::column_meta> columns;
    // Parsed page layouts, one per (column, page), parsed once. Indexed like
    // columns[c].pages[p].
    std::vector<std::vector<format::page_layout>> layouts;

    struct mb_index_key {
        size_t col;
        size_t page;
        bool operator<(const mb_index_key& o) const {
            return col != o.col ? col < o.col : page < o.page;
        }
    };
    // Shard-local, mutated under the seastar single-thread model; mutable so a
    // reader holding a const entry can fill it. Growth is reported through
    // sstable::grow_pq_footer_cache by the reader that inserts.
    mutable std::map<mb_index_key, format::miniblock_index> mb_indexes;

    // Decoded miniblock chunks, for point reads: the pq reader keeps a
    // decompressed-page cache for exactly this reason -- a steady point-read
    // workload otherwise pays the fetch and the zstd decompress of the same
    // few-KB chunk on every probe. Whole decoded chunks, keyed by
    // (leaf, page, chunk); crudely bounded: past the cap the whole map is
    // dropped, which for a cache of independent equal-sized entries is a
    // serviceable stand-in for LRU at a fraction of the bookkeeping.
    struct chunk_key {
        size_t col;
        size_t page;
        size_t chunk;
        bool operator<(const chunk_key& o) const {
            return std::tie(col, page, chunk) < std::tie(o.col, o.page, o.chunk);
        }
    };
    // One cached chunk: the def sub-buffer as stored (bitpacked, ~128 B) and
    // the value sub-buffer already de-zstd'd. Raw bytes, not decoded values:
    // a decoded 4096-string chunk costs ~30x its raw size in std::string
    // overhead alone, which turned the first version of this cache into a
    // thrash loop at any real row count. Probes slice-decode from the raw
    // bytes, which is exactly the cheap operation the format designed for.
    struct plain_chunk {
        std::string def;
        std::string values;
    };
    mutable std::map<chunk_key, plain_chunk> chunk_cache;
    mutable size_t chunk_cache_bytes = 0;
    static constexpr size_t chunk_cache_cap = 32u << 20;

    size_t memory_size() const noexcept override {
        size_t n = sizeof(*this);
        for (const auto& f : fd.fields) {
            n += heap_size(f.name) + heap_size(f.logical_type);
        }
        for (const auto& cm : columns) {
            n += cm.pages.capacity() * sizeof(format::page_info);
            for (const auto& p : cm.pages) {
                n += p.buffers.capacity() * sizeof(format::buffer_ref);
                n += heap_size(p.encoding.any_bytes);
            }
        }
        for (const auto& v : layouts) {
            n += v.capacity() * sizeof(format::page_layout);
        }
        for (const auto& [k, idx] : mb_indexes) {
            n += 64 + idx.chunks.capacity() * sizeof(format::miniblock_index::chunk);
        }
        return n;
    }
};

namespace {

class lc_reader : public mutation_reader::impl {
    sstables::shared_sstable _sst;
    const dht::partition_range* _pr;
    query::partition_slice _slice;
    sstables::read_monitor& _mon;
    const bool _use_index;
    sstables::reader_position_tracker _tracker;
    bool _read_started = false;
    bool _read_completed = false;

    bool _init = false;
    seastar::shared_ptr<const cached_lance_meta> _cm;
    mapped_schema _ms;
    std::vector<cql_column> _cols;
    size_t _n_pk = 0, _n_ck = 0, _static_base = 0;

    // Ordinal window this read is confined to, from the partition index.
    int64_t _row_lo = 0, _row_hi = 0;

    // Decoded window.
    std::vector<row> _rows;
    size_t _pos = 0;
    int64_t _cursor = 0;

    // Partition being assembled across windows.
    std::vector<value> _open_pk;
    std::optional<dht::decorated_key> _open_dk;
    std::optional<mutation_fragment_filter> _filter;
    bool _open = false;
    bool _skipping = false;

    future<> init();
    future<temporary_buffer<char>> tracked_read(uint64_t off, size_t len);
    future<> load_meta();
    future<bool> next_window();
    future<format::column_values> read_leaf_rows(size_t leaf, uint64_t lo, uint64_t hi);
    const format::miniblock_index& mb_index_of(size_t leaf, size_t page, std::string_view meta_buf);
    void emit_row(const row& r);
    void close_partition();

    dht::decorated_key key_of(std::span<const value> pk) const {
        std::vector<bytes> parts;
        parts.reserve(_n_pk);
        for (size_t i = 0; i < _n_pk; ++i) { parts.push_back(lc_encode(_cols[i].type, pk[i])); }
        return dht::decorate_key(*_schema, partition_key::from_exploded(*_schema, parts));
    }

public:
    lc_reader(sstables::shared_sstable sst, schema_ptr s, reader_permit permit,
              const dht::partition_range& pr, const query::partition_slice& slice,
              sstables::read_monitor& mon, bool use_index)
        : impl(std::move(s), std::move(permit))
        , _sst(std::move(sst)), _pr(&pr), _slice(slice), _mon(mon), _use_index(use_index) {}

    future<> fill_buffer() override {
        if (_end_of_stream) { co_return; }
        co_await init();
        while (!is_buffer_full() && !_end_of_stream) {
            if (_pos == _rows.size()) {
                if (!co_await next_window()) {
                    close_partition();
                    _end_of_stream = true;
                    break;
                }
                continue;
            }
            emit_row(_rows[_pos++]);
        }
    }

    future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty() && !_end_of_stream) {
            while (_open) {
                if (_pos == _rows.size()) {
                    if (!co_await next_window()) { _open = false; _end_of_stream = true; break; }
                    continue;
                }
                std::vector<value> pk(_rows[_pos].key.begin(),
                                      _rows[_pos].key.begin() + long(_n_pk));
                if (pk != _open_pk) { _open = false; break; }
                ++_pos;
            }
        }
        co_return;
    }

    future<> fast_forward_to(const dht::partition_range& pr) override {
        clear_buffer();
        _end_of_stream = false;
        _pr = &pr;
        _init = false;
        _open = false;
        _rows.clear();
        _pos = 0;
        return make_ready_future<>();
    }

    future<> fast_forward_to(position_range) override {
        on_internal_error(lclog, "lc_reader: intra-partition forwarding should be "
                                 "handled by the forwardable adapter");
    }

    future<> close() noexcept override {
        if (_read_started && !_read_completed) {
            _read_completed = true;
            _mon.on_read_completed();
        }
        return make_ready_future<>();
    }
};

} // namespace

future<temporary_buffer<char>> lc_reader::tracked_read(uint64_t off, size_t len) {
    if (!_read_started) {
        _read_started = true;
        _tracker.total_read_size = _sst->ondisk_data_size();
        _mon.on_read_started(_tracker);
    }
    auto buf = co_await _sst->data_read(off, len, _permit);
    _tracker.position += buf.size();
    co_return std::move(buf);
}

future<> lc_reader::load_meta() {
    if (_cm) { co_return; }
    auto& stats = parquet::footer_cache_stats_local();
    if (auto& cached = _sst->pq_footer_cache()) {
        ++stats.hits;
        _cm = seastar::static_pointer_cast<const cached_lance_meta>(cached);
        co_return;
    }
    ++stats.misses;

    auto entry = seastar::make_shared<cached_lance_meta>();
    const uint64_t len = _sst->ondisk_data_size();
    if (len < format::footer_size) {
        throw std::runtime_error("lc: data component too small for a Lance footer");
    }
    {
        auto tail = co_await tracked_read(len - format::footer_size, format::footer_size);
        entry->ft = format::parse_footer(std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(tail.get()), tail.size()));
    }
    const auto& ft = entry->ft;
    // The tables and column metadata blocks sit contiguously between
    // col_meta_start and the footer; one read covers them all. The schema
    // global buffer sits before them and needs its own read.
    if (ft.cmo_offset > len || ft.gbo_offset > len || ft.col_meta_start > ft.cmo_offset) {
        throw std::runtime_error("lc: metadata offsets out of range");
    }
    auto region = co_await tracked_read(ft.col_meta_start, size_t(len - ft.col_meta_start));
    auto at = [&](uint64_t off, uint64_t sz) {
        if (off < ft.col_meta_start || off + sz > len) {
            throw std::runtime_error("lc: metadata reference out of the tail region");
        }
        return std::string_view(region.get() + (off - ft.col_meta_start), size_t(sz));
    };
    auto gbo = format::parse_offset_table(std::span<const uint8_t>(
                    reinterpret_cast<const uint8_t*>(at(ft.gbo_offset, uint64_t(ft.num_global_buffers) * 16).data()),
                    size_t(ft.num_global_buffers) * 16), ft.num_global_buffers);
    if (gbo.empty()) {
        throw std::runtime_error("lc: no global buffers; the schema is missing");
    }
    {
        auto schema_buf = co_await tracked_read(gbo[0].offset, size_t(gbo[0].size));
        entry->fd = format::parse_file_descriptor(std::string_view(schema_buf.get(), schema_buf.size()));
    }
    auto cmo = format::parse_offset_table(std::span<const uint8_t>(
                    reinterpret_cast<const uint8_t*>(at(ft.cmo_offset, uint64_t(ft.num_columns) * 16).data()),
                    size_t(ft.num_columns) * 16), ft.num_columns);
    entry->columns.reserve(cmo.size());
    entry->layouts.resize(cmo.size());
    for (size_t c = 0; c < cmo.size(); ++c) {
        entry->columns.push_back(format::parse_column_meta(at(cmo[c].offset, cmo[c].size)));
        auto& lv = entry->layouts[c];
        lv.reserve(entry->columns.back().pages.size());
        for (const auto& pg : entry->columns.back().pages) {
            if (pg.encoding.any_bytes.empty()) {
                throw std::runtime_error("lc: page without a direct encoding");
            }
            lv.push_back(format::parse_page_layout(pg.encoding.any_bytes));
        }
    }
    _cm = entry;
    _sst->set_pq_footer_cache(std::move(entry));
}

const format::miniblock_index& lc_reader::mb_index_of(size_t leaf, size_t page,
                                                      std::string_view meta_buf) {
    cached_lance_meta::mb_index_key key{leaf, page};
    auto it = _cm->mb_indexes.find(key);
    if (it != _cm->mb_indexes.end()) {
        ++parquet::offset_index_cache_stats_local().hits;
        return it->second;
    }
    ++parquet::offset_index_cache_stats_local().misses;
    const auto& pl = _cm->layouts[leaf][page];
    auto idx = format::parse_miniblock_index(meta_buf, pl.num_items);
    const size_t grew = 64 + idx.chunks.capacity() * sizeof(format::miniblock_index::chunk);
    auto [pos, inserted] = _cm->mb_indexes.emplace(key, std::move(idx));
    if (inserted) {
        ++parquet::offset_index_cache_stats_local().populations;
        _sst->grow_pq_footer_cache(grew);
    }
    return pos->second;
}

// Reads and decodes values [lo, hi) of one leaf -- the Lance replacement for
// the pq reader's row-group/page machinery. Per page: constant costs no I/O,
// full-zip costs one or two reads, miniblock costs the chunk-metadata buffer
// (cached after the first read) plus exactly the chunks covering the rows.
future<format::column_values> lc_reader::read_leaf_rows(size_t leaf, uint64_t lo, uint64_t hi) {
    const auto& cm = _cm->columns.at(leaf);
    format::column_values out;
    size_t p = cm.page_for_row(lo);
    for (; p < cm.pages.size(); ++p) {
        const auto& pg = cm.pages[p];
        if (pg.priority >= hi) { break; }
        const uint64_t page_lo = std::max(lo, pg.priority);
        const uint64_t page_hi = std::min(hi, pg.priority + pg.rows);
        const auto& pl = _cm->layouts[leaf][p];
        format::column_values part;
        switch (pl.k) {
        case format::page_layout::kind::miniblock: {
            if (pg.buffers.size() < 2) { throw std::runtime_error("lc: miniblock page needs two buffers"); }
            const format::miniblock_index* idx;
            {
                cached_lance_meta::mb_index_key key{leaf, p};
                auto it = _cm->mb_indexes.find(key);
                if (it != _cm->mb_indexes.end()) {
                    ++parquet::offset_index_cache_stats_local().hits;
                    idx = &it->second;
                } else {
                    auto meta_buf = co_await tracked_read(pg.buffers[0].offset, size_t(pg.buffers[0].size));
                    idx = &mb_index_of(leaf, p, std::string_view(meta_buf.get(), meta_buf.size()));
                }
            }
            const size_t c_lo = idx->chunk_for(page_lo - pg.priority);
            const size_t c_hi = idx->chunk_for(page_hi - 1 - pg.priority);
            if (c_lo == c_hi) {
                // The point-read shape: everything wanted lives in one chunk.
                // A miss fetches the chunk and stores its sub-buffers with the
                // zstd already undone; hits and misses alike then slice-decode
                // just the requested values.
                cached_lance_meta::chunk_key ckey{leaf, p, c_lo};
                auto& pstats = parquet::page_cache_stats_local();
                const auto& ch = idx->chunks[c_lo];
                auto it = _cm->chunk_cache.find(ckey);
                if (it == _cm->chunk_cache.end()) {
                    ++pstats.misses;
                    auto chunk_bytes = co_await tracked_read(
                            pg.buffers[1].offset + ch.byte_offset, ch.byte_size);
                    auto plain = format::split_miniblock_chunk(pl, ch.values,
                            std::string_view(chunk_bytes.get(), chunk_bytes.size()));
                    const size_t sz = 64 + plain.first.size() + plain.second.size();
                    if (_cm->chunk_cache_bytes + sz > cached_lance_meta::chunk_cache_cap) {
                        _cm->chunk_cache.clear();
                        _cm->chunk_cache_bytes = 0;
                    }
                    _cm->chunk_cache_bytes += sz;
                    ++pstats.populations;
                    it = _cm->chunk_cache.emplace(ckey,
                            cached_lance_meta::plain_chunk{std::move(plain.first),
                                                           std::move(plain.second)}).first;
                } else {
                    ++pstats.hits;
                }
                part = format::decode_plain_chunk(lphys_of(_ms.columns[leaf]), pl,
                        it->second.def, it->second.values, ch.values,
                        size_t(page_lo - pg.priority - ch.first_value),
                        size_t(page_hi - pg.priority - ch.first_value));
                break;
            }
            const auto& first = idx->chunks[c_lo];
            const auto& last = idx->chunks[c_hi];
            auto chunk_bytes = co_await tracked_read(
                    pg.buffers[1].offset + first.byte_offset,
                    size_t(last.byte_offset + last.byte_size - first.byte_offset));
            part = format::decode_miniblock_chunks(lphys_of(_ms.columns[leaf]), pl, *idx,
                    c_lo, c_hi - c_lo + 1,
                    std::string_view(chunk_bytes.get(), chunk_bytes.size()),
                    page_lo - pg.priority, page_hi - pg.priority);
            break;
        }
        case format::page_layout::kind::fullzip: {
            const uint64_t zlo = page_lo - pg.priority;
            const uint64_t zhi = page_hi - pg.priority;
            if (pl.val.k == format::chan_enc::kind::variable) {
                if (pg.buffers.size() < 2) {
                    throw std::runtime_error("lc: variable full-zip page needs a repetition index");
                }
                const uint32_t w = format::fullzip_rep_index_width(pg.buffers[1].size, pg.rows);
                auto ents = co_await tracked_read(pg.buffers[1].offset + zlo * w,
                                                  size_t((zhi - zlo + 1) * w));
                auto sv = std::string_view(ents.get(), ents.size());
                const uint64_t a = format::read_rep_index_entry(sv, w, 0);
                const uint64_t b = format::read_rep_index_entry(sv, w, zhi - zlo);
                if (a > b || b > pg.buffers[0].size) {
                    throw std::runtime_error("lc: repetition index out of range");
                }
                auto zipped = co_await tracked_read(pg.buffers[0].offset + a, size_t(b - a));
                part = format::decode_fullzip_variable(pl, zlo, zhi,
                        std::string_view(zipped.get(), zipped.size()));
            } else {
                const size_t stride = (pl.has_def ? 1 : 0) + pl.val.bits / 8;
                auto zipped = co_await tracked_read(pg.buffers[0].offset + zlo * stride,
                                                    size_t((zhi - zlo) * stride));
                part = format::decode_fullzip_fixed(lphys_of(_ms.columns[leaf]), pl, zlo, zhi,
                        std::string_view(zipped.get(), zipped.size()));
            }
            break;
        }
        case format::page_layout::kind::constant:
            part = format::decode_constant(lphys_of(_ms.columns[leaf]), pl,
                                           page_lo - pg.priority, page_hi - pg.priority);
            break;
        }
        // Keep def positional across pages that disagree about having one.
        if (!out.def.empty() || !part.def.empty()) {
            out.def.resize(out.rows(), 0);
            part.def.resize(part.rows(), 0);
        }
        out.def.insert(out.def.end(), part.def.begin(), part.def.end());
        auto app = [](auto& d, auto& s) {
            d.insert(d.end(), std::make_move_iterator(s.begin()), std::make_move_iterator(s.end()));
        };
        app(out.i32, part.i32);
        app(out.i64, part.i64);
        app(out.f64, part.f64);
        app(out.str, part.str);
    }
    if (out.rows() != hi - lo) {
        throw std::runtime_error("lc: row range not fully covered by pages");
    }
    co_return out;
}

future<> lc_reader::init() {
    if (_init) { co_return; }
    _init = true;

    co_await load_meta();

    _cols = columns_of(*_schema);
    _ms = lance::recover_mapped_schema(_cm->fd, _cols);

    _n_pk = _schema->partition_key_size();
    _n_ck = _schema->clustering_key_size();
    _static_base = static_base(*_schema);

    const int64_t acc = int64_t(_cm->fd.num_rows);
    _row_lo = 0;
    _row_hi = acc;

    // A bounded range becomes a bounded ordinal window, via the partition
    // index -- byte-for-byte the pq reader's logic (see the long comments
    // there, especially on the singular upper bound).
    if (_use_index && (_pr->start() || _pr->end()) && _sst->has_component(component_type::Index)) {
        auto ir = _sst->make_index_reader(_permit, {}, use_caching::yes, _pr->is_singular());
        std::exception_ptr ex;
        try {
            bool present = true;
            std::optional<int64_t> singular_end;
            if (_pr->is_singular()) {
                present = co_await ir->advance_lower_and_check_if_present(
                        dht::ring_position_view(_pr->start()->value()));
                if (present && !ir->eof()) {
                    _row_lo = std::min<int64_t>(int64_t(ir->data_file_positions().start), acc);
                    co_await ir->advance_to_next_partition();
                    singular_end = std::min<int64_t>(int64_t(ir->data_file_positions().start), acc);
                }
            } else {
                co_await ir->advance_to(*_pr);
            }
            if (!present) { _sst->get_filter_tracker().add_false_positive(); }
            if (singular_end) {
                _row_hi = *singular_end;
            } else {
                auto pos = present ? ir->data_file_positions() : sstables::data_file_positions_range{0, 0};
                _row_lo = std::min<int64_t>(int64_t(pos.start), acc);
                _row_hi = pos.end ? std::min<int64_t>(int64_t(*pos.end), acc) : acc;
            }
            if (_row_hi < _row_lo) { _row_hi = _row_lo; }
        } catch (...) { ex = std::current_exception(); }
        co_await ir->close();
        if (ex) { std::rethrow_exception(ex); }
    }
    _cursor = _row_lo;
}

future<bool> lc_reader::next_window() {
    if (_cursor >= _row_hi) { co_return false; }
    const bool bounded = _pr->start() || _pr->end();
    const int64_t win = bounded ? point_window_rows : scan_window_rows;
    const uint64_t lo = uint64_t(_cursor);
    const uint64_t hi = uint64_t(std::min(_row_hi, _cursor + win));

    // All leaves in parallel: with the metadata in memory each leaf costs its
    // own small number of reads, and they are independent.
    std::vector<future<format::column_values>> futs;
    futs.reserve(_ms.columns.size());
    for (size_t leaf = 0; leaf < _ms.columns.size(); ++leaf) {
        futs.push_back(read_leaf_rows(leaf, lo, hi));
    }
    auto vals = co_await seastar::when_all_succeed(futs.begin(), futs.end());

    std::vector<pq::format::column_data> colsdata;
    colsdata.reserve(vals.size());
    for (size_t leaf = 0; leaf < vals.size(); ++leaf) {
        colsdata.push_back(to_column_data(_ms.columns[leaf], std::move(vals[leaf])));
    }
    _rows = reassemble(_ms, _cols, colsdata, size_t(hi - lo));
    _pos = 0;
    _cursor = int64_t(hi);
    co_return !_rows.empty();
}

void lc_reader::close_partition() {
    if (!_open) { return; }
    if (_filter && _filter->current_tombstone()) {
        auto res = _filter->apply(position_in_partition_view::after_all_clustered_rows(), {});
        for (auto&& rt : res.rts) {
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
        }
    }
    push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, partition_end()));
    _open = false;
}

void lc_reader::emit_row(const row& r) {
    bool just_opened = false;
    std::vector<value> pk(r.key.begin(), r.key.begin() + long(_n_pk));
    if (!_open || pk != _open_pk) {
        close_partition();
        auto dk = key_of(pk);
        dht::ring_position_comparator cmp(*_schema);
        if (_pr->end() && cmp(dht::ring_position_view(dk), _pr->end()->value()) > 0) {
            _end_of_stream = true;
            _open_pk = std::move(pk);
            _skipping = true;
            return;
        }
        if (!_pr->contains(dk, cmp)) {
            _open_pk = std::move(pk);
            _open = false;
            _skipping = true;
            return;
        }
        tombstone pt;
        if (r.part_del) {
            pt = tombstone(r.part_del->timestamp,
                           gc_clock::time_point(gc_clock::duration(r.part_del->local_deletion_time)));
        }
        _open_dk = dk;
        _filter.emplace(*_schema,
                query::clustering_key_filter_ranges::get_ranges(*_schema, _slice, dk.key()),
                streamed_mutation::forwarding::no);
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit,
                partition_start(std::move(dk), pt)));
        _open_pk = std::move(pk);
        _open = true;
        _skipping = false;
        just_opened = true;
    }
    if (_skipping) { return; }

    if (just_opened) {
        ::row st;
        for (const auto& [k, c] : r.cells) {
            if (k < _static_base) { continue; }
            const column_definition& cdef = _schema->static_column_at(column_id(k - _static_base));
            if (!c.v && c.live) { continue; }
            if (!c.live) {
                auto ldt = gc_clock::time_point(gc_clock::duration(c.local_deletion_time.value_or(0)));
                st.append_cell(column_id(k - _static_base),
                               atomic_cell::make_dead(c.timestamp, ldt));
                continue;
            }
            auto raw = lc_encode(_cols[_n_pk + _n_ck + k].type, *c.v);
            if (c.ttl) {
                auto expiry = gc_clock::time_point(
                        gc_clock::duration(c.local_deletion_time.value_or(0)));
                st.append_cell(column_id(k - _static_base),
                        atomic_cell::make_live(*cdef.type, c.timestamp, bytes_view(raw),
                                               expiry, gc_clock::duration(*c.ttl)));
            } else {
                st.append_cell(column_id(k - _static_base),
                        atomic_cell::make_live(*cdef.type, c.timestamp, bytes_view(raw)));
            }
        }
        for (const auto& [k, cc] : r.collections) {
            if (k < _static_base) { continue; }
            const column_definition& cdef =
                    _schema->static_column_at(column_id(k - _static_base));
            st.append_cell(column_id(k - _static_base),
                    cdef.is_counter() ? build_counter(cc) : build_collection(cdef, cc));
        }
        if (!st.empty()) {
            ::static_row sr(std::move(st));
            if (_filter->apply(sr) == mutation_fragment_filter::result::emit) {
                push_mutation_fragment(mutation_fragment_v2(*_schema, _permit,
                        std::move(sr)));
            }
        }
    }

    if (r.rtc) {
        std::vector<bytes> parts;
        parts.reserve(size_t(std::max<int32_t>(r.rtc->prefix_len, 0)));
        for (int32_t i = 0; i < r.rtc->prefix_len && size_t(i) < _n_ck; ++i) {
            parts.push_back(lc_encode(_cols[_n_pk + size_t(i)].type, r.key[_n_pk + size_t(i)]));
        }
        auto pos = position_in_partition(partition_region(r.rtc->region),
                                         bound_weight(r.rtc->weight),
                                         clustering_key_prefix::from_exploded(*_schema, std::move(parts)));
        tombstone t;
        if (r.rtc->tomb) {
            t = tombstone(r.rtc->tomb->timestamp,
                          gc_clock::time_point(gc_clock::duration(r.rtc->tomb->local_deletion_time)));
        }
        auto res = _filter->apply(pos, t);
        for (auto&& rt : res.rts) {
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
        }
        return;
    }

    if (r.no_ck) { return; }

    std::vector<bytes> ck_parts;
    ck_parts.reserve(_n_ck);
    for (size_t i = 0; i < _n_ck; ++i) {
        ck_parts.push_back(lc_encode(_cols[_n_pk + i].type, r.key[_n_pk + i]));
    }
    auto ck = clustering_key::from_exploded(*_schema, ck_parts);

    auto row_res = _filter->apply(position_in_partition_view::for_key(ck));
    for (auto&& rt : row_res.rts) {
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
    }
    if (row_res.action != mutation_fragment_filter::result::emit) { return; }

    ::row cells;
    for (const auto& [k, c] : r.cells) {
        if (k >= _static_base) { continue; }
        if (!c.v && c.live) { continue; }
        const column_definition& cdef = _schema->regular_column_at(column_id(k));
        if (!c.live) {
            auto ldt = gc_clock::time_point(gc_clock::duration(c.local_deletion_time.value_or(0)));
            cells.append_cell(column_id(k), atomic_cell::make_dead(c.timestamp, ldt));
            continue;
        }
        auto raw = lc_encode(_cols[_n_pk + _n_ck + k].type, *c.v);
        if (c.ttl) {
            auto expiry = gc_clock::time_point(
                    gc_clock::duration(c.local_deletion_time.value_or(0)));
            cells.append_cell(column_id(k),
                    atomic_cell::make_live(*cdef.type, c.timestamp, bytes_view(raw),
                                           expiry, gc_clock::duration(*c.ttl)));
        } else {
            cells.append_cell(column_id(k),
                    atomic_cell::make_live(*cdef.type, c.timestamp, bytes_view(raw)));
        }
    }

    for (const auto& [k, cc] : r.collections) {
        if (k >= _static_base) { continue; }
        const column_definition& cdef = _schema->regular_column_at(column_id(k));
        cells.append_cell(column_id(k),
                cdef.is_counter() ? build_counter(cc) : build_collection(cdef, cc));
    }

    row_marker rm;
    if (r.marker) {
        rm = (r.marker->ttl && r.marker->expiry)
           ? row_marker(r.marker->timestamp,
                        gc_clock::duration(*r.marker->ttl),
                        gc_clock::time_point(gc_clock::duration(*r.marker->expiry)))
           : row_marker(r.marker->timestamp);
    }
    row_tombstone rt;
    if (r.row_del) {
        auto sh = tombstone(r.row_del->timestamp,
                gc_clock::time_point(gc_clock::duration(r.row_del->local_deletion_time)));
        auto reg = r.row_del_regular
                ? tombstone(r.row_del_regular->timestamp,
                        gc_clock::time_point(
                                gc_clock::duration(r.row_del_regular->local_deletion_time)))
                : tombstone();
        rt = row_tombstone(reg, shadowable_tombstone(sh));
    }
    push_mutation_fragment(mutation_fragment_v2(*_schema, _permit,
            clustering_row(std::move(ck), rt, rm, std::move(cells))));
}

mutation_reader make_reader(
        sstables::shared_sstable sst,
        schema_ptr query_schema,
        reader_permit permit,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        tracing::trace_state_ptr,
        streamed_mutation::forwarding fwd,
        mutation_reader::forwarding,
        sstables::read_monitor& mon) {
    // Column projection is deliberately not applied, for the same reason the
    // pq reader records at this exact spot: row existence can depend on any
    // cell, so the slice's column set must not change what is read.
    auto rd = make_mutation_reader<lc_reader>(std::move(sst), std::move(query_schema),
                                              std::move(permit), range, slice, mon, true);
    if (fwd) {
        rd = make_forwardable(std::move(rd));
    }
    return rd;
}

mutation_reader make_full_scan_reader(
        sstables::shared_sstable sst,
        schema_ptr schema,
        reader_permit permit,
        tracing::trace_state_ptr,
        sstables::read_monitor& mon) {
    auto& s_ref = *schema;
    return make_mutation_reader<lc_reader>(std::move(sst), schema,
            std::move(permit), query::full_partition_range, s_ref.full_slice(), mon, false);
}

} // namespace sstables::lance
