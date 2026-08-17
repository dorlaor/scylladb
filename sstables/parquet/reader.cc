/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/reader.hh"
#include "sstables/parquet/writer_impl.hh"
#include "sstables/parquet/schema_mapping.hh"
#include "sstables/parquet/format/parquet_reader.hh"
#include "sstables/sstables.hh"
#include "sstables/index_reader.hh"
#include "sstables/mutation_fragment_filter.hh"
#include "readers/forwardable.hh"
#include "mutation/mutation_fragment.hh"
#include "mutation/mutation.hh"
#include "keys/keys.hh"
#include "schema/schema.hh"
#include "types/types.hh"

#include <seastar/core/coroutine.hh>

#include <bit>
#include <cstring>

namespace sstables::parquet {

namespace {

// How many rows are decoded at a time. This, plus one row group's compressed
// bytes, is the reader's memory footprint -- neither grows with the sstable,
// which is what R-13 asks for.
constexpr int64_t scan_window_rows = 16384;
// A bounded read usually wants one partition, so it starts small: the cost of
// looping is a page header walk, while the cost of over-decoding is real work.
constexpr int64_t point_window_rows = 512;

// Inverse of the `decode` in writer_impl.cc. Scylla serialises fixed-width
// scalars big-endian, so these rebuild exactly the bytes the shredder took
// apart -- anything that arrived as an opaque BYTE_ARRAY goes back verbatim.
bytes encode(cql_type t, const value& v) {
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

// Streams a pq sstable: footer once, then one row group's bytes at a time,
// decoded in fixed-size row windows and turned straight into fragments.
//
// Two things make the bounded behaviour possible, and both were built for it:
// the index entry written by pq_writer_impl carries a *row ordinal* rather than
// a byte offset (design doc 5.4, option A), and read_row_range() steps over
// pages outside the requested rows using the V2 header's num_rows without
// decompressing them.
class pq_reader : public mutation_reader::impl {
    sstables::shared_sstable _sst;
    const dht::partition_range* _pr;
    // Held by value. The caller's slice can be a temporary -- the reversed path
    // in sstable::make_reader builds one with reverse_slice() -- and a reader
    // outlives the call that made it.
    query::partition_slice _slice;
    sstables::read_monitor& _mon;
    const bool _use_index;

    bool _init = false;
    format::file_metadata _md;
    mapped_schema _ms;
    std::vector<cql_column> _cols;
    size_t _n_pk = 0, _n_ck = 0, _static_base = 0;
    std::vector<int64_t> _rg_start;     // cumulative first row of each row group

    // Ordinal window this read is confined to, from the partition index.
    int64_t _row_lo = 0, _row_hi = 0;

    // Currently loaded row group and its bytes. A scan holds the whole row group
    // (sequential reads are what a scan wants); a bounded read instead holds two
    // small extents per column and never touches the pages in between.
    size_t _cur_rg = size_t(-1);
    temporary_buffer<char> _rg_buf;
    int64_t _rg_base = 0;               // file offset of _rg_buf[0]
    size_t _oi_rg = size_t(-1);
    std::vector<std::optional<format::offset_index>> _oi;

    // Decoded window.
    std::vector<row> _rows;
    size_t _pos = 0;
    int64_t _cursor = 0;                // next row ordinal to decode

    // Partition being assembled across windows.
    std::vector<value> _open_pk;
    std::optional<dht::decorated_key> _open_dk;
    // Applies the clustering slice, and -- the part that cannot be done by
    // filtering rows alone -- clips range tombstone changes to the slice's
    // boundaries, re-opening a range at the start of each range it spans.
    std::optional<mutation_fragment_filter> _filter;
    bool _open = false;
    // True while sitting inside a partition the range excludes.
    bool _skipping = false;

    future<> init();
    future<bool> next_window();         // false at end of the ordinal range
    future<> load_row_group(size_t rg);
    future<> load_offset_indexes(size_t rg);
    // Reads only the pages covering [lo, hi). Falls back to the whole row group
    // when the file carries no OffsetIndex.
    future<std::vector<format::column_data>> decode_paged(size_t rg, int64_t lo, int64_t hi);
    void emit_row(const row& r);
    void close_partition();

    dht::decorated_key key_of(std::span<const value> pk) const {
        std::vector<bytes> parts;
        parts.reserve(_n_pk);
        for (size_t i = 0; i < _n_pk; ++i) { parts.push_back(encode(_cols[i].type, pk[i])); }
        return dht::decorate_key(*_schema, partition_key::from_exploded(*_schema, parts));
    }

public:
    // Always constructed non-forwarding. Intra-partition forwarding is provided
    // by wrapping in make_forwardable() -- see make_reader() -- because seeking
    // by clustering position inside a row group is not implemented, and honouring
    // the interface while ignoring the position range would silently return rows
    // the caller did not ask for.
    pq_reader(sstables::shared_sstable sst, schema_ptr s, reader_permit permit,
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
            // Skip the rest of the open partition's rows.
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
        _init = false;          // recompute the ordinal window for the new range
        _open = false;
        _rows.clear();
        _pos = 0;
        return make_ready_future<>();
    }

    future<> fast_forward_to(position_range) override {
        // Unreachable: this reader is never handed to a caller that forwards.
        // make_reader() wraps it in make_forwardable() instead.
        on_internal_error(sstlog, "pq_reader: intra-partition forwarding should be "
                                  "handled by the forwardable adapter");
    }

    future<> close() noexcept override { return make_ready_future<>(); }
};

future<> pq_reader::init() {
    if (_init) { co_return; }
    _init = true;

    // Footer only: the last 8 bytes give its length, then one bounded read.
    const uint64_t len = _sst->ondisk_data_size();
    if (len < 12) { throw std::runtime_error("pq: data component too small"); }
    auto tail = co_await _sst->data_read(len - 8, 8, _permit);
    uint32_t flen;
    std::memcpy(&flen, tail.get(), 4);
    if (uint64_t(flen) + 12 > len) { throw std::runtime_error("pq: bad footer length"); }
    auto fbuf = co_await _sst->data_read(len - 8 - flen, flen, _permit);
    _md = format::parse_file_metadata(
            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(fbuf.get()), fbuf.size()));

    _cols = columns_of(*_schema);
    _ms = recover_mapped_schema(_md, _cols);
    _n_pk = _schema->partition_key_size();
    _n_ck = _schema->clustering_key_size();
    _static_base = static_base(*_schema);

    _rg_start.clear();
    int64_t acc = 0;
    for (const auto& g : _md.row_groups) { _rg_start.push_back(acc); acc += g.num_rows; }
    _row_lo = 0;
    _row_hi = acc;

    // A bounded range becomes a bounded ordinal window, via the partition index.
    // This is what turns a point read from "decode the file" into "decode a page".
    if (_use_index && (_pr->start() || _pr->end()) && _sst->has_component(component_type::Index)) {
        auto ir = _sst->make_index_reader(_permit, {}, use_caching::yes, _pr->is_singular());
        std::exception_ptr ex;
        try {
            bool present = true;
            if (_pr->is_singular()) {
                // A singular range needs the exact-key lookup, not advance_to():
                // advance_to() positions for a *range* and leaves both bounds at
                // the start for a point, which reads back as an empty window.
                // This is the same call mx makes for a single-partition read.
                present = co_await ir->advance_lower_and_check_if_present(
                        dht::ring_position_view(_pr->start()->value()));
            } else {
                co_await ir->advance_to(*_pr);
            }
            auto pos = present ? ir->data_file_positions() : sstables::data_file_positions_range{0, 0};
            if (!present) { _sst->get_filter_tracker().add_false_positive(); }
            if (getenv("PQDBG")) fprintf(stderr, "[pqdbg] eof=%d data_size=%llu want_tok=%s sum_tok=%s\n", int(ir->eof()), (unsigned long long)_sst->data_size(), _pr->start()->value().token().to_sstring().c_str(), _sst->get_summary().entries.empty()?"-":_sst->get_summary().entries[0].get_token().to_sstring().c_str());
            _row_lo = std::min<int64_t>(int64_t(pos.start), acc);
            _row_hi = pos.end ? std::min<int64_t>(int64_t(*pos.end), acc) : acc;
            if (_row_hi < _row_lo) { _row_hi = _row_lo; }
        } catch (...) { ex = std::current_exception(); }
        co_await ir->close();
        if (ex) { std::rethrow_exception(ex); }
    }
    _cursor = _row_lo;
    _mon.on_read_completed();
}

future<> pq_reader::load_row_group(size_t rg) {
    if (_cur_rg == rg) { co_return; }
    const auto& g = _md.row_groups[rg];
    int64_t lo = std::numeric_limits<int64_t>::max(), hi = 0;
    for (const auto& cc : g.columns) {
        if (!cc.meta) { continue; }
        const auto& cm = *cc.meta;
        const int64_t s = cm.dictionary_page_offset ? *cm.dictionary_page_offset
                                                    : cm.data_page_offset;
        lo = std::min(lo, s);
        hi = std::max(hi, s + cm.total_compressed_size);
    }
    if (lo >= hi) { throw std::runtime_error("pq: empty row group extent"); }
    _rg_buf = co_await _sst->data_read(uint64_t(lo), size_t(hi - lo), _permit);
    _rg_base = lo;
    _cur_rg = rg;
}

future<bool> pq_reader::next_window() {
    _rows.clear();
    _pos = 0;
    if (_cursor >= _row_hi) { co_return false; }

    // Which row group holds _cursor.
    size_t rg = 0;
    while (rg + 1 < _rg_start.size() && _rg_start[rg + 1] <= _cursor) { ++rg; }
    const int64_t rg_end = _rg_start[rg] + _md.row_groups[rg].num_rows;

    const int64_t lo = _cursor;
    const bool bounded = _pr->start() || _pr->end();
    const int64_t win = bounded ? point_window_rows : scan_window_rows;
    const int64_t hi = std::min({rg_end, _row_hi, lo + win});
    if (hi <= lo) { co_return false; }

    std::vector<format::column_data> colsdata;
    if (bounded) {
        colsdata = co_await decode_paged(rg, lo - _rg_start[rg], hi - _rg_start[rg]);
    } else {
        co_await load_row_group(rg);
        auto img = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(_rg_buf.get()), _rg_buf.size());
        colsdata = format::read_row_range(img, _rg_base, _md, rg,
                                          lo - _rg_start[rg], hi - _rg_start[rg]);
    }
    _rows = reassemble(_ms, _cols, colsdata, size_t(hi - lo));
    _cursor = hi;
    co_return true;
}

future<> pq_reader::load_offset_indexes(size_t rg) {
    if (_oi_rg == rg) { co_return; }
    _oi.assign(_md.row_groups[rg].columns.size(), std::nullopt);
    _oi_rg = rg;

    // The per-column OffsetIndex blobs sit together near the end of the file, so
    // one read covers all of them.
    int64_t lo = std::numeric_limits<int64_t>::max(), hi = 0;
    for (const auto& cc : _md.row_groups[rg].columns) {
        if (!cc.offset_index_offset || !cc.offset_index_length) { continue; }
        lo = std::min(lo, *cc.offset_index_offset);
        hi = std::max(hi, *cc.offset_index_offset + *cc.offset_index_length);
    }
    if (lo >= hi) { co_return; }        // file has no page index; caller falls back
    auto buf = co_await _sst->data_read(uint64_t(lo), size_t(hi - lo), _permit);
    auto img = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(buf.get()), buf.size());
    for (size_t c = 0; c < _oi.size(); ++c) {
        const auto& cc = _md.row_groups[rg].columns[c];
        if (!cc.offset_index_offset || !cc.offset_index_length) { continue; }
        try {
            _oi[c] = format::parse_offset_index_blob(
                    img.subspan(size_t(*cc.offset_index_offset - lo), size_t(*cc.offset_index_length)));
        } catch (...) { _oi[c] = std::nullopt; }
    }
}

future<std::vector<format::column_data>> pq_reader::decode_paged(size_t rg, int64_t lo, int64_t hi) {
    co_await load_offset_indexes(rg);
    const auto& g = _md.row_groups[rg];

    bool all = true;
    for (const auto& o : _oi) { if (!o || o->pages.empty()) { all = false; break; } }
    if (!all) {
        // No page index: nothing to seek with, so read the row group whole.
        co_await load_row_group(rg);
        auto img = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(_rg_buf.get()), _rg_buf.size());
        co_return format::read_row_range(img, _rg_base, _md, rg, lo, hi);
    }

    // Two extents per column: the dictionary page at the head of the chunk, and
    // the contiguous run of data pages covering the wanted rows. Everything else
    // in the chunk is never read.
    std::vector<temporary_buffer<char>> held;
    held.reserve(g.columns.size() * 2);
    std::vector<format::column_input> in(g.columns.size());

    for (size_t c = 0; c < g.columns.size(); ++c) {
        const auto& cm = *g.columns[c].meta;
        const auto& pages = _oi[c]->pages;
        const size_t i0 = _oi[c]->page_for_row(lo);
        const size_t i1 = _oi[c]->page_for_row(hi > lo ? hi - 1 : lo);
        if (i0 >= pages.size() || i1 >= pages.size() || i1 < i0) {
            throw std::runtime_error("pq: OffsetIndex does not cover the requested rows");
        }

        if (cm.dictionary_page_offset) {
            const int64_t d0 = *cm.dictionary_page_offset;
            const int64_t d1 = pages.front().offset;    // first data page
            if (d1 > d0) {
                auto b = co_await _sst->data_read(uint64_t(d0), size_t(d1 - d0), _permit);
                in[c].dict = std::span<const uint8_t>(
                        reinterpret_cast<const uint8_t*>(b.get()), b.size());
                held.push_back(std::move(b));
            }
        }
        const int64_t p0 = pages[i0].offset;
        const int64_t p1 = pages[i1].offset + pages[i1].compressed_page_size;
        auto b = co_await _sst->data_read(uint64_t(p0), size_t(p1 - p0), _permit);
        in[c].pages = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(b.get()), b.size());
        in[c].first_row = pages[i0].first_row_index;
        held.push_back(std::move(b));
    }

    co_return format::decode_columns(in, _md, rg, lo, hi);
}

void pq_reader::close_partition() {
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

void pq_reader::emit_row(const row& r) {
    bool just_opened = false;
    std::vector<value> pk(r.key.begin(), r.key.begin() + long(_n_pk));
    if (!_open || pk != _open_pk) {
        close_partition();
        auto dk = key_of(pk);
        // The index gives a lower bound but not always an upper one, and the
        // window it yields is a superset anyway. Rows are in token order, so once
        // a partition sorts past the range's end there is nothing further to
        // find -- that is what keeps a point read from decoding to EOF.
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
        // The partition tombstone rides on every row of the partition, so the
        // first row of the run carries it.
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

    // Static cells were replayed onto every row of the partition; they belong to
    // the partition, not the row, so they come back out as a static_row emitted
    // once, immediately after the partition_start.
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
            auto raw = encode(_cols[_n_pk + _n_ck + k].type, *c.v);
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
        if (!st.empty()) {
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit,
                    static_row(std::move(st))));
        }
    }

    // A range tombstone change: rebuild its bound from the stored prefix length,
    // weight and region, and emit it in place. Everything past prefix_len in the
    // clustering columns is padding the writer put there.
    if (r.rtc) {
        std::optional<clustering_key_prefix> prefix;
        if (r.rtc->prefix_len > 0) {
            std::vector<bytes> parts;
            parts.reserve(size_t(r.rtc->prefix_len));
            for (int32_t i = 0; i < r.rtc->prefix_len && size_t(i) < _n_ck; ++i) {
                parts.push_back(encode(_cols[_n_pk + size_t(i)].type, r.key[_n_pk + size_t(i)]));
            }
            prefix = clustering_key_prefix::from_exploded(*_schema, std::move(parts));
        }
        auto pos = position_in_partition(partition_region(r.rtc->region),
                                         bound_weight(r.rtc->weight), std::move(prefix));
        tombstone t;
        if (r.rtc->tomb) {
            t = tombstone(r.rtc->tomb->timestamp,
                          gc_clock::time_point(gc_clock::duration(r.rtc->tomb->local_deletion_time)));
        }
        // The filter decides what actually goes out: a change outside the slice
        // is swallowed, and one that opens a range spanning into the slice is
        // re-emitted at the slice boundary instead.
        auto res = _filter->apply(pos, t);
        for (auto&& rt : res.rts) {
            push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
        }
        return;
    }

    // A placeholder row exists only to carry the static row or the partition
    // tombstone; it has no clustering row of its own.
    if (r.no_ck) { return; }

    std::vector<bytes> ck_parts;
    ck_parts.reserve(_n_ck);
    for (size_t i = 0; i < _n_ck; ++i) {
        ck_parts.push_back(encode(_cols[_n_pk + i].type, r.key[_n_pk + i]));
    }
    auto ck = clustering_key::from_exploded(*_schema, ck_parts);

    // Honour the query's clustering slice. The reader does not *seek* to it --
    // the ColumnIndex's per-page clustering bounds would let a later version do
    // that -- but it must not emit rows outside it, and any range tombstone the
    // slice boundary cuts has to be re-opened there.
    auto row_res = _filter->apply(position_in_partition_view::for_key(ck));
    for (auto&& rt : row_res.rts) {
        push_mutation_fragment(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
    }
    if (row_res.action != mutation_fragment_filter::result::emit) { return; }

    ::row cells;
    for (const auto& [k, c] : r.cells) {
        if (k >= _static_base) { continue; }       // static: already emitted above
        if (!c.v && c.live) { continue; }          // absent, not deleted
        const column_definition& cdef = _schema->regular_column_at(column_id(k));
        if (!c.live) {
            auto ldt = gc_clock::time_point(gc_clock::duration(c.local_deletion_time.value_or(0)));
            cells.append_cell(column_id(k), atomic_cell::make_dead(c.timestamp, ldt));
            continue;
        }
        auto raw = encode(_cols[_n_pk + _n_ck + k].type, *c.v);
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
        rt = row_tombstone(tombstone(r.row_del->timestamp,
                gc_clock::time_point(gc_clock::duration(r.row_del->local_deletion_time))));
    }
    push_mutation_fragment(mutation_fragment_v2(*_schema, _permit,
            clustering_row(std::move(ck), rt, rm, std::move(cells))));
}

} // namespace

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
    // Column projection is not pushed down yet -- that is where the big scan win
    // lives (design doc 10.1c) and is tracked separately. The *row* slice is
    // honoured, in pq_reader::emit_row.
    auto rd = make_mutation_reader<pq_reader>(std::move(sst), std::move(query_schema),
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
    // A full scan wants everything, so the schema's full slice is the right one.
    auto& s_ref = *schema;
    return make_mutation_reader<pq_reader>(std::move(sst), schema,
            std::move(permit), query::full_partition_range, s_ref.full_slice(), mon, false);
}

} // namespace sstables::parquet
