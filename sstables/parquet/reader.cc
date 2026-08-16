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
#include "readers/from_mutations.hh"
#include "mutation/mutation.hh"
#include "keys/keys.hh"
#include "schema/schema.hh"
#include "types/types.hh"

#include <seastar/core/coroutine.hh>

#include <bit>
#include <cstring>

namespace sstables::parquet {

namespace {

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

// Decode every row group in the image and reassemble the layer-2 rows. The
// mapped_schema is recovered from the footer, not carried over from the writer:
// a reader is handed a file and a CQL schema and nothing else.
std::vector<row> rows_of(std::span<const uint8_t> img, const std::vector<cql_column>& cols) {
    auto fm = format::parse_footer(img);
    auto ms = recover_mapped_schema(fm, cols);

    std::vector<row> out;
    out.reserve(size_t(fm.num_rows));
    for (size_t g = 0; g < fm.row_groups.size(); ++g) {
        auto colsdata = format::read_row_group(img, fm, g);
        auto rows = reassemble(ms, cols, colsdata, size_t(fm.row_groups[g].num_rows));
        out.insert(out.end(), std::make_move_iterator(rows.begin()),
                              std::make_move_iterator(rows.end()));
    }
    return out;
}

// Rows -> mutations. Rows come back in the order they were written, which is
// token order, so a partition is a maximal run of rows sharing a key prefix.
utils::chunked_vector<mutation> mutations_of(const ::schema& s,
                                             const std::vector<cql_column>& cols,
                                             const std::vector<row>& rows) {
    const size_t n_pk = s.partition_key_size();
    const size_t n_ck = s.clustering_key_size();

    utils::chunked_vector<mutation> out;
    std::optional<std::vector<value>> cur_pk;
    std::optional<mutation> m;

    auto flush = [&] { if (m) { out.push_back(std::move(*m)); m.reset(); } };

    for (const auto& r : rows) {
        std::vector<value> pk(r.key.begin(), r.key.begin() + long(n_pk));
        if (!cur_pk || pk != *cur_pk) {
            flush();
            std::vector<bytes> parts;
            for (size_t i = 0; i < n_pk; ++i) { parts.push_back(encode(cols[i].type, pk[i])); }
            m.emplace(s.shared_from_this(),
                      dht::decorate_key(s, partition_key::from_exploded(s, parts)));
            cur_pk = std::move(pk);
        }

        std::vector<bytes> ck_parts;
        for (size_t i = 0; i < n_ck; ++i) {
            ck_parts.push_back(encode(cols[n_pk + i].type, r.key[n_pk + i]));
        }
        auto ck = clustering_key::from_exploded(s, ck_parts);

        // Materialise the row even when every cell is absent, so a row that
        // exists with all-null columns is not silently dropped. Row markers are
        // not yet carried through the shredder, so this row has none -- see
        // docs/dev/parquet-storage-format.md section 11.
        m->partition().clustered_row(s, ck);

        for (const auto& [k, c] : r.cells) {
            if (!c.v && c.live) { continue; }          // absent, not deleted
            const column_definition& cdef = s.regular_column_at(column_id(k));
            if (!c.live) {
                auto ldt = gc_clock::time_point(
                        gc_clock::duration(c.local_deletion_time.value_or(0)));
                m->set_clustered_cell(ck, cdef, atomic_cell::make_dead(c.timestamp, ldt));
                continue;
            }
            auto raw = encode(cols[n_pk + n_ck + k].type, *c.v);
            if (c.ttl) {
                auto expiry = gc_clock::time_point(
                        gc_clock::duration(c.local_deletion_time.value_or(0)));
                m->set_clustered_cell(ck, cdef,
                        atomic_cell::make_live(*cdef.type, c.timestamp, bytes_view(raw),
                                               expiry, gc_clock::duration(*c.ttl)));
            } else {
                m->set_clustered_cell(ck, cdef,
                        atomic_cell::make_live(*cdef.type, c.timestamp, bytes_view(raw)));
            }
        }
    }
    flush();
    return out;
}

// Loads the whole Parquet image on first use and then delegates to an in-memory
// reader over the mutations it decoded. See the header for why this is not the
// final shape.
class pq_reader : public mutation_reader::impl {
    sstables::shared_sstable _sst;
    const dht::partition_range* _pr;
    streamed_mutation::forwarding _fwd;
    sstables::read_monitor& _mon;
    std::optional<mutation_reader> _underlying;

    struct consumer {
        pq_reader* _owner;
        stop_iteration operator()(mutation_fragment_v2&& mf) {
            _owner->push_mutation_fragment(std::move(mf));
            return stop_iteration(_owner->is_buffer_full());
        }
    };

    future<> ensure_loaded() {
        if (_underlying) { co_return; }
        const uint64_t len = _sst->ondisk_data_size();
        auto buf = co_await _sst->data_read(0, len, _permit);
        auto img = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(buf.get()), buf.size());
        auto cols = columns_of(*_schema);
        auto ms = mutations_of(*_schema, cols, rows_of(img, cols));
        _mon.on_read_completed();
        _underlying = make_mutation_reader_from_mutations(
                _schema, _permit, std::move(ms), *_pr, _fwd);
    }

public:
    pq_reader(sstables::shared_sstable sst, schema_ptr s, reader_permit permit,
              const dht::partition_range& pr, streamed_mutation::forwarding fwd,
              sstables::read_monitor& mon)
        : impl(std::move(s), std::move(permit))
        , _sst(std::move(sst)), _pr(&pr), _fwd(fwd), _mon(mon) {}

    future<> fill_buffer() override {
        if (_end_of_stream) { co_return; }
        co_await ensure_loaded();
        co_await _underlying->consume_pausable(consumer{this});
        if (_underlying->is_end_of_stream()) { _end_of_stream = true; }
    }

    future<> next_partition() override {
        clear_buffer_to_next_partition();
        if (is_buffer_empty() && _underlying) { return _underlying->next_partition(); }
        return make_ready_future<>();
    }

    future<> fast_forward_to(const dht::partition_range& pr) override {
        clear_buffer();
        _end_of_stream = false;
        _pr = &pr;
        if (!_underlying) { return make_ready_future<>(); }
        return _underlying->fast_forward_to(pr);
    }

    future<> fast_forward_to(position_range pr) override {
        clear_buffer();
        _end_of_stream = false;
        if (!_underlying) { return make_ready_future<>(); }
        return _underlying->fast_forward_to(std::move(pr));
    }

    future<> close() noexcept override {
        if (!_underlying) { return make_ready_future<>(); }
        return _underlying->close();
    }
};

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
    // The slice is applied by the caller's filtering layers; honouring it here
    // is an optimisation the streaming reader will want, not a correctness
    // requirement for this one.
    (void)slice;
    return make_mutation_reader<pq_reader>(std::move(sst), std::move(query_schema),
                                           std::move(permit), range, fwd, mon);
}

mutation_reader make_full_scan_reader(
        sstables::shared_sstable sst,
        schema_ptr schema,
        reader_permit permit,
        tracing::trace_state_ptr,
        sstables::read_monitor& mon) {
    return make_mutation_reader<pq_reader>(std::move(sst), std::move(schema),
            std::move(permit), query::full_partition_range,
            streamed_mutation::forwarding::no, mon);
}

} // namespace sstables::parquet
