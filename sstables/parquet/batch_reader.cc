/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/batch_reader.hh"
#include "sstables/parquet/format/encryption.hh"
#include "sstables/parquet/writer_impl.hh"
#include "sstables/sstables.hh"
#include "schema/schema.hh"

#include <seastar/core/coroutine.hh>

#include <cstring>
#include <limits>
#include <stdexcept>

namespace sstables::parquet {

namespace {

class pq_batch_reader final : public batch_reader {
    shared_sstable _sst;
    schema_ptr _schema;
    reader_permit _permit;

    bool _init = false;
    format::file_metadata _md;
    std::vector<cql_column> _cols;
    mapped_schema _ms;
    // Cumulative first row of each group, so a batch can say where it starts without the consumer
    // re-deriving it.
    std::vector<int64_t> _rg_start;
    size_t _next_rg = 0;

public:
    pq_batch_reader(shared_sstable sst, schema_ptr s, reader_permit permit)
        : _sst(std::move(sst)), _schema(std::move(s)), _permit(std::move(permit)) {}

    const mapped_schema& schema_mapping() const override { return _ms; }
    const std::vector<cql_column>& columns() const override { return _cols; }

    future<> close() override { return make_ready_future<>(); }

    future<std::optional<column_batch>> next() override {
        if (!_init) { co_await init(); }
        if (_next_rg >= _md.row_groups.size()) { co_return std::nullopt; }
        const size_t rg = _next_rg++;
        const auto& g = _md.row_groups[rg];

        // The group's own extent: from the first byte of its first chunk to the last byte of its
        // last. Chunks of a group are contiguous, so this is one sequential read -- the same shape
        // the mutation reader's streaming path uses, and the reason a scan is not I/O-bound.
        int64_t lo = std::numeric_limits<int64_t>::max(), hi = 0;
        for (const auto& cc : g.columns) {
            if (!cc.meta) { throw std::runtime_error("pq batch: column chunk without metadata"); }
            const auto& cm = *cc.meta;
            const int64_t start = cm.dictionary_page_offset ? *cm.dictionary_page_offset
                                                            : cm.data_page_offset;
            lo = std::min(lo, start);
            hi = std::max(hi, start + cm.total_compressed_size);
        }
        if (lo >= hi) { co_return std::nullopt; }

        auto buf = co_await _sst->data_read(uint64_t(lo), size_t(hi - lo), _permit);
        auto image = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(buf.get()), buf.size());

        column_batch out;
        out.first_row = _rg_start[rg];
        out.rows = g.num_rows;
        out.columns = format::read_row_range(image, lo, _md, rg, 0, g.num_rows);
        co_return std::move(out);
    }

private:
    future<> init() {
        _init = true;
        if (_sst->get_version() != sstable_version_types::pq) {
            throw std::runtime_error("pq batch: not a parquet sstable");
        }
        const uint64_t len = _sst->ondisk_data_size();
        if (len < 12) { throw std::runtime_error("pq batch: file too short for a footer"); }

        auto tail = co_await _sst->data_read(len - 8, 8, _permit);
        uint32_t flen;
        std::memcpy(&flen, tail.get(), 4);
        if (uint64_t(flen) + 12 > len) { throw std::runtime_error("pq batch: bad footer length"); }
        if (std::memcmp(tail.get() + 4, format::magic_encrypted, 4) == 0) {
            // Refused rather than attempted. Reading an encrypted file needs the key provider, the
            // per-column key resolution and the AAD construction that pq_reader does; a batch
            // reader that quietly returned the ciphertext would be worse than one that does not
            // open the file at all.
            throw std::runtime_error(
                    "pq batch: file has an encrypted footer; the batch reader does not support "
                    "encryption yet");
        }

        auto raw = co_await _sst->data_read(len - 8 - flen, flen, _permit);
        auto blob = std::span<const uint8_t>(
                reinterpret_cast<const uint8_t*>(raw.get()), raw.size());
        // Eager: this reader decodes whole row groups in order, so every group's column metadata
        // is wanted. The lazy mode pq_reader uses exists to keep a *point* read's footer cost
        // independent of file size, which is not this reader's problem.
        _md = format::parse_file_metadata(blob, {}, format::semantic_check::yes,
                                          format::metadata_mode::eager);

        _cols = columns_of(*_schema);
        _ms = recover_mapped_schema(_md, _cols);

        _rg_start.resize(_md.row_groups.size() + 1, 0);
        for (size_t i = 0; i < _md.row_groups.size(); ++i) {
            _rg_start[i + 1] = _rg_start[i] + _md.row_groups[i].num_rows;
        }
        co_return;
    }
};

} // namespace

std::unique_ptr<batch_reader> make_batch_reader(shared_sstable sst, schema_ptr s,
                                                reader_permit permit) {
    return std::make_unique<pq_batch_reader>(std::move(sst), std::move(s), std::move(permit));
}

} // namespace sstables::parquet
