/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Assembles a Lance 2.1 file from per-column value batches. Columns buffer
// independently and flush a page whenever their own buffer crosses the page
// target -- there are no row groups, which is the point of the format (see
// docs/dev/lance-storage-format.md 1). The writer pushes bytes through a
// caller-supplied sink so the Scylla layer can stream into an sstable Data
// component while unit tests collect into a string.

#include "lance_encodings.hh"
#include "lance_metadata.hh"

#include <functional>
#include <map>
#include <string>
#include <vector>

namespace sstables::lance::format {

struct lance_column_spec {
    std::string name;
    lphys type = lphys::i64;
    bool nullable = true;
    // Arrow-ish logical type ("int64", "string", "timestamp:ms:-", ...).
    // Defaulted from `type` when empty; the CQL mapping sets it so external
    // readers see real types.
    std::string logical_type;
    std::map<std::string, std::string> field_metadata;
};

struct writer_options {
    // Per-column page flush threshold, in buffered value bytes. The format
    // recommends >= 8 MiB pages; random access inside a page is chunk- or
    // value-grained, so large pages do not hurt point reads.
    size_t page_target_bytes = 8u << 20;
    encode_options enc{};
};

class lance_file_writer {
public:
    using sink = std::function<void(std::string_view)>;

    lance_file_writer(std::vector<lance_column_spec> specs, writer_options opt, sink out);

    // Appends one batch: exactly one column_values per column, all with the
    // same row count. Null slots must be materialised (zero / empty string)
    // per the column_values contract.
    void add_batch(std::vector<column_values> cols);

    // Flushes every buffered column, writes the schema global buffer, the
    // column metadata blocks, the offset tables and the footer. The writer is
    // unusable afterwards.
    void finish(const std::map<std::string, std::string>& schema_metadata = {});

    uint64_t rows_written() const { return _rows; }
    uint64_t bytes_written() const { return _offset; }
    size_t num_columns() const { return _specs.size(); }

private:
    struct pending {
        column_values vals;
        uint64_t first_row = 0;   // priority of the page this will become
        size_t buffered_bytes = 0;
    };

    std::vector<lance_column_spec> _specs;
    writer_options _opt;
    sink _out;
    uint64_t _offset = 0;
    uint64_t _rows = 0;
    std::vector<pending> _pending;
    std::vector<column_meta> _meta;
    bool _finished = false;

    void write_raw(std::string_view s);
    void align_to(size_t alignment);
    void flush_column(size_t c);
};

} // namespace sstables::lance::format
