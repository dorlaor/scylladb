/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Reads a Lance 2.1 file. Two layers:
//
//  * lance_file_view -- parsed metadata (footer, offset tables, schema,
//    ColumnMetadata) over caller-provided bytes. The Scylla reader keeps one
//    of these per sstable (it is the analogue of the parsed Parquet footer)
//    and does its own page-buffer I/O.
//  * read_rows() -- whole-image decode of a row range of one column, used by
//    the standalone tests, the interop suite and full scans over an image
//    already in memory.
//
// Point-read plumbing (which chunk of which page covers row N, and which
// byte range to fetch) is exposed as small pure helpers so the Scylla layer
// can drive its own I/O with them.

#include "lance_encodings.hh"
#include "lance_metadata.hh"

#include <span>

namespace sstables::lance::format {

class lance_file_view {
    std::span<const uint8_t> _image;
    metadata_limits _lim;
    footer _footer;
    file_descriptor _fd;
    std::vector<column_meta> _columns;

public:
    explicit lance_file_view(std::span<const uint8_t> image, metadata_limits lim = {});

    const footer& file_footer() const { return _footer; }
    const file_descriptor& schema() const { return _fd; }
    uint64_t num_rows() const { return _fd.num_rows; }
    size_t num_columns() const { return _columns.size(); }
    const column_meta& column(size_t c) const { return _columns.at(c); }

    std::string_view buffer(const buffer_ref& b) const;

    // Decodes values [lo, hi) of column c. `t` comes from the caller's schema
    // mapping; the file's logical type is not consulted (the CQL mapping is
    // the authority, exactly as in the Parquet reader).
    column_values read_rows(size_t c, lphys t, uint64_t lo, uint64_t hi) const;
};

// Drops rows outside [keep_from, keep_to) from `v`, where `v` starts at row
// `first`. Decoding hands back whole chunks; this trims to the request.
void slice_values(column_values& v, uint64_t first, uint64_t keep_from, uint64_t keep_to);

} // namespace sstables::lance::format
