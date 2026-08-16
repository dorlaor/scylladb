/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// Reads a Parquet file image back into the column_data the writer consumes,
// which -- combined with schema_mapping::reassemble -- closes the round trip:
// rows -> Parquet -> rows.
//
// Whole-image for now, mirroring the writer. A seastar-native streaming reader
// is a separate piece of work; the point of this one is that nothing written
// can be trusted until it can be read back and compared.

#include "parquet_metadata.hh"
#include "parquet_writer.hh"

#include <span>
#include <vector>

namespace sstables::parquet::format {

// Decode one row group into one column_data per leaf, in schema order.
std::vector<column_data> read_row_group(std::span<const uint8_t> image,
                                        const file_metadata&,
                                        size_t row_group_index);

// The bytes one leaf column needs for a ranged decode. Splitting the dictionary
// page from the data pages is what lets a point read fetch two small extents per
// column instead of the whole row group: the dictionary lives at the head of the
// chunk, the wanted pages live somewhere in the middle, and the pages between
// them never have to be read at all.
struct column_input {
    std::span<const uint8_t> dict;       // dictionary page, header included; may be empty
    std::span<const uint8_t> pages;      // a contiguous run of data pages
    int64_t first_row = 0;               // row index, within the row group, of pages[0]
};

// Decode rows [row_lo, row_hi) from per-column byte spans. One entry per leaf,
// in schema order.
std::vector<column_data> decode_columns(std::span<const column_input>,
                                        const file_metadata&, size_t row_group_index,
                                        int64_t row_lo, int64_t row_hi);

// Decode rows [row_lo, row_hi) of one row group. `base_offset` is the file
// offset that image[0] maps to, so the caller can hand over just the bytes that
// matter instead of the whole file. Pages outside the range are stepped over
// using the V2 header's num_rows without being decompressed -- this is what
// makes a point read cost one page rather than one file.
std::vector<column_data> read_row_range(std::span<const uint8_t> image, int64_t base_offset,
                                        const file_metadata&, size_t row_group_index,
                                        int64_t row_lo, int64_t row_hi);

// Convenience: parse the footer and decode row group 0.
std::vector<column_data> read_file(std::span<const uint8_t> image);

} // namespace sstables::parquet::format
