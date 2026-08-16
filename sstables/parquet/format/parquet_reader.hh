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

// Convenience: parse the footer and decode row group 0.
std::vector<column_data> read_file(std::span<const uint8_t> image);

} // namespace sstables::parquet::format
