/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

// The bridge between the CQL <-> columnar mapping (sstables/parquet/
// schema_mapping.hh -- the folding layer is format-agnostic and shared, see
// docs/dev/lance-storage-format.md 3.1) and the Lance codec: leaf specs from
// a mapped_schema, batches to and from the Parquet layer's column_data, and
// mapped-schema recovery from a Lance file's own schema.
//
// The two value models differ in exactly two ways, both handled here and
// nowhere else:
//  * definition semantics are inverted: Parquet's def == max_def means
//    "present" while Lance's def 0 means "valid";
//  * Parquet's column_data stores only the present values (sparse), Lance's
//    column_values materialises a slot for every row (dense).

#include "sstables/lance/format/lance_reader.hh"
#include "sstables/lance/format/lance_writer.hh"
#include "sstables/parquet/schema_mapping.hh"

namespace sstables::lance {

namespace pq = sstables::parquet;

// Leaf specs for the Lance writer, in mapped_schema leaf order. Throws
// std::invalid_argument if the mapping contains a repeated leaf (a non-frozen
// collection or counter) -- out of scope for the lc format, and rejected at
// DDL time; this is the backstop.
std::vector<format::lance_column_spec> specs_of(const pq::mapped_schema& ms);

// One shredded batch, Parquet layout -> Lance layout (sparse -> dense).
std::vector<format::column_values> to_lance_batch(const pq::mapped_schema& ms,
                                                  std::vector<pq::format::column_data> cols);

// One decoded leaf, Lance layout -> Parquet layout (dense -> sparse), ready
// for pq::reassemble(). `spec` is the mapped_schema leaf it belongs to.
pq::format::column_data to_column_data(const pq::format::column_spec& spec,
                                       format::column_values vals);

// The physical type of a mapped leaf, in Lance terms.
format::lphys lphys_of(const pq::format::column_spec& spec);

// Rebuild the mapped_schema of a Lance file we did not write, from its
// FileDescriptor -- the Lance twin of pq::recover_mapped_schema, sharing
// recover_mapped_schema_from_leaves underneath.
pq::mapped_schema recover_mapped_schema(const format::file_descriptor& fd,
                                        const std::vector<pq::cql_column>& cols);

} // namespace sstables::lance
