/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/writer_impl.hh"
#include "sstables/parquet/encryption_keys.hh"

#include "exceptions/exceptions.hh"

#include "mutation/mutation_fragment.hh"
#include "schema/schema.hh"
#include "types/types.hh"
#include "sstables/sstables.hh"
#include "sstables/storage.hh"

#include <seastar/core/fstream.hh>

#include "sstables/writer.hh"
#include "sstables/storage.hh"
#include "keys/keys.hh"
#include "dht/i_partitioner.hh"
#include "sstables/mx/writer.hh"
#include "mutation/collection_mutation.hh"
#include "mutation/counters.hh"
#include "types/collection.hh"

#include <cstdlib>
#include <cstring>

namespace sstables::parquet {

namespace {

// Scylla serialises fixed-width scalars big-endian, so the physical mapping can
// read them straight out of the cell without going through deserialize().
// Anything not handled here keeps its serialised form and travels as an opaque
// BYTE_ARRAY, which is lossless but gives up type-specific encoding.
} // namespace

cql_type cql_type_of(const abstract_type& t) {
    if (&t == int32_type.get())                            { return cql_type::int32; }
    if (&t == long_type.get())                             { return cql_type::bigint; }
    if (&t == timestamp_type.get())                        { return cql_type::timestamp; }
    if (&t == double_type.get())                           { return cql_type::dbl; }
    if (&t == utf8_type.get() || &t == ascii_type.get())   { return cql_type::text; }
    return cql_type::blob;
}

namespace {

int64_t be64(const bytes_view& b) {
    uint64_t v = 0;
    const size_t n = std::min<size_t>(8, b.size());
    for (size_t i = 0; i < n; ++i) { v = (v << 8) | uint8_t(b[i]); }
    return int64_t(v);
}
int32_t be32(const bytes_view& b) {
    uint32_t v = 0;
    const size_t n = std::min<size_t>(4, b.size());
    for (size_t i = 0; i < n; ++i) { v = (v << 8) | uint8_t(b[i]); }
    return int32_t(v);
}

// A non-frozen collection as the mapping sees it: the collection-level tombstone
// plus one entry per (key, cell) pair. Keys and values stay serialised -- the
// mapping treats both as opaque bytes, which is what makes one code path work for
// sets, lists, maps and non-frozen UDTs alike.
// A counter cell, in the collection representation: one element per shard, keyed
// by the shard's id, with its value and logical clock packed into the element
// value. Counters are atomic cells, so without this they would be stored as an
// opaque blob and lose the shard structure that makes them mergeable.
//
// The cell's own timestamp is repeated on every element -- it is the same for all
// of them, so it costs nothing once compressed -- and a dead counter cell becomes
// an element-less collection carrying the deletion in the collection tombstone
// slot, which is how absent is told apart from deleted on the way back.
static collection_cell read_counter_cell(const atomic_cell_view& av) {
    collection_cell cc;
    if (!av.is_live()) {
        cc.tomb = deletion_info{av.timestamp(),
                int32_t(av.deletion_time().time_since_epoch().count())};
        return cc;
    }
    counter_cell_view ccv(av);
    for (auto&& cs : ccv.shards()) {
        const auto u = cs.id().uuid();
        collection_element e;
        e.key = pack_i64_pair(u.get_most_significant_bits(), u.get_least_significant_bits());
        e.value = pack_i64_pair(cs.value(), cs.logical_clock());
        e.timestamp = av.timestamp();
        cc.elements.push_back(std::move(e));
    }
    return cc;
}

collection_cell read_collection_mutation(const atomic_cell_or_collection& acoc) {
    auto cmv = acoc.as_collection_mutation();
    collection_cell cc;
    if (cmv.tomb()) {
        cc.tomb = deletion_info{cmv.tomb().timestamp,
                int32_t(cmv.tomb().deletion_time.time_since_epoch().count())};
    }
    for (auto&& kv : cmv) {
        auto kb = linearized(kv.first);
        collection_element e;
        e.key.assign(reinterpret_cast<const char*>(kb.data()), kb.size());
        e.timestamp = kv.second.timestamp();
        if (kv.second.is_live()) {
            auto lv = kv.second.value().linearize();
            e.value = std::string(reinterpret_cast<const char*>(lv.data()), lv.size());
            if (kv.second.is_live_and_has_ttl()) {
                e.ttl = int32_t(kv.second.ttl().count());
                e.local_deletion_time = int32_t(kv.second.expiry().time_since_epoch().count());
            }
        } else {
            e.local_deletion_time =
                    int32_t(kv.second.deletion_time().time_since_epoch().count());
        }
        cc.elements.push_back(std::move(e));
    }
    return cc;
}

value decode(cql_type t, bytes_view b) {
    switch (t) {
    case cql_type::int32:     return be32(b);
    case cql_type::bigint:
    case cql_type::timestamp: return be64(b);
    case cql_type::dbl: {
        uint64_t bits = uint64_t(be64(b));
        double d;
        std::memcpy(&d, &bits, 8);
        return d;
    }
    default:
        return std::string(reinterpret_cast<const char*>(b.data()), b.size());
    }
}

// Key components arrive already serialised, one per key column.
void explode_key(const std::vector<bytes>& parts,
                 const std::vector<cql_type>& types,
                 std::vector<value>& out) {
    for (size_t i = 0; i < types.size(); ++i) {
        out.push_back(i < parts.size() ? decode(types[i], bytes_view(parts[i]))
                                       : decode(types[i], bytes_view()));
    }
}

// Anything the shredder cannot represent must stop the write. Dropping a
// fragment here does not corrupt the file -- it produces a *valid* Parquet file
// that is quietly missing data, which is the worse failure: a dropped partition
// tombstone resurrects deleted rows. Refusing is recoverable; silence is not.
[[noreturn]] void unrepresentable(const char* what) {
    throw std::runtime_error(
            std::string("pq: cannot represent ") + what + " yet; this sstable would "
            "silently lose data. See docs/dev/parquet-storage-format.md section 11.");
}

} // namespace

std::vector<cql_column> columns_of(const ::schema& s) {
    std::vector<cql_column> cols;
    for (const auto& c : s.partition_key_columns()) {
        cols.push_back({c.name_as_text(), cql_type_of(*c.type), column_kind::partition_key});
    }
    for (const auto& c : s.clustering_key_columns()) {
        cols.push_back({c.name_as_text(), cql_type_of(*c.type), column_kind::clustering_key});
    }
    for (const auto& c : s.regular_columns()) {
        cql_column col{c.name_as_text(), cql_type_of(*c.type), column_kind::regular};
        col.multi_cell = !c.is_atomic() || c.is_counter();
        col.counter = c.is_counter();
        cols.push_back(std::move(col));
    }
    // Static columns ride along as ordinary value columns, appended after the
    // regular ones. That gets them the whole cell machinery -- timestamps, TTLs,
    // the divergence channel -- for free, and costs nothing on disk because a
    // static value is constant within its partition and so compresses away.
    // The reader splits them back out by index; see static_base().
    for (const auto& c : s.static_columns()) {
        cql_column col{"__s_" + c.name_as_text(), cql_type_of(*c.type), column_kind::regular};
        col.multi_cell = !c.is_atomic() || c.is_counter();
        cols.push_back(std::move(col));
    }
    return cols;
}

// ------------------------------------------------------- the `parquet = {...}` property
namespace {

size_t parse_count(const sstring& key, const sstring& v, size_t lo, size_t hi) {
    size_t out = 0;
    try {
        size_t pos = 0;
        unsigned long long n = std::stoull(std::string(v), &pos);
        if (pos != v.size()) { throw std::invalid_argument("trailing"); }
        out = size_t(n);
    } catch (...) {
        throw exceptions::configuration_exception(
                seastar::format("Invalid value '{}' for '{}' in the 'parquet' option; expected a "
                       "whole number", v, key));
    }
    if (out < lo || out > hi) {
        throw exceptions::configuration_exception(
                seastar::format("'{}' must be between {} and {} (got {})", key, lo, hi, out));
    }
    return out;
}

// Accepts a plain byte count or a KiB/MiB/GiB suffix, as design doc 8.2 shows.
size_t parse_bytes(const sstring& key, const sstring& v, size_t lo, size_t hi) {
    static const std::pair<const char*, size_t> units[] = {
        {"GiB", 1024ull * 1024 * 1024}, {"MiB", 1024 * 1024}, {"KiB", 1024},
    };
    for (auto [suffix, mult] : units) {
        const size_t sl = std::strlen(suffix);
        if (v.size() > sl && v.compare(v.size() - sl, sl, suffix) == 0) {
            auto head = v.substr(0, v.size() - sl);
            return parse_count(key, head, (lo + mult - 1) / mult, hi / mult) * mult;
        }
    }
    return parse_count(key, v, lo, hi);
}

} // namespace

namespace {

// The CQL enum to Parquet's. `dictionary` becomes RLE_DICTIONARY, the only dictionary encoding this
// writer emits -- PLAIN_DICTIONARY is the deprecated v1 spelling and nothing here produces it.
format::encoding to_format_encoding(parquet_parameters::column_encoding e) {
    using ce = parquet_parameters::column_encoding;
    switch (e) {
    case ce::plain:                   return format::encoding::plain;
    case ce::dictionary:              return format::encoding::rle_dictionary;
    case ce::delta_binary_packed:     return format::encoding::delta_binary_packed;
    case ce::delta_byte_array:        return format::encoding::delta_byte_array;
    case ce::delta_length_byte_array: return format::encoding::delta_length_byte_array;
    case ce::byte_stream_split:       return format::encoding::byte_stream_split;
    case ce::automatic:               break;
    }
    return format::encoding::plain;   // unreachable: 'auto' is filtered before this is called
}

} // namespace

std::optional<parquet_parameters::column_encoding>
parquet_parameters::parse_column_encoding(std::string_view v) {
    using ce = column_encoding;
    // 'dictionary' rather than 'rle_dictionary': the Parquet name is an implementation detail of the
    // index stream, and 'plain_dictionary' is the deprecated v1 spelling. One name for the concept.
    if (v == "auto")                    { return ce::automatic; }
    if (v == "plain")                   { return ce::plain; }
    if (v == "dictionary")              { return ce::dictionary; }
    if (v == "delta_binary_packed")     { return ce::delta_binary_packed; }
    if (v == "delta_byte_array")        { return ce::delta_byte_array; }
    if (v == "delta_length_byte_array") { return ce::delta_length_byte_array; }
    if (v == "byte_stream_split")       { return ce::byte_stream_split; }
    return std::nullopt;
}

const char* parquet_parameters::to_string(column_encoding e) {
    using ce = column_encoding;
    switch (e) {
    case ce::automatic:               return "auto";
    case ce::plain:                   return "plain";
    case ce::dictionary:              return "dictionary";
    case ce::delta_binary_packed:     return "delta_binary_packed";
    case ce::delta_byte_array:        return "delta_byte_array";
    case ce::delta_length_byte_array: return "delta_length_byte_array";
    case ce::byte_stream_split:       return "byte_stream_split";
    }
    return "auto";
}

bool parquet_parameters::applies_to(column_encoding e, cql_type t) {
    using ce = column_encoding;
    // PLAIN, a dictionary and 'auto' are legal for every physical type. The rest are type-specific,
    // and rejecting the mismatch at DDL time is the whole point: an encoding the writer would have to
    // ignore is a setting that lies, and one it would have to fail on is an outage.
    switch (e) {
    case ce::automatic:
    case ce::plain:
    case ce::dictionary:
        return true;
    case ce::delta_binary_packed:
        // Integer deltas. Timestamps are int64 underneath, which is what makes them the best case.
        return t == cql_type::int32 || t == cql_type::bigint || t == cql_type::timestamp;
    case ce::delta_byte_array:
    case ce::delta_length_byte_array:
        return t == cql_type::text || t == cql_type::blob;
    case ce::byte_stream_split:
        // Measured to *cost* 55 % on real doubles (design doc 10.3f). Accepted because a column
        // whose values do not repeat can still benefit, and refusing a legal Parquet encoding on the
        // strength of one corpus would be overreach -- but it is never chosen automatically.
        return t == cql_type::dbl;
    }
    return false;
}

parquet_parameters::parquet_parameters(const std::map<sstring, sstring>& opts) {
    for (const auto& [k, v] : opts) {
        if (k == ROW_GROUP_ROWS) {
            _cfg.row_group_rows = parse_count(k, v, min_row_group_rows, max_row_group_rows);
        } else if (k == ROW_GROUP_BUFFER_BYTES) {
            _cfg.row_group_buffer_bytes = parse_bytes(k, v, min_buffer_bytes, max_buffer_bytes);
        } else if (k == PAGE_ROWS) {
            // A page is the unit a point read decodes, so the same reasoning as row
            // groups applies one level down.
            _cfg.wopt.page_values = parse_count(k, v, 128, 1'000'000);
        } else if (k == COMPRESSION) {
            // Only what the writer can actually emit. See the note on the class.
            if (v == "zstd") {
                _cfg.wopt.compression = format::codec::zstd;
            } else if (v == "none") {
                _cfg.wopt.compression = format::codec::uncompressed;
            } else {
                throw exceptions::configuration_exception(
                        seastar::format("Unsupported 'compression' value '{}' in the 'parquet' "
                               "option; supported: none, zstd", v));
            }
        } else if (k == COMPRESSION_LEVEL) {
            _cfg.wopt.zstd_level = int(parse_count(k, v, 1, 22));
        } else if (k == DICTIONARY) {
            // Which columns may be dictionary-encoded. 'text' is the default: strings
            // benefit and numerics cost more point-read latency than they save disk
            // (+10.5% for -3.9%, see writer_options::numeric_dictionary). 'all' is for a
            // bottom tier that is scanned rather than point-read.
            if (v == "text") {
                _cfg.wopt.use_dictionary = true;
                _cfg.wopt.numeric_dictionary = false;
            } else if (v == "all") {
                _cfg.wopt.use_dictionary = true;
                _cfg.wopt.numeric_dictionary = true;
            } else if (v == "none") {
                _cfg.wopt.use_dictionary = false;
                _cfg.wopt.numeric_dictionary = false;
            } else {
                throw exceptions::configuration_exception(
                        seastar::format("Unsupported 'dictionary' value '{}' in the 'parquet' "
                                        "option; supported: text, all, none", v));
            }
        } else if (k == METADATA_FOLDING) {
            if (v == "auto" || v == "row") {
                _cfg.level = folding_level::row_folded;
            } else if (v == "verbatim") {
                _cfg.level = folding_level::verbatim;
            } else if (v == "uniform") {
                _cfg.level = folding_level::uniform;
            } else {
                // 'logical' (L3) is export-only: it discards write times and TTLs, so it
                // must never be reachable as a storage setting.
                throw exceptions::configuration_exception(
                        seastar::format("Unsupported 'metadata_folding' value '{}' in the 'parquet' "
                               "option; supported: auto, verbatim, row, uniform", v));
            }
        } else if (k == ENCRYPTION) {
            if (v == "none") {
                _cfg.encryption_key_id = "";
            } else if (v == "aes_gcm_v1") {
                _cfg.encryption_algo = format::cipher::aes_gcm_v1;
            } else if (v == "aes_gcm_ctr_v1") {
                // Page *bodies* are AES-CTR and carry no authentication tag, so tampering with
                // one is not detected by the format. It exists because it is measurably faster
                // and because other writers produce it, not because it is a good default.
                _cfg.encryption_algo = format::cipher::aes_gcm_ctr_v1;
            } else {
                throw exceptions::configuration_exception(seastar::format(
                        "Unsupported 'encryption' value '{}' in the 'parquet' option; supported: "
                        "none, aes_gcm_v1, aes_gcm_ctr_v1", v));
            }
        } else if (k == ENCRYPTION_KEY) {
            _cfg.encryption_key_id = v;
        } else if (k == ENCRYPTION_KEY_METADATA) {
            auto f = parse_key_metadata_format(v);
            if (!f) {
                throw exceptions::configuration_exception(seastar::format(
                        "Unsupported 'encryption_key_metadata' value '{}' in the 'parquet' option; "
                        "supported: provider, parquet_kms", v));
            }
            _cfg.encryption_key_metadata = *f;
        } else if (k.size() > std::strlen(ENCODING_PREFIX)
                   && k.compare(0, std::strlen(ENCODING_PREFIX), ENCODING_PREFIX) == 0) {
            const sstring col = k.substr(std::strlen(ENCODING_PREFIX));
            if (col.empty()) {
                throw exceptions::configuration_exception(
                        "The 'encoding.' sub-option needs a column name, e.g. 'encoding.my_col'");
            }
            auto enc = parse_column_encoding(v);
            if (!enc) {
                throw exceptions::configuration_exception(seastar::format(
                        "Unsupported '{}' value '{}' in the 'parquet' option; supported: "
                        "auto, plain, dictionary, delta_binary_packed, delta_byte_array, "
                        "delta_length_byte_array, byte_stream_split", k, v));
            }
            _column_encodings.emplace(col, *enc);
            // Translated once, here, so the writer and the mapping only ever deal in Parquet's own
            // enum. `auto` records the user's intent for DESCRIBE but contributes no hint, which is
            // what makes 'auto' the way to cancel an override in an ALTER.
            if (*enc != column_encoding::automatic) {
                _cfg.column_encodings[std::string(col)] = to_format_encoding(*enc);
            }
        } else {
            throw exceptions::configuration_exception(
                    seastar::format("Unknown sub-option '{}' for the 'parquet' option; supported: "
                                    "row_group_rows, row_group_buffer_bytes, page_rows, compression, "
                                    "compression_level, metadata_folding, dictionary, encryption, "
                                    "encryption_key, encryption_key_metadata, and "
                                    "'encoding.<column>'", k));
        }
    }
}

std::map<sstring, sstring> parquet_parameters::to_map() const {
    const pq_writer_config def;
    std::map<sstring, sstring> m;
    if (_cfg.row_group_rows != def.row_group_rows) {
        m[ROW_GROUP_ROWS] = seastar::format("{}", _cfg.row_group_rows);
    }
    if (_cfg.row_group_buffer_bytes != def.row_group_buffer_bytes) {
        m[ROW_GROUP_BUFFER_BYTES] = seastar::format("{}", _cfg.row_group_buffer_bytes);
    }
    if (_cfg.wopt.page_values != def.wopt.page_values) {
        m[PAGE_ROWS] = seastar::format("{}", _cfg.wopt.page_values);
    }
    if (_cfg.wopt.compression != def.wopt.compression) {
        m[COMPRESSION] = _cfg.wopt.compression == format::codec::zstd ? "zstd" : "none";
    }
    if (_cfg.wopt.zstd_level != def.wopt.zstd_level) {
        m[COMPRESSION_LEVEL] = seastar::format("{}", _cfg.wopt.zstd_level);
    }
    if (_cfg.wopt.use_dictionary != def.wopt.use_dictionary ||
        _cfg.wopt.numeric_dictionary != def.wopt.numeric_dictionary) {
        m[DICTIONARY] = !_cfg.wopt.use_dictionary ? "none"
                      : (_cfg.wopt.numeric_dictionary ? "all" : "text");
    }
    if (_cfg.level != def.level) {
        // The user-facing vocabulary, not to_string()'s internal "L0"/"L1"/"L2". These
        // have to be the words the parser accepts or the property does not survive a
        // round trip through persistence -- which is exactly how this was caught.
        switch (_cfg.level) {
        case folding_level::verbatim:   m[METADATA_FOLDING] = "verbatim"; break;
        case folding_level::row_folded: m[METADATA_FOLDING] = "row";      break;
        case folding_level::uniform:    m[METADATA_FOLDING] = "uniform";  break;
        default: break;   // L3 is export-only and unreachable as a stored setting
        }
    }
    // Overrides are round-tripped verbatim, and unlike the scalar options they are *not* elided when
    // they equal the default: an explicit 'auto' is how a user cancels an override in an ALTER, and
    // dropping it from DESCRIBE would make the schema unreproducible.
    for (const auto& [col, enc] : _column_encodings) {
        m[sstring(ENCODING_PREFIX) + col] = to_string(enc);
    }
    // The key *id* round-trips; there is no key material here to leak. The algorithm is only
    // emitted alongside it, because 'encryption' without a key is meaningless.
    if (!_cfg.encryption_key_id.empty()) {
        m[ENCRYPTION] = _cfg.encryption_algo == format::cipher::aes_gcm_v1
                      ? "aes_gcm_v1" : "aes_gcm_ctr_v1";
        m[ENCRYPTION_KEY] = _cfg.encryption_key_id;
        if (_cfg.encryption_key_metadata != key_metadata_format::provider) {
            // Qualified: the member to_string(column_encoding) would otherwise hide it.
            m[ENCRYPTION_KEY_METADATA] =
                    sstables::parquet::to_string(_cfg.encryption_key_metadata);
        }
    }
    return m;
}

// ---------------------------------------------------------------- shredder
fragment_shredder::fragment_shredder(const ::schema& s)
    : _schema(s), _cols(columns_of(s)) {
    _n_pk = s.partition_key_size();
    _n_ck = s.clustering_key_size();
    _static_base = static_base(s);
}

void fragment_shredder::set_partition_tombstone(tombstone t) {
    _part_del = deletion_info{t.timestamp,
                              int32_t(t.deletion_time.time_since_epoch().count())};
}

void fragment_shredder::new_partition(const dht::decorated_key& dk) {
    _pk.clear();
    _part_del.reset();
    _static_cells.clear();
    _static_collections.clear();
    _saw_clustering_row = false;
    std::vector<cql_type> types;
    for (const auto& c : _schema.partition_key_columns()) { types.push_back(cql_type_of(*c.type)); }
    std::vector<bytes> parts;
    for (auto&& v : dk.key().components(_schema)) { parts.push_back(linearized(v)); }
    explode_key(parts, types, _pk);
}

void fragment_shredder::add_clustering_row(const clustering_row& cr) {
    row r;
    r.key = _pk;
    std::vector<cql_type> ck_types;
    for (const auto& c : _schema.clustering_key_columns()) { ck_types.push_back(cql_type_of(*c.type)); }
    std::vector<bytes> parts;
    for (auto&& v : cr.key().components(_schema)) { parts.push_back(linearized(v)); }
    explode_key(parts, ck_types, r.key);

    // Row-level metadata. The marker is what makes a row with no live cells
    // exist at all, so losing it deletes the row.
    if (!cr.marker().is_missing()) {
        marker_info m;
        m.timestamp = cr.marker().timestamp();
        if (cr.marker().is_expiring()) {
            m.ttl    = int32_t(cr.marker().ttl().count());
            m.expiry = int32_t(cr.marker().expiry().time_since_epoch().count());
        }
        r.marker = m;
    }
    if (cr.tomb()) {
        const auto& sh = cr.tomb().tomb();          // the shadowable half
        r.row_del = deletion_info{sh.timestamp,
                                  int32_t(sh.deletion_time.time_since_epoch().count())};
        const auto& reg = cr.tomb().regular();
        if (reg) {
            r.row_del_regular = deletion_info{
                    reg.timestamp, int32_t(reg.deletion_time.time_since_epoch().count())};
        }
    }
    r.part_del = _part_del;
    _saw_clustering_row = true;
    replay_statics(r);

    // Regular cells. column_id indexes the regular columns, which is exactly the
    // index space schema_mapping uses for cells.
    cr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& acoc) {
        const column_definition& cdef = _schema.regular_column_at(id);
        if (cdef.is_counter()) {
            // Counter updates are a pre-shard-transformation form that never
            // reaches storage; a cell still in that form is a bug upstream, not
            // something to invent a representation for.
            auto av = acoc.as_atomic_cell(cdef);
            if (av.is_counter_update()) { unrepresentable("counter updates"); }
            r.collections.emplace(size_t(id), read_counter_cell(av));
            return;
        }
        if (cdef.is_atomic()) {
            auto av = acoc.as_atomic_cell(cdef);
            cell c;
            c.timestamp = av.timestamp();
            c.live = av.is_live();
            if (c.live) {
                auto lv = av.value().linearize();
                c.v = decode(cql_type_of(*cdef.type), bytes_view(lv));
                if (av.is_live_and_has_ttl()) {
                    c.ttl = int32_t(av.ttl().count());
                    c.local_deletion_time = int32_t(av.expiry().time_since_epoch().count());
                }
            } else {
                c.local_deletion_time = int32_t(av.deletion_time().time_since_epoch().count());
            }
            r.cells.emplace(size_t(id), std::move(c));
        }
        else {
            r.collections.emplace(size_t(id), read_collection_mutation(acoc));
        }
    });
    push_row(std::move(r));
}

void fragment_shredder::push_row(row&& r) {
    _buffered_bytes += heap_bytes(r);
    _rows.push_back(std::move(r));
}

void fragment_shredder::replay_statics(row& r) const {
    for (const auto& [k, c] : _static_cells) { r.cells.emplace(k, c); }
    for (const auto& [k, c] : _static_collections) { r.collections.emplace(k, c); }
}

void fragment_shredder::add_static_row(const static_row& sr) {
    // Held, not emitted: static cells are replayed onto every clustering row of
    // the partition, where they cost nothing because they are constant and
    // compress away. A partition with no clustering rows gets a placeholder from
    // end_partition().
    sr.cells().for_each_cell([&] (column_id id, const atomic_cell_or_collection& acoc) {
        const column_definition& cdef = _schema.static_column_at(id);
        if (cdef.is_counter()) {
            auto av = acoc.as_atomic_cell(cdef);
            if (av.is_counter_update()) { unrepresentable("counter updates"); }
            _static_collections.emplace(_static_base + size_t(id), read_counter_cell(av));
            return;
        }
        if (!cdef.is_atomic()) {
            _static_collections.emplace(_static_base + size_t(id),
                                        read_collection_mutation(acoc));
            return;
        }
        auto av = acoc.as_atomic_cell(cdef);
        cell c;
        c.timestamp = av.timestamp();
        c.live = av.is_live();
        if (c.live) {
            auto lv = av.value().linearize();
            c.v = decode(cql_type_of(*cdef.type), bytes_view(lv));
            if (av.is_live_and_has_ttl()) {
                c.ttl = int32_t(av.ttl().count());
                c.local_deletion_time = int32_t(av.expiry().time_since_epoch().count());
            }
        } else {
            c.local_deletion_time = int32_t(av.deletion_time().time_since_epoch().count());
        }
        _static_cells.emplace(_static_base + size_t(id), std::move(c));
    });
}

void fragment_shredder::add_range_tombstone_change(const range_tombstone_change& rtc) {
    // Carried as a row so it keeps its place in the clustering order. The
    // clustering-key columns hold the bound's prefix; anything past prefix_len is
    // padding and is ignored on the way back.
    const auto& pos = rtc.position();
    row r;
    r.key = _pk;

    std::vector<bytes> parts;
    if (pos.has_key()) {
        for (auto&& v : pos.key().components(_schema)) { parts.push_back(linearized(v)); }
    }
    for (size_t i = 0; i < _n_ck; ++i) {
        r.key.push_back(i < parts.size()
                ? decode(_cols[_n_pk + i].type, bytes_view(parts[i]))
                : decode(_cols[_n_pk + i].type, bytes_view()));
    }

    rtc_info ri;
    ri.weight     = int32_t(pos.get_bound_weight());
    ri.region     = int32_t(pos.region());
    ri.prefix_len = int32_t(parts.size()); // ok
    if (rtc.tombstone()) {
        ri.tomb = deletion_info{rtc.tombstone().timestamp,
                int32_t(rtc.tombstone().deletion_time.time_since_epoch().count())};
    }
    r.rtc = ri;
    r.part_del = _part_del;
    replay_statics(r);

    // The partition has content, so end_partition() must not add a placeholder.
    _saw_clustering_row = true;
    push_row(std::move(r));
}

void fragment_shredder::end_partition() {
    if (_saw_clustering_row || _pk.empty()) {
        _saw_clustering_row = false;
        return;
    }
    if (_static_cells.empty() && _static_collections.empty() && !_part_del) {
        return;                       // nothing to record
    }
    // Static-only (or tombstone-only) partition: one placeholder row, whose
    // clustering values are meaningless and are flagged as such.
    row r;
    r.key = _pk;
    for (size_t i = 0; i < _n_ck; ++i) {
        r.key.push_back(decode(_cols[_n_pk + i].type, bytes_view()));
    }
    r.no_ck = true;
    r.part_del = _part_del;
    replay_statics(r);
    push_row(std::move(r));
}

std::vector<uint8_t> fragment_shredder::to_parquet(const pq_writer_config& cfg) const {
    // The per-column overrides have to be handed over here too. There are two ways an sstable
    // gets written -- cut_row_group() when it outgrows the row-group budget, and this one-shot
    // path when the whole thing fits a single row group -- and for a while only the first passed
    // the overrides on. The effect was that `encoding.<col>` worked on large tables and did
    // nothing on small ones, which reads as an intermittent bug rather than a missing argument:
    // raising row_group_rows made it "start working" only because it forced a cut. Any per-column
    // writer setting added later has to travel down both paths for the same reason.
    return write_rows(_cols, _rows, cfg.level, cfg.wopt, cfg.exc, cfg.column_encodings);
}

std::vector<uint8_t> fragment_shredder::to_parquet_for_storage(const pq_writer_config& cfg) const {
    if (!folding_is_lossless(cfg.level)) {
        throw std::invalid_argument(
                std::string("folding level ") + to_string(cfg.level) +
                " discards cell metadata and cannot be used as a storage format; "
                "it is available for export only");
    }
    return to_parquet(cfg);
}

// ---------------------------------------------------------------- writer_impl
pq_writer_impl::pq_writer_impl(sstables::sstable& sst, const ::schema& s,
                               uint64_t estimated_partitions,
                               const sstables::sstable_writer_config& cfg,
                               pq_writer_config pcfg, encoding_stats enc_stats,
                               shard_id shard, sink_type sink)
    : sstables::sstable_writer::writer_impl(sst, s, cfg)
    , _shredder(s)
    , _pcfg(std::move(pcfg))
    , _enc_stats(enc_stats)
    , _sink(std::move(sink)) {
    if (_sink) {
        return;   // unit-test path: no sstable components at all
    }
    // Zero is benign here but not further down; mx clamps for the same reason.
    estimated_partitions = std::max(uint64_t(1), estimated_partitions);

    // create_data() is what actually opens the Data and Index files. Until it
    // runs, sst._data_file is null and make_data_or_index_sink dereferences it.
    sst.open_sstable(cfg.origin);
    sst.create_data().get();
    sst._shards = { shard };

    _index_sampling_state.summary_byte_cost = cfg.summary_byte_cost;
    _index_sampling_state.max_partitions_per_page = cfg.summary_max_partitions_per_page;

    {
        auto out = sst._storage->make_data_or_index_sink(sst, component_type::Data).get();
        _data_writer = std::make_unique<crc32_checksummed_file_writer>(
                std::move(out), sst.sstable_buffer_size, sst.get_filename());
    }
    if (sst.has_component(component_type::Index)) {
        auto out = sst._storage->make_data_or_index_sink(sst, component_type::Index).get();
        _index_writer = std::make_unique<crc32_digest_file_writer>(
                std::move(out), sst.sstable_buffer_size, sst.index_filename());
        sstables::prepare_summary(sst._components->summary, estimated_partitions,
                                  s.min_index_interval());
    }

    _cfg.monitor->on_write_started(_data_writer->offset_tracker());
    if (sst.has_component(component_type::Filter)) {
        sst._components->filter = utils::i_filter::get_filter(
                estimated_partitions, s.bloom_filter_fp_chance(), utils::filter_format::m_format);
    }
}

// The mc index entry is: key, then a vint position, then a vint promoted-index
// size. For pq the position is the row ordinal, and the promoted index is always
// absent because Parquet's ColumnIndex already provides intra-partition seeking
// (design doc open question 2).
void pq_writer_impl::finish_open_partition() {
    if (!_in_partition || !_index_writer) {
        _in_partition = false;
        return;
    }
    write_vint(*_index_writer, uint64_t(0));   // no promoted index
    _in_partition = false;
}


// ------------------------------------------------------ statistics collection
//
// Mirrors sstables/mx/writer.cc: write_cell() for atomic cells, write_liveness_info()
// for row markers, and collect_row_stats() / collect_range_tombstone_stats() for the
// counters. Keeping the same call order and the same is_live distinctions matters --
// min_live_timestamp and the tombstone drop-time histogram feed tombstone GC
// decisions, so "close enough" here is a correctness bug, not a reporting one.

void pq_writer_impl::collect_atomic_cell(const atomic_cell_view& cell) {
    if (!cell.is_live()) {
        _c_stats.update_timestamp(cell.timestamp(), is_live::no);
        _c_stats.update_local_deletion_time_and_tombstone_histogram(cell.deletion_time());
        return;
    }
    _c_stats.update_timestamp(cell.timestamp(), is_live::yes);
    if (cell.is_live_and_has_ttl()) {
        _c_stats.update_ttl(cell.ttl());
        // The histogram takes the *expiry*, not the TTL: if a long TTL were fed in
        // instead, an sstable whose data expires far in the future would look fully
        // expired now.
        _c_stats.update_local_deletion_time_and_tombstone_histogram(cell.expiry());
    } else {
        _c_stats.update_local_deletion_time(std::numeric_limits<int32_t>::max());
    }
}

void pq_writer_impl::collect_cell(const column_definition& cdef,
                                  const atomic_cell_or_collection& acoc) {
    if (!cdef.is_atomic()) {
        // A non-frozen collection counts as one column and one cell per element,
        // and carries its own collection-wide tombstone.
        auto cmv = acoc.as_collection_mutation();
        _c_stats.update(cmv.tomb());
        for (auto&& kv : cmv) {
            collect_atomic_cell(kv.second);
            ++_c_stats.cells_count;
        }
        ++_c_stats.column_count;
        return;
    }
    collect_atomic_cell(acoc.as_atomic_cell(cdef));
    ++_c_stats.cells_count;
    ++_c_stats.column_count;
}

void pq_writer_impl::collect_cells(const ::row& cells, ::column_kind kind) {
    cells.for_each_cell([&] (column_id id, const atomic_cell_or_collection& acoc) {
        collect_cell(_schema.column_at(kind, id), acoc);
    });
}

void pq_writer_impl::collect_marker(const row_marker& marker) {
    if (marker.is_missing()) {
        return;
    }
    if (marker.is_live()) {
        _c_stats.update_timestamp(marker.timestamp(), is_live::yes);
        _c_stats.update_live_row_marker_timestamp(marker.timestamp());
    } else {
        _c_stats.update_timestamp(marker.timestamp(), is_live::no);
    }
    if (!marker.is_live()) {
        _c_stats.update_ttl(gc_clock::duration(sstables::expired_liveness_ttl));
        _c_stats.update_local_deletion_time_and_tombstone_histogram(marker.deletion_time());
    } else if (marker.is_expiring()) {
        _c_stats.update_ttl(marker.ttl());
        _c_stats.update_local_deletion_time_and_tombstone_histogram(marker.expiry());
    } else {
        _c_stats.update_ttl(0);
        _c_stats.update_local_deletion_time(std::numeric_limits<int32_t>::max());
    }
}

// Emit the buffered rows as one row group and drop them.
//
// Cut only at a partition boundary, so a partition never spans row groups. That keeps the
// option-A index entry a single ordinal and keeps a point read inside one row group.
//
// A partition larger than the budget therefore overshoots it rather than being split, and
// that is deliberate (decided 2026-08-18): keeping a partition whole is worth more than
// holding the budget exactly. Splitting one would mean the index entry carrying
// (row group, ordinal) instead of a bare ordinal, and a point read spanning row groups --
// complexity paid on every read to bound a rare case. The budget is a target, not a
// guarantee; see design doc 5.5a for the residual exposure.
void pq_writer_impl::cut_row_group() {
    if (_shredder.size() == 0) { return; }
    if (!_pq) {
        // First cut. Parquet fixes one leaf set for the whole file, and we are only a
        // prefix of the way through the rows, so it has to cover every case a later row
        // might need rather than only what these rows use.
        _ms.emplace(map_schema(_shredder.columns(), _pcfg.level, _shredder.rows(),
                               _pcfg.exc, leaf_set::conservative, _pcfg.column_encodings));
        if (!folding_is_lossless(_ms->level)) {
            throw std::invalid_argument(
                    std::string("folding level ") + to_string(_ms->level) +
                    " discards cell metadata and cannot be used as a storage format");
        }
        std::vector<std::optional<format::encoding>> hints;
        hints.reserve(_ms->columns.size());
        for (const auto& c : _ms->columns) { hints.push_back(c.preferred); }
        _pq = std::make_unique<format::parquet_file_writer>(
                format::parquet_file_writer::nested_schema{_ms->tree, std::move(hints)},
                _pcfg.wopt);
        _pq->add_key_value("scylla.folding_level", to_string(_ms->level));
        // L2 keeps one timestamp for the whole file rather than a per-row column, so the value
        // lives in the footer and the reader requires it -- mapped_schema_from_footer() throws
        // "L2 file without scylla.uniform_timestamp" when it is missing. write_rows() emits it and
        // this path did not.
        //
        // That asymmetry is currently unreachable rather than a live bug, and the reason is worth
        // stating so nobody 'fixes' it by deleting this: the conservative leaf set this path uses
        // sets all_same_ts = false and turns every optional metadata flag on, which breaks L2's
        // precondition, so build_mapped_schema() falls the level back to L1 and never sets
        // uniform_ts. The guard stays because the invariant it protects -- an L2 footer carries its
        // timestamp -- belongs with the code that writes the footer, not with the leaf-set logic
        // three files away that happens to make it moot today.
        if (_ms->uniform_ts) {
            _pq->add_key_value("scylla.uniform_timestamp", std::to_string(*_ms->uniform_ts));
        }
        add_counter_metadata(*_pq, _shredder.columns());

        // Stream straight into the Data component instead of accumulating the file.
        // Without this, peak write memory is the whole output -- ~253 MB for a 256 MB
        // bottom-tier sstable, per concurrent compaction per shard (design doc 7.2).
        //
        // Only on the real sstable path. `_sink` is the unit-test route, which wants the
        // finished image handed back in one piece, and there is no _data_writer at all in
        // that case.
        //
        // Safe to write here even though finish_open_partition() and the index bookkeeping
        // run later: the Parquet index is by *row ordinal*, not by data-file offset
        // (section 5.4, option A), so nothing downstream depends on where the data lands.
        if (_data_writer && !_sink) {
            _streaming = true;
            _pq->set_sink([this] (std::span<const uint8_t> bytes) {
                _data_writer->write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
                _pos += bytes.size();
            });
        }
    }
    auto data = shred(*_ms, _shredder.columns(), _shredder.rows());
    _pq->add_row_group(data);
    _rows_flushed += _shredder.size();
    _shredder.clear();
}

void pq_writer_impl::consume_new_partition(const dht::decorated_key& dk) {
    finish_open_partition();
    _shredder.new_partition(dk);
    _partition_first_row = _rows_flushed + _shredder.size();

    if (_index_writer) {
        auto pk = key::from_partition_key(_schema, dk.key());
        // The filter and the min/max key statistics are fed per partition, as
        // mx does. Without the filter every point read on this sstable misses.
        if (_sst._components->filter) {
            _sst._components->filter->add(utils::make_hashed_key(bytes_view(pk)));
        }
        _collector.add_key(bytes_view(pk));
        sstables::maybe_add_summary_entry(_sst._components->summary, dk.token(),
                bytes_view(pk), _index_writer->offset(), _index_writer->offset(),
                _index_sampling_state);
        // Same on-disk shape as mc: a uint16-prefixed key, then a vint. Only the
        // meaning of the vint differs.
        auto p_key = disk_string_view<uint16_t>();
        p_key.value = bytes_view(pk);
        write(_sst.get_version(), *_index_writer, p_key);
        // Option A: the row ordinal of this partition's first row, not a byte
        // offset. The reader maps it to a page through the OffsetIndex.
        write_vint(*_index_writer, _partition_first_row);
        _in_partition = true;

        // After the write: p_key views into pk.
        if (!_first_key) { _first_key = pk; }
        _last_key = std::move(pk);
    }
    ++_num_partitions;
}

void pq_writer_impl::consume(tombstone t) {
    if (t) {
        _shredder.set_partition_tombstone(t);
        _c_stats.update(t);
        // A partition tombstone spans the whole clustering range, so it widens the
        // min/max clustering key to both sentinels -- exactly what mx records.
        _collector.update_min_max_components(
                position_in_partition_view::before_all_clustered_rows());
        _collector.update_min_max_components(
                position_in_partition_view::after_all_clustered_rows());
    }
}

stop_iteration pq_writer_impl::consume(static_row&& sr) {
    collect_cells(sr.cells(), ::column_kind::static_column);
    _shredder.add_static_row(sr);
    return stop_iteration::no;
}

stop_iteration pq_writer_impl::consume(clustering_row&& cr) {
    _collector.update_min_max_components(cr.position());
    collect_marker(cr.marker());
    _c_stats.update(cr.tomb().regular());
    _c_stats.update(cr.tomb().tomb());
    collect_cells(cr.cells(), ::column_kind::regular_column);
    ++_c_stats.rows_count;
    if (cr.tomb()) {
        ++_c_stats.dead_rows_count;
    }
    _shredder.add_clustering_row(cr);
    return stop_iteration::no;
}

stop_iteration pq_writer_impl::consume(range_tombstone_change&& rtc) {
    _collector.update_min_max_components(rtc.position());
    _c_stats.update(rtc.tombstone());
    // mx counts a range tombstone change as a row as well as a range tombstone,
    // because on its side the marker occupies a row slot in the data file.
    ++_c_stats.rows_count;
    ++_c_stats.range_tombstones_count;
    _shredder.add_range_tombstone_change(rtc);
    return stop_iteration::no;
}

stop_iteration pq_writer_impl::consume_end_of_partition() {
    _shredder.end_partition();
    // Byte offsets are not available per partition here: the whole Parquet image is
    // encoded once at end of stream, so a partition has no start offset or on-disk
    // length while it is being consumed. Everything else in column_stats is exact;
    // partition_size stays 0, which only affects the estimated-partition-size
    // histogram, not any GC decision.
    _collector.update(std::move(_c_stats));
    _c_stats.reset();
    // A partition boundary is the only place a cut is allowed, so this is where the
    // budget is checked.
    if (_shredder.buffered_bytes() >= _pcfg.row_group_buffer_bytes ||
        _shredder.size() >= _pcfg.row_group_rows) {
        cut_row_group();
    }
    return stop_iteration::no;
}

void pq_writer_impl::consume_end_of_stream() {
    // Two paths on purpose. If no cut ever happened the whole sstable fits the budget and
    // goes out as a single row group with the *derived* leaf set -- identical to what this
    // writer produced before row-group cutting existed. Only once a cut has forced the
    // conservative leaf set does the streaming path take over.
    std::vector<uint8_t> img;
    if (_pq) {
        cut_row_group();            // the tail
        img = _pq->finish();        // empty when streaming: already in the Data component
    } else {
        // No cut ever happened, so the whole sstable fitted the row-group budget and the
        // image is bounded by it. Materialising here costs at most one row group.
        img = _shredder.to_parquet_for_storage(_pcfg);
    }
    if (!_streaming) {
        _pos = img.size();          // streaming keeps _pos as it goes
    }

    // Compression ratio, for `nodetool` and the REST API. Without this a Parquet table reports
    // no ratio at all, because sstable::get_compression_ratio() looks for a CompressionInfo
    // component and Parquet has none -- it compresses inside the file. The honest numerator is
    // the file we wrote and the denominator is the sum of the column chunks' uncompressed sizes,
    // which is the serialised volume before the codec.
    const int64_t uncompressed = _pq ? _pq->uncompressed_bytes() : [&] {
        // No cut happened, so the whole file is in `img` and its footer is the only place the
        // per-chunk uncompressed sizes exist.
        if (img.empty()) { return int64_t(0); }
        try {
            auto md = format::parse_footer(img);
            int64_t n = 0;
            for (const auto& rg : md.row_groups) { n += rg.total_byte_size; }
            return n;
        } catch (...) {
            return int64_t(0);      // reporting must never fail a write
        }
    }();
    if (uncompressed > 0 && _pos > 0) {
        _collector.add_compression_ratio(_pos, uint64_t(uncompressed));
    }

    // A sink is the unit-test path: it lets the whole fragment -> Parquet route be
    // driven without constructing an sstable.
    if (_sink) {
        _sink(std::move(img));
        return;
    }

    // Otherwise the image becomes the Data component. consume_end_of_stream runs
    // in a seastar thread (mx::writer relies on the same), so blocking here is
    // allowed.
    finish_open_partition();
    if (!_streaming) {
        _data_writer->write(reinterpret_cast<const char*>(img.data()), img.size());
    }
    _data_writer->close();
    _sst.write_digest(_data_writer->full_checksum());
    _sst.write_crc(_data_writer->finalize_checksum());
    _data_writer.reset();
    _cfg.monitor->on_data_write_completed();
    write_components();
}

void pq_writer_impl::write_components() {
    if (_index_writer) {
        sstables::seal_summary(_sst._components->summary, std::move(_first_key),
                               std::move(_last_key), _index_sampling_state).get();
        _index_writer->close();
        _index_writer.reset();
    }
    _sst.set_first_and_last_keys();

    // The mc serialization header. Without it sstable::get_column_translation()
    // is empty, and the index parser reads that as "not mc format" and decodes
    // our vint entries as fixed-width big-endian -- every lookup then misses.
    _sst._components->statistics.contents[metadata_type::Serialization] =
            std::make_unique<serialization_header>(
                    mc::make_serialization_header(_schema, _enc_stats, _cfg));

    sstables::seal_statistics(_sst.get_version(), _sst._components->statistics, _collector,
            _schema.get_partitioner().name(), _schema.bloom_filter_fp_chance(),
            _sst.get_schema(), _sst.get_first_decorated_key(), _sst.get_last_decorated_key(),
            _enc_stats);

    _sst.maybe_rebuild_filter_from_index(_num_partitions);
    _sst.write_summary();
    _sst.write_filter();
    _sst.write_statistics();
    _sst.write_scylla_metadata(this_shard_id(), run_identifier{_cfg.run_identifier},
                               std::nullopt, std::nullopt, std::nullopt);
    if (!_cfg.leave_unsealed) {
        _sst.seal_sstable(_cfg.backup).get();
    }
}

std::unique_ptr<sstables::sstable_writer::writer_impl> make_writer(
        sstables::sstable& sst,
        const ::schema& s,
        uint64_t estimated_partitions,
        const sstables::sstable_writer_config& cfg,
        encoding_stats enc_stats,
        shard_id shard) {
    // From the table's `parquet = {...}` property. Already validated at CREATE/ALTER
    // time, so anything stored here parses; an empty map yields the defaults.
    pq_writer_config pcfg = parquet_parameters(s.parquet_options()).config();
    // The key is resolved here, at the last moment before writing, so a key file that changes
    // takes effect on the next sstable rather than needing a restart -- and so a missing key is
    // a loud failure at write time rather than a file written in the clear. Writing an
    // unencrypted file for a table that asked for encryption would be the worst outcome
    // available, since nothing downstream would ever notice.
    if (!pcfg.encryption_key_id.empty()) {
        auto k = keys().find(pcfg.encryption_key_id);
        if (!k) {
            throw std::runtime_error(seastar::format(
                    "{}.{}: parquet encryption key '{}' is not in parquet_encryption_key_file",
                    s.ks_name(), s.cf_name(), pcfg.encryption_key_id));
        }
        pcfg.wopt.encryption.enabled = true;
        pcfg.wopt.encryption.algo = pcfg.encryption_algo;
        pcfg.wopt.encryption.footer_key = *k;
        // Binds the file to the table it belongs to: a Data.db moved between tables, or replayed
        // from a backup into a different one, fails authentication instead of decoding.
        pcfg.wopt.encryption.aad_prefix = seastar::format("{}.{}", s.ks_name(), s.cf_name());
        pcfg.wopt.encryption.store_aad_prefix = true;
        // The reader needs to know which key. Wrapped in the key-material JSON that pyarrow and
        // Spark's KMS layers require, so an authorised external reader can open the file; see
        // make_key_metadata() for why the convention beats the minimal encoding here.
        pcfg.wopt.encryption.key_metadata = std::string(
                make_key_metadata(pcfg.encryption_key_id, pcfg.encryption_key_metadata));
    }
    return std::make_unique<pq_writer_impl>(sst, s, estimated_partitions, cfg,
                                            std::move(pcfg), enc_stats, shard, nullptr);
}

} // namespace sstables::parquet
