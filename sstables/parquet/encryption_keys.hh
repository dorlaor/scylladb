/*
 * Copyright (C) 2026-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

// Where a Parquet encryption key comes from.
//
// A table names its key by *id*, never by value. That matters more than it looks: schema
// properties are replicated to every node, stored in system tables and printed by DESCRIBE, so a
// key placed in one would be copied into the cluster's metadata and into any schema dump. The id
// is the only thing that travels; the bytes are resolved locally, from a file each node holds.
//
// This is deliberately the smallest thing that is honest rather than an attempt at key
// management. It has no rotation, no KMS, no wrapping, and it reads keys from a file on local
// disk -- so it is exactly as strong as the file's permissions and no stronger. The interface is
// what a real provider would have to implement, which is the useful part: everything above it
// deals in key ids.

#pragma once

#include "sstables/parquet/format/encryption.hh"

#include <seastar/core/sstring.hh>

#include <map>
#include <optional>

namespace sstables::parquet {

// Loads `key_id base64_key` pairs, one per line; blank lines and '#' comments ignored. Key
// lengths must be 16, 24 or 32 bytes -- anything else is a configuration error at load time
// rather than a write failure later.
class key_registry {
    std::map<seastar::sstring, format::encryption_key> _keys;
public:
    // Throws on a malformed file. An empty path yields an empty registry, which is the
    // no-encryption-configured case and is not itself an error.
    void load(const seastar::sstring& path);
    std::optional<format::encryption_key> find(const seastar::sstring& id) const;
    bool empty() const { return _keys.empty(); }
    size_t size() const { return _keys.size(); }
};

// Process-wide, because the writer and reader reach it from deep inside call paths that have no
// business carrying a key provider as an argument. Set once during startup.
key_registry& keys();

// FileCryptoMetaData.key_metadata is opaque to the Parquet spec -- "whatever the reader needs to
// find the key". Writing the bare key id would be the minimal honest choice, and it is what the
// first version did; it is also unreadable by pyarrow and Spark, whose Python/Java APIs decrypt
// only through a KMS and require this particular JSON (parquet-java's "key tools" key material).
// Since interoperability is the entire reason for encrypting inside the format, the convention
// wins over the minimal encoding.
//
// No key material goes into it: `wrappedDEK` is a fixed placeholder and `masterKeyID` carries our
// key id, so a reader's KMS is asked for the key by name -- which is exactly how an operator
// would wire this key file to a reader's KMS.
seastar::sstring make_key_metadata(const seastar::sstring& key_id);

// The inverse: pull the key id back out. Accepts a bare id too, so a file written by the earlier
// form still opens.
seastar::sstring key_id_from_metadata(const seastar::sstring& key_metadata);

} // namespace sstables::parquet
