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
// find the key" -- so what goes in it is a deployment choice, not a format one. Two shapes, and the
// default is deliberately the provider-neutral one.
//
//  provider    the key's own identifier, verbatim. This is what Scylla's own encryption at rest
//              deals in: ent/encryption's key_provider::key() returns a key *and* an opaque id to
//              store with the data and hand back to retrieve the same key later, which is exactly
//              the role key_metadata plays. Works with every provider, including BYOK through
//              KMIP/KMS/Azure/GCP, because the provider defines the id and we never interpret it.
//
//  parquet_kms parquet-java's "key tools" key-material JSON. pyarrow's Python API and Spark
//              decrypt *only* through a KMS that requires this shape, so it is the only way those
//              readers can open the file through their high-level API. But it encodes a specific
//              key-management model -- a masterKeyID plus a wrapped DEK -- and a BYOK deployment
//              whose keys live in a KMIP server or a cloud KMS does not necessarily map onto it.
//              Opt-in for that reason: it buys one reader's convenience at the cost of assuming
//              its key management, and that trade is the operator's to make, not ours.
//
// A reader using explicit keys (the C++/Java low-level API) opens either shape. Only the
// KMS-mediated high-level path needs `parquet_kms`.
enum class key_metadata_format { provider, parquet_kms };

std::optional<key_metadata_format> parse_key_metadata_format(std::string_view);
const char* to_string(key_metadata_format);

seastar::sstring make_key_metadata(const seastar::sstring& key_id, key_metadata_format);

// The inverse: pull the key id back out of either shape.
seastar::sstring key_id_from_metadata(const seastar::sstring& key_metadata);

} // namespace sstables::parquet
