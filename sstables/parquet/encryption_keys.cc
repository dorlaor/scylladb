/*
 * Copyright (C) 2026-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "sstables/parquet/encryption_keys.hh"

#include <seastar/core/seastar.hh>

#include <fstream>
#include <sstream>
#include <stdexcept>

namespace sstables::parquet {

namespace {

std::vector<uint8_t> b64_decode(const std::string& s) {
    static const std::string tbl =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::vector<uint8_t> out;
    int val = 0, bits = -8;
    for (char c : s) {
        if (c == '=') { break; }
        auto pos = tbl.find(c);
        if (pos == std::string::npos) {
            throw std::runtime_error("parquet key file: not base64");
        }
        val = (val << 6) + int(pos);
        bits += 6;
        if (bits >= 0) { out.push_back(uint8_t((val >> bits) & 0xff)); bits -= 8; }
    }
    return out;
}

} // namespace

void key_registry::load(const seastar::sstring& path) {
    _keys.clear();
    if (path.empty()) { return; }
    std::ifstream f(path.c_str());
    if (!f) {
        throw std::runtime_error("parquet key file: cannot open " + std::string(path));
    }
    std::string line;
    size_t lineno = 0;
    while (std::getline(f, line)) {
        ++lineno;
        auto hash = line.find('#');
        if (hash != std::string::npos) { line.resize(hash); }
        std::istringstream is(line);
        std::string id, b64;
        if (!(is >> id)) { continue; }               // blank or comment-only
        if (!(is >> b64)) {
            throw std::runtime_error("parquet key file: line " + std::to_string(lineno)
                                     + ": expected 'key_id base64_key'");
        }
        format::encryption_key k{b64_decode(b64)};
        if (!k.valid()) {
            // Caught here rather than at write time: a 15-byte key would otherwise be a table
            // that accepts its DDL and then fails every flush.
            throw std::runtime_error("parquet key file: line " + std::to_string(lineno)
                                     + ": key '" + id + "' is " + std::to_string(k.bytes.size())
                                     + " bytes; must be 16, 24 or 32");
        }
        _keys[seastar::sstring(id)] = std::move(k);
    }
}

std::optional<format::encryption_key> key_registry::find(const seastar::sstring& id) const {
    auto it = _keys.find(id);
    if (it == _keys.end()) { return std::nullopt; }
    return it->second;
}

key_registry& keys() {
    static key_registry r;
    return r;
}

seastar::sstring make_key_metadata(const seastar::sstring& key_id) {
    // Field order matters to nobody, but the field *set* does: pyarrow rejects the material if
    // kmsInstanceID or kmsInstanceURL is missing, which is how the first attempt failed.
    return seastar::sstring("{\"keyMaterialType\":\"PKMT1\",\"internalStorage\":true,"
                            "\"isFooterKey\":true,\"kmsInstanceID\":\"DEFAULT\","
                            "\"kmsInstanceURL\":\"DEFAULT\",\"masterKeyID\":\"")
           + key_id
           + "\",\"wrappedDEK\":\"AAAAAAAAAAAAAAAAAAAAAA==\",\"doubleWrapping\":false}";
}

seastar::sstring key_id_from_metadata(const seastar::sstring& km) {
    const std::string s(km);
    const std::string tag = "\"masterKeyID\"";
    auto p = s.find(tag);
    if (p == std::string::npos) {
        return km;      // the bare-id form
    }
    p = s.find(':', p + tag.size());
    if (p == std::string::npos) { return {}; }
    auto a = s.find('"', p);
    if (a == std::string::npos) { return {}; }
    auto b = s.find('"', a + 1);
    if (b == std::string::npos) { return {}; }
    return seastar::sstring(s.substr(a + 1, b - a - 1));
}

} // namespace sstables::parquet
