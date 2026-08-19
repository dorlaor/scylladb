// Write encrypted Parquet files for an external reader to open.
//
// The conformance test reads parquet-cpp's files with our code. This is the other direction, and
// it is the one that decides whether the feature is worth anything: a file only we can read is a
// container, not a Parquet file. So this writes files and prints nothing but their paths -- the
// verdict comes from pyarrow, in test_encrypt_interop.py.
//
// Two configurations, because they exercise different module types: with and without a
// dictionary (the dictionary page is a separate pair of modules with no page ordinal, which is
// exactly the kind of detail that round-trips fine against ourselves and fails against anyone
// else).

#include "parquet_writer.hh"
#include "encryption.hh"

#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

using namespace sstables::parquet::format;

static void write_file(const std::string& path, bool dict, cipher algo,
                       const std::string& key_bytes, const std::string& aad_prefix,
                       bool store_prefix) {
    std::vector<column_spec> schema;
    schema.push_back(column_spec{"id", phys_type::int64, repetition::required});
    schema.push_back(column_spec{"name", phys_type::byte_array, repetition::optional});

    writer_options opt;
    opt.compression = codec::zstd;
    opt.use_dictionary = dict;
    opt.write_page_index = true;
    opt.page_values = 40;            // several pages, so page ordinals actually vary
    opt.encryption.enabled = true;
    opt.encryption.algo = algo;
    opt.encryption.footer_key = encryption_key{
        std::vector<uint8_t>(key_bytes.begin(), key_bytes.end())};
    opt.encryption.aad_prefix = aad_prefix;
    opt.encryption.store_aad_prefix = store_prefix;
    // key_metadata is opaque to the format: the spec says only "whatever the reader needs to
    // find the key". pyarrow's *Python* API can only decrypt through a KMS, and its KMS layer
    // expects this particular JSON -- parquet-java's "key tools" key-material format. Emitting
    // it is what makes the file openable by a stock high-level reader rather than only by one
    // using the C++ explicit-key API. No key material is in it: wrappedDEK is a placeholder and
    // the test's KMS returns the key for masterKeyID.
    opt.encryption.key_metadata = std::string(
            "{\"keyMaterialType\":\"PKMT1\",\"internalStorage\":true,"
            "\"isFooterKey\":true,\"kmsInstanceID\":\"DEFAULT\","
            "\"kmsInstanceURL\":\"DEFAULT\",\"masterKeyID\":\"scylla-test-key\","
            "\"wrappedDEK\":\"AAAAAAAAAAAAAAAAAAAAAA==\",\"doubleWrapping\":false}");

    parquet_file_writer w(schema, opt);
    w.add_key_value("scylla.test", "encrypted");

    column_data ids, names;
    constexpr int N = 100;
    for (int i = 0; i < N; ++i) {
        ids.i64.push_back(int64_t(i));
        names.def_levels.push_back(1);
        // Low cardinality when a dictionary is wanted, so the dictionary path is really taken.
        names.str.push_back(dict ? ("g" + std::to_string(i % 4)) : ("v" + std::to_string(i)));
    }
    std::vector<column_data> cols{std::move(ids), std::move(names)};
    w.add_row_group(cols);
    auto img = w.finish();

    std::ofstream f(path, std::ios::binary);
    f.write(reinterpret_cast<const char*>(img.data()), std::streamsize(img.size()));
    std::cout << path << " " << img.size() << "\n";
}

int main(int argc, char** argv) {
    const std::string dir = argc > 1 ? argv[1] : ".";
    const std::string key = "0123456789abcdef";
    write_file(dir + "/scylla_gcm_dict.parquet",   true,  cipher::aes_gcm_v1, key, "", false);
    write_file(dir + "/scylla_gcm_plain.parquet",  false, cipher::aes_gcm_v1, key, "", false);
    write_file(dir + "/scylla_ctr_dict.parquet",   true,  cipher::aes_gcm_ctr_v1, key, "", false);
    // An AAD prefix, stored so a reader needs nothing out of band.
    write_file(dir + "/scylla_gcm_prefix.parquet", true,  cipher::aes_gcm_v1, key,
               "ks.tbl/generation-42", true);
    return 0;
}
