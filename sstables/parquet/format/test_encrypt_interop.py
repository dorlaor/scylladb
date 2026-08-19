#!/usr/bin/env python3
"""Can a stock Parquet reader open the encrypted files we write?

This is the assertion that matters for modular encryption. A file only our own reader can open
would be a Scylla container that happens to end in .parquet -- the whole argument for encrypting
inside the format rather than underneath it is that an authorised external reader still works.

Run test_encrypt_write first and pass its output directory as argv[1].
"""
import sys, pathlib
import pyarrow.parquet as pq

KEY = b"0123456789abcdef"

def check(ok, what):
    print(("  ok   " if ok else "  FAIL ") + what)
    return 0 if ok else 1

def main():
    d = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ".")
    fails = 0
    # (file, aad_prefix the reader must supply -- None when the writer stored it)
    cases = [
        ("scylla_gcm_dict.parquet",   None),
        ("scylla_gcm_plain.parquet",  None),
        ("scylla_ctr_dict.parquet",   None),
        ("scylla_gcm_prefix.parquet", None),
    ]
    for name, prefix in cases:
        p = d / name
        if not p.exists():
            fails += check(False, "%s: file missing" % name)
            continue
        print("== %s" % name)
        # Explicit keys, not a KMS: one key for the footer and therefore for every column, which
        # is what a storage engine handing a reader a per-table key looks like.
        kw = {"footer_key": KEY}
        if prefix is not None:
            kw["aad_prefix"] = prefix
        try:
            props = pq.encryption.DecryptionProperties(**kw) \
                if hasattr(pq, "encryption") and hasattr(pq.encryption, "DecryptionProperties") \
                else None
        except Exception:
            props = None
        try:
            if props is not None:
                f = pq.ParquetFile(p, decryption_properties=props)
            else:
                import pyarrow.parquet.encryption as pe
                class KMS(pe.KmsClient):
                    def __init__(self, cfg): super().__init__()
                    def wrap_key(self, key, master): return key
                    def unwrap_key(self, wrapped, master):
                        # The file names the key; the KMS supplies it. wrappedDEK is a
                        # placeholder, so there is no key material in the file itself.
                        assert master == "scylla-test-key", master
                        return KEY
                factory = pe.CryptoFactory(lambda cfg: KMS(cfg))
                kms = pe.KmsConnectionConfig(custom_kms_conf={})
                dc = pe.DecryptionConfiguration(cache_lifetime=None)
                f = pq.ParquetFile(p, decryption_properties=factory.file_decryption_properties(
                        kms, dc))
            t = f.read()
            fails += check(t.num_rows == 100, "100 rows read")
            fails += check(t.column_names == ["id", "name"], "column names")
            fails += check(t.column("id").to_pylist() == list(range(100)), "id values exact")
            fails += check(all(n is not None for n in t.column("name").to_pylist()),
                           "name values present")
            md = f.metadata.metadata
            fails += check(md is not None and md.get(b"scylla.test") == b"encrypted",
                           "key/value metadata survived")
        except Exception as e:
            fails += check(False, "pyarrow could not read it: %s" % str(e)[:220])

        # Without the key it must fail rather than return anything at all.
        try:
            pq.ParquetFile(p).read()
            fails += check(False, "opened WITHOUT a key")
        except Exception:
            fails += check(True, "refuses to open without a key")

    print("ENCRYPTION INTEROP " + ("FAIL" if fails else "PASS") + " (%d failures)" % fails)
    return 1 if fails else 0

if __name__ == "__main__":
    sys.exit(main())
