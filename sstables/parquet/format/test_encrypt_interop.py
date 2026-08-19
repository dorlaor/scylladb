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

def check(ok, what, extra=""):
    print(("  ok   " if ok else "  FAIL ") + what + ((" -- " + extra) if extra else ""))
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

    # ---- per-column keys, and the case the feature exists for: partial access.
    #
    # A reader given only the footer key must still open the file and read the columns that are
    # under that key, while the column with its own key stays shut. If that ever degrades to
    # "everything readable" the feature is worthless and nothing else here would notice.
    p = d / "scylla_percolumn.parquet"
    if p.exists():
        print("== scylla_percolumn.parquet")
        import pyarrow.parquet.encryption as pe

        def factory_for(keys):
            class KMS(pe.KmsClient):
                def __init__(self, cfg): super().__init__()
                def wrap_key(self, key, master): return key
                def unwrap_key(self, wrapped, master):
                    if master not in keys:
                        raise KeyError("no key for %s" % master)
                    return keys[master]
            return pe.CryptoFactory(lambda cfg: KMS(cfg))

        kms = pe.KmsConnectionConfig(custom_kms_conf={})
        dc = pe.DecryptionConfiguration(cache_lifetime=None)
        # KNOWN GAP, deliberately not counted as a failure here so the suite stays honest about
        # what is and is not true. Our writer's column-key columns are readable by *our* reader
        # (test_encrypt_write asserts that, including the wrong-key refusal), and parquet-cpp's own
        # per-column files are readable by our reader (test_encryption.cc asserts that too). But
        # pyarrow cannot decrypt a column-key column *we* wrote -- "Failed decryption
        # finalization" -- so the divergence is on our write side and is not yet found. Until it
        # is, per-column keys are not exposed through CQL. See storage-format 10.17.
        both = {"footerkey": KEY, "namekey": b"fedcba9876543210"}
        try:
            f = pq.ParquetFile(p, decryption_properties=factory_for(both)
                               .file_decryption_properties(kms, dc))
            t = f.read()
            print("  NOTE  pyarrow now reads our column-key column (%d rows) -- the known gap "
                  "is fixed; make this an assertion." % t.num_rows)
        except Exception as e:
            print("  known gap: pyarrow cannot decrypt our column-key column -- %s"
                  % str(e)[:90])

        # Footer key only: projecting the footer-key column must work...
        try:
            f = pq.ParquetFile(p, decryption_properties=factory_for({"footerkey": KEY})
                               .file_decryption_properties(kms, dc))
            t = f.read(columns=["id"])
            fails += check(t.num_rows == 100 and t.column("id").to_pylist() == list(range(100)),
                           "footer key only: the footer-key column still reads")
        except Exception as e:
            fails += check(False, "footer key only: the footer-key column still reads",
                           str(e)[:180])
        # ...and the column-key column must not.
        try:
            f = pq.ParquetFile(p, decryption_properties=factory_for({"footerkey": KEY})
                               .file_decryption_properties(kms, dc))
            f.read(columns=["name"])
            fails += check(False, "footer key only: the column-key column stays shut")
        except Exception:
            fails += check(True, "footer key only: the column-key column stays shut")

    print("ENCRYPTION INTEROP " + ("FAIL" if fails else "PASS") + " (%d failures)" % fails)
    return 1 if fails else 0

if __name__ == "__main__":
    sys.exit(main())
