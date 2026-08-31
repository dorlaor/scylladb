#!/usr/bin/env python3
# Suite 5 fixture generator: pylance writes golden files, our reader must
# agree with pylance's own decode of them (compared via `lc_file_t dump`).
#
# The columns deliberately stay inside our reader's decode subset: full-width
# integers (so the writer keeps Flat instead of bitpacking), high-cardinality
# strings (no dictionary), binary long values (no FSST symbol tables on
# incompressible data), one all-null column, one zstd-forced column.
#
# usage: gen_golden.py <outdir>

import os
import sys

import pyarrow as pa
import lance.file as lf

ROWS = 12_000
MASK = (1 << 64) - 1


def mix(i):
    return ((i + 1) * 0x9E3779B97F4A7C15) & MASK


def signed64(u):
    return u - (1 << 64) if u >= (1 << 63) else u


def build():
    import hashlib
    pk = [signed64(mix(i)) for i in range(ROWS)]
    v32 = [None if i % 7 == 0 else ((mix(i) & 0xFFFFFFFF) - ((mix(i) & 0x80000000) << 1))
           for i in range(ROWS)]
    x = [None if i % 9 == 0 else i * 0.25 for i in range(ROWS)]
    # Full-entropy short binary: any text-ish distribution triggers the
    # writer's FSST, which this reader deliberately does not decode (see
    # design doc 4), so the miniblock-variable case is exercised with bytes.
    import hashlib
    s = [None if i % 11 == 0
         else hashlib.blake2b(i.to_bytes(8, "little"), digest_size=20).digest()
         for i in range(ROWS)]
    fat = [None if i % 13 == 0
           else b"".join(hashlib.blake2b(i.to_bytes(8, "little") + j.to_bytes(2, "little"),
                                          digest_size=60).digest() for j in range(5))
           for i in range(ROWS)]
    gone = [None] * ROWS
    zs = ["z-%d-%s" % (i % 40, "x" * (i % 25)) for i in range(ROWS)]

    fields = [
        pa.field("pk", pa.int64(), nullable=False),
        pa.field("v32", pa.int32()),
        pa.field("x", pa.float64()),
        pa.field("s", pa.binary(), metadata={b"lance-encoding:dict-divisor": b"1000000"}),
        pa.field("fat", pa.binary(), metadata={b"lance-encoding:dict-divisor": b"1000000"}),
        pa.field("gone", pa.int64()),
        pa.field("zs", pa.string(), metadata={
            b"lance-encoding:compression": b"zstd",
            b"lance-encoding:dict-divisor": b"1000000",
        }),
    ]
    schema = pa.schema(fields, metadata={b"scylla.test": b"golden"})
    return pa.table(
        {"pk": pa.array(pk, pa.int64()), "v32": pa.array(v32, pa.int32()),
         "x": pa.array(x, pa.float64()), "s": pa.array(s, pa.binary()),
         "fat": pa.array(fat, pa.binary()), "gone": pa.array(gone, pa.int64()),
         "zs": pa.array(zs, pa.string())},
        schema=schema)


def main():
    out = sys.argv[1]
    os.makedirs(out, exist_ok=True)
    tbl = build()
    path = f"{out}/golden_21.lance"
    w = lf.LanceFileWriter(path, tbl.schema, version="2.1")
    for batch in tbl.to_batches(max_chunksize=4096):
        w.write_batch(batch)
    w.close()

    # The comparison baseline: pylance's own decode, in the dump TSV format
    # (col, row, value-or-NULL; strings/binary hex-encoded).
    r = lf.LanceFileReader(path)
    got = r.read_all().to_table()
    with open(f"{out}/golden_21.expected", "w") as f:
        for c, name in enumerate(got.schema.names):
            typ = got.schema.field(name).type
            for i, v in enumerate(got.column(name).to_pylist()):
                if v is None:
                    f.write(f"{c}\t{i}\tNULL\n")
                elif typ == pa.string():
                    f.write(f"{c}\t{i}\t{v.encode().hex()}\n")
                elif typ == pa.binary():
                    f.write(f"{c}\t{i}\t{v.hex()}\n")
                elif typ == pa.float64():
                    f.write(f"{c}\t{i}\t{v:.17g}\n")
                else:
                    f.write(f"{c}\t{i}\t{v}\n")
    print(f"golden: {path} ({ROWS} rows)")


if __name__ == "__main__":
    main()
