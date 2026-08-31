#!/usr/bin/env python3
# Suite 4 of run_tests.sh: the official Lance reader (pylance) must read what
# our writer emits, value-exact. The expected values reproduce
# test_file.cc make_table() -- same mixer, same null pattern.
#
# usage: writer_interop.py <dir with ours_plain.lance / ours_zstd.lance>

import sys

import lance.file as lf

ROWS = 20_000
MASK = (1 << 64) - 1


def mix(i: int) -> int:
    return ((i + 1) * 0x9E3779B97F4A7C15) & MASK


def expected():
    pk, v32, x, s, fat, gone = [], [], [], [], [], []
    for i in range(ROWS):
        h = mix(i)
        pk.append(h >> 1)
        v32.append(None if i % 7 == 0 else (h & 0xFFFFFFFF) - ((h & 0x80000000) << 1))
        x.append(i * 1.5)
        s.append(None if i % 11 == 0 else "s-%d" % (h % 100000))
        if i % 13 == 0:
            fat.append(None)
        else:
            n = 280 + i % 90
            fat.append("".join(chr(ord("A") + (mix((i * 1315423911 + k) & MASK) % 26)) for k in range(n)))
        gone.append(None)
    return {"pk": pk, "v32": v32, "x": x, "s": s, "fat": fat, "gone": gone}


def check(path, exp):
    r = lf.LanceFileReader(path)
    tbl = r.read_all().to_table()
    assert tbl.num_rows == ROWS, f"{path}: {tbl.num_rows} rows"
    names = tbl.schema.names
    assert names == ["pk", "v32", "x", "s", "fat", "gone"], names
    md = r.metadata()
    for name in names:
        got = tbl.column(name).to_pylist()
        want = exp[name]
        if name == "pk":
            # int64 wraparound: our writer stores h >> 1 as a signed value
            want = [w if w < (1 << 63) else w - (1 << 64) for w in want]
        for i, (g, w) in enumerate(zip(got, want)):
            assert g == w, f"{path}: {name}[{i}]: {g!r} != {w!r}"
    print(f"{path}: {ROWS} rows x {len(names)} cols OK "
          f"(num_rows={md.num_rows})")


def main():
    d = sys.argv[1]
    exp = expected()
    for f in ("ours_plain.lance", "ours_zstd.lance"):
        check(f"{d}/{f}", exp)
    print("writer_interop: ALL OK")


if __name__ == "__main__":
    main()
