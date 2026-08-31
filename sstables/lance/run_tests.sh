#!/bin/bash
# Standalone test suite for sstables/lance (layer 1 format codec + layer 2
# glue). No Seastar, no Scylla headers, no libprotobuf -- same discipline as
# sstables/parquet/run_tests.sh, including the terminator-line rules: grep for
# "LANCE SUITE: ALL PASS", anything else is not a clean full run.
set -u
cd "$(dirname "$0")" || exit 1
FAIL=0
S=format
# Scratch space for interop files.
OUT=${LANCE_TEST_OUT:-/tmp/lance-suite}
mkdir -p "$OUT"

echo "### build ###"
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined \
    -o /tmp/lc_meta_t $S/lance_metadata.cc $S/test_metadata.cc || FAIL=1
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined \
    -o /tmp/lc_enc_t $S/lance_metadata.cc $S/lance_encodings.cc $S/test_encodings.cc -lzstd || FAIL=1
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined \
    -o /tmp/lc_file_t $S/lance_metadata.cc $S/lance_encodings.cc $S/lance_writer.cc \
       $S/lance_reader.cc $S/test_file.cc -lzstd || FAIL=1
for f in $S/lance_metadata.cc $S/lance_encodings.cc $S/lance_writer.cc $S/lance_reader.cc; do
  # -Werror + -Wunused-private-field mirrors the in-tree Scylla build.
  clang++ -std=c++20 -O2 -Wall -Wextra -Wpedantic -Werror -Wunused-private-field \
          -I. -I../.. -c $f -o /tmp/lc_clang_chk.o || FAIL=1
done

echo; echo "### 1. container metadata round-trip + hostile input ###"
/tmp/lc_meta_t || FAIL=1
echo; echo "### 2. structural encodings round-trip ###"
/tmp/lc_enc_t || FAIL=1
echo; echo "### 3. file round-trip: columns -> lance -> columns ###"
/tmp/lc_file_t roundtrip "$OUT" || FAIL=1
echo; echo "### 4. pylance reads what we write ###"
if python3 -c 'import lance' 2>/dev/null; then
  /tmp/lc_file_t emit "$OUT" && python3 $S/writer_interop.py "$OUT" || FAIL=1
else
  echo "!! pylance missing -- interop is a REQUIRED suite, not a skip"; FAIL=1
fi
echo; echo "### 5. we read what pylance writes ###"
if python3 -c 'import lance' 2>/dev/null; then
  # gen_golden.py writes both the .lance file (official writer) and pylance's
  # own decode of it; our dump must be byte-identical to the latter.
  if python3 $S/gen_golden.py "$OUT/golden" \
      && /tmp/lc_file_t dump "$OUT/golden/golden_21.lance" > "$OUT/golden/ours.tsv" \
      && diff -q "$OUT/golden/ours.tsv" "$OUT/golden/golden_21.expected"; then
    echo "crossread: MATCH"
  else
    FAIL=1
  fi
else
  echo "!! pylance missing -- interop is a REQUIRED suite, not a skip"; FAIL=1
fi

echo; echo "==================================="
if [ $FAIL -ne 0 ]; then
  echo "LANCE SUITE: FAILURES"
else
  echo "LANCE SUITE: ALL PASS"
fi
exit $FAIL
