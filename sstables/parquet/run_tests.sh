#!/bin/bash
# Full test suite for sstables/parquet (layer 1 format codec + layer 2 mapping).
# Standalone by design: no Seastar, no Scylla headers, no libthrift.
set -u
cd "$(dirname "$0")"
# Test fixtures: real Parquet files from public datasets plus generated
# conformance cases. Point PARQUET_TEST_DATA at a directory containing
# nyc_taxi.parquet, hits_0.parquet and conf/*.parquet (see
# docs/dev/parquet-storage-format.md section 9.3 for where they come from).
DATA=${1:-${PARQUET_TEST_DATA:-./testdata}}
if [ ! -d "$DATA" ]; then
  echo "test data directory '$DATA' not found."
  echo "Set PARQUET_TEST_DATA or pass the path as \$1."
  echo "Suites 1, 6 and 7 need no fixtures; 2-5 do."
  exit 2
fi
FAIL=0
S=format

echo "### build ###"
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined \
    -o /tmp/pq_meta_t $S/parquet_metadata.cc $S/test_parquet_metadata.cc || FAIL=1
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined \
    -o /tmp/pq_lvl_t  $S/parquet_metadata.cc $S/page_header.cc $S/test_levels.cc || FAIL=1
g++ -std=c++20 -O2 -Wall -Wextra -Wpedantic \
    -o /tmp/pq_write_t $S/parquet_writer.cc $S/parquet_metadata.cc $S/test_writer.cc -lzstd || FAIL=1
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined -I. \
    -o /tmp/pq_shred_t schema_mapping.cc $S/parquet_writer.cc $S/parquet_metadata.cc \
       $S/page_header.cc $S/parquet_reader.cc test_shred.cc -lzstd -lsnappy || FAIL=1
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined -I../.. \
    -o /tmp/pq_tier_t tiering_policy.cc test_tiering.cc -lfmt || FAIL=1
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined -I. -I../.. \
    -o /tmp/pq_oi_t $S/test_offset_index.cc $S/parquet_reader.cc $S/parquet_metadata.cc \
       $S/page_header.cc -lzstd -lsnappy || FAIL=1
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined -I. -I../.. \
    -o /tmp/pq_xread_t $S/test_crossread.cc $S/parquet_reader.cc $S/parquet_metadata.cc \
       $S/page_header.cc -lzstd -lsnappy || FAIL=1
for f in $S/parquet_metadata.cc $S/page_header.cc $S/parquet_writer.cc $S/parquet_reader.cc schema_mapping.cc tiering_policy.cc; do
  # -Werror + -Wunused-private-field mirrors the in-tree Scylla build, which is
  # stricter than the gcc invocations above.
  clang++ -std=c++20 -O2 -Wall -Wextra -Wpedantic -Werror -Wunused-private-field \
          -I. -I../.. -c $f -o /tmp/clang_chk.o || FAIL=1
done

echo; echo "### 1. RLE/bit-packed round-trip ###";            /tmp/pq_lvl_t roundtrip || FAIL=1
echo; echo "### 2. footer conformance vs pyarrow ###"
python3 $S/conformance.py /tmp/pq_meta_t $DATA/nyc_taxi.parquet $DATA/hits_0.parquet $DATA/conf/*.parquet || FAIL=1
echo; echo "### 3. real V2 page level decode ###"
for f in $DATA/conf/v2page_*.parquet; do /tmp/pq_lvl_t levels "$f" || FAIL=1; done
echo; echo "### 4. footer fuzz / corruption ###";              /tmp/pq_meta_t fuzz $DATA/nyc_taxi.parquet | tail -11 || FAIL=1
echo; echo "### 5. writer -> pyarrow interop ###"
mkdir -p $DATA/wout && /tmp/pq_write_t emit $DATA/wout >/dev/null && \
  python3 $S/writer_interop.py $DATA/wout || FAIL=1
echo; echo "### 6. folding round-trip (losslessness) ###";     /tmp/pq_shred_t roundtrip || FAIL=1
echo; echo "### 7. divergence cost curve ###";                 /tmp/pq_shred_t cost || FAIL=1
echo; echo "### 8. hybrid tiering policy (C1-C7) ###";         /tmp/pq_tier_t || FAIL=1
echo; echo "### 9. file round-trip: rows -> parquet -> rows ###"; /tmp/pq_shred_t filetrip || FAIL=1
echo; echo "### 10. OffsetIndex: row -> page lookup ###"
/tmp/pq_oi_t $DATA/wout/*.parquet || FAIL=1
echo; echo "### 11. L3 logical export (lossy, export-only) ###"; /tmp/pq_shred_t logical || FAIL=1
echo; echo "### 12. cross-read: parquet-cpp files, values vs pyarrow ###"
python3 $S/crossread.py /tmp/pq_xread_t $DATA/conf/v2page_*.parquet || FAIL=1

echo; echo "==================================="
[ $FAIL -eq 0 ] && echo "PARQUET SUITE: ALL PASS" || echo "PARQUET SUITE: FAILURES"
exit $FAIL
