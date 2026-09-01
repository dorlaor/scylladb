# Lance storage format — design notes and plan

Status: design + plan. This document is the working memory of the Lance
storage-format effort, in the same spirit as `parquet-storage-format.md`:
decisions carry their reasons, measurements carry their dates, and
retractions stay in place.

## 0. Goal

Add Lance (https://lance.org/, the `.lance` columnar *file* format, not the
table/dataset format) as a third storage format next to the native row
format and Parquet, on its own branch (`lance`, forked from the parquet
tip), and benchmark all three — native LSM sstables vs `pq` vs `lc` — with
the same harness the Parquet work used. The hypothesis under test: Lance's
design center is random access, which is exactly the axis where `pq` lost
to native (point reads 2.5–2.8× slower, first contact dominated by footer
parse; design doc §10). If Lance closes the point-read gap while keeping a
useful fraction of the columnar size win, it is a better fit than Parquet
for tables that see mixed scan + point workloads.

## 1. What Lance is (condensed)

Primary sources: `protos/file2.proto`, `protos/encodings_v2_1.proto`,
`protos/file.proto` (normative in practice), https://lance.org/format/file/,
and the paper "Lance: Efficient Random Access in Columnar Storage through
Adaptive Structural Encodings" (arXiv:2504.15247).

The 2.x container is deliberately minimal:

```
| data pages (per-column buffers, writer-aligned)      |
| column metadatas (one ColumnMetadata proto / column) |
| column-metadata offset table (u64 pos, u64 size)     |
| global-buffer offset table  (u64 pos, u64 size)      |
| footer: 40 bytes LE                                  |
|   u64 col-meta-0 offset, u64 CMO offset,             |
|   u64 GBO offset, u32 n-global-bufs, u32 n-cols,     |
|   u16 major, u16 minor, "LANC"                       |
```

- **No row groups.** Each column flushes pages independently when its own
  buffer fills (≥8 MB advised). `Page.priority` = top-level row number of
  the page's first row, so row → page is a per-column binary search with
  no cross-column coupling. This is the load-bearing difference from
  Parquet: no single row-group knob that is simultaneously too big for
  wide columns and too small for narrow ones.
- **Schema** lives in global buffer 0 as a `lance.file.FileDescriptor`
  proto (flat `Field` list linked by `parent_id`, plus a free-form
  `map<string,bytes>` metadata — where `scylla.*` keys go).
- **Two structural page layouts** (v2.1) carry the random-access story:
  - **MiniBlock** for small values: the page is split into ~4 KiB chunks,
    each independently decodable; a u16-per-chunk metadata buffer (12-bit
    word count + 4-bit log2 value count) turns "row ordinal → chunk" into
    prefix-sum arithmetic. A point read decompresses ≤4 KiB, never a
    Parquet-style ~1 MiB page.
  - **FullZip** for large values: rep/def bits and (for var-width) the
    length are zipped in front of every value; fixed-width lookup is pure
    arithmetic, var-width lookup goes through a one-u64-per-row repetition
    index. 1–2 IOPs per value, no decompression amplification, because
    only "transparent" (per-value) compression is allowed here.
- Definition/repetition levels are Parquet-inspired but **local to the
  page and stored with the values**, so decoding one row never touches a
  column-wide level stream. All-valid layers cost zero stored bits.
- Compressive encodings (v2.1): Flat, Variable, inline/out-of-line
  bitpacking, FSST, Dictionary, RLE, ByteStreamSplit, General (LZ4/zstd
  wrapper). All optional — a writer picks a subset, a reader must decode
  what it wrote. **There is no delta encoding** (nothing like Parquet's
  DELTA_BINARY_PACKED / DELTA_BYTE_ARRAY).
- Honest performance framing from the paper (not the marketing "1000×"):
  Parquet at *defaults* does ~5.5K random rows/s; Parquet tuned for point
  reads (tiny pages, no compression, no dictionary) reaches ~350K rows/s
  but pays for it in scan speed, size, and ~20 B/page reader state; Lance
  2.1 matches tuned-Parquet on scalars and beats it on strings/nested/
  large values *without* giving up scan compression, and stays flat as
  value size and nesting grow.

## 2. Schema and workload fit — Lance vs Parquet vs native LSM

This is the "which schemas want which format" analysis the effort was
asked for. The native row format is the baseline everything must justify
itself against; it remains the only format for tables that take
overwrite/delete-heavy OLTP traffic at high rates, because both columnar
formats pay a full rewrite per compaction either way and neither can be
read without reconstructing rows.

**Where Lance should beat Parquet (and why):**

- **Point-read-heavy access over columnar data.** The `pq` numbers that
  motivated this branch: steady-state point reads 2.53–2.82× native, and
  §10.4's ~90 µs/column decode cost — both are row-group/page
  amplification costs. Lance's miniblock bounds a point read to a ~4 KiB
  chunk per touched column and fullzip to a single value; there is no
  rows_per_row_group dial to trade size against latency (the dial that
  cost pq +10% size to get 2.1× latency in §10.4c). Expected shape:
  Lance point reads land materially closer to native than pq at any size
  configuration, with *less* size sacrifice.
- **Wide-value columns: blobs, long text, embeddings.** FullZip and (at
  the extreme) BlobLayout mean a 100 KB blob read costs that blob, not
  the page around it. Parquet's page granularity makes big-cell tables
  choose between huge pages (read amplification) and per-value pages
  (offset-index/reader-state blowup, ~20 B/page). Schemas with a few key
  columns plus one or two fat payload columns — media metadata stores,
  document tables, feature/embedding stores — are Lance's home turf.
- **Very wide schemas (hundreds of CQL columns).** Per-column metadata
  blocks are independently fetchable (CMO table), and page sizing is per
  column, so a 197-column Backblaze-style table doesn't force narrow
  columns into the wide columns' geometry. Parquet's footer carries every
  chunk of every row group; pq's cold first-contact was 69% footer parse.
  Lance's footer is 40 bytes and column metadata is lazy by construction.
- **Mixed narrow+fat rows** (the classic Scylla shape: keys, a timestamp,
  a couple of ints, one text/blob payload): each column gets its optimal
  layout independently — miniblock for the ints, fullzip for the payload.

**Where Parquet should keep winning:**

- **Maximum-compression scans of narrow, sorted numerics — i.e. classic
  time series.** pq's biggest verified win is size (0.081× of
  zstd+dicts on the local corpus, 87.3% saving at 1 B rows on AWS), and a
  large slice of that is DELTA_BINARY_PACKED on sorted
  timestamp/clustering keys plus RLE_DICTIONARY on low-cardinality
  columns, compressed page-wide. Lance 2.1 has no delta encoding, its
  dictionary support is optional (and out of v1 scope here), and
  miniblock compresses in ~4 KiB windows — structurally worse zstd
  context than Parquet's ~1 MB pages. The paper's own "dates" dataset is
  the one case Parquet wins on both scan speed and size. For TWCS
  append-only telemetry that is scanned in bulk and rarely point-read,
  Parquet (or pq-hybrid bottom tiers) remains the right answer.
- **Ecosystem.** Anything exported for external consumption (Spark,
  DuckDB, pandas, every warehouse) reads Parquet natively. Lance is read
  by lancedb/pylance and little else; a `lc` sstable is for *us*, and
  export tooling would go through pylance or a converter.
- **Modular encryption.** Parquet has a spec-level per-column encryption
  story, and the pq branch implements it against Scylla's key providers.
  Lance has nothing equivalent; encryption for `lc` would be whole-file
  at the I/O layer (out of scope for v1).

**Where native LSM stays the default:** small rows read whole (the row
format reads one row in one contiguous read — columnar formats pay one
window per column); overwrite/tombstone-churn workloads where columnar
rewrite amplification dominates; latency-critical tables already served
from cache (format only matters below the cache); and anything needing
features v1 scopes out (see §4).

**Rule-of-thumb table** (what to recommend an operator):

| Schema / workload | Format |
|---|---|
| Sorted time series, scan-dominant, TWCS | `pq` (hybrid) |
| Key-value with fat blob/text payloads, point-read-heavy | `lc` |
| Very wide sparse schemas (100+ columns), mixed access | `lc` (expect pq to win size, lc to win latency) |
| Embedding / feature store (fixed-width float vectors) | `lc` |
| Narrow hot OLTP rows, heavy overwrites/deletes | native |
| Cold archival scanned by external tools | `pq` |

The three-way benchmark (§6) exists to check the two `lc` rows of this
table with numbers.

## 3. Design

### 3.1 What is reused from the parquet branch (everything above the file format)

The deliberate two-layer split of the parquet work pays off here: the
CQL ⇄ columnar model is format-agnostic, and only the container changes.

- **`fragment_shredder`** (writer_impl.hh) — mutation fragments →
  `std::vector<row>`: reused as-is, including statics replay, tombstone
  channels, memory accounting.
- **`schema_mapping`** — `map_schema()` / `shred()` / `reassemble()` and
  the whole L1 folding design (`__ts`, `__dmask`, `__rtc_*`, …): reused
  as-is. `shred()` produces `format::column_data` (def/rep levels + typed
  vectors) against `column_spec` descriptors; those two structs become
  the writer-side interface of the Lance format layer too. The folding
  level and leaf-name conventions ride in Lance schema metadata
  (`scylla.folding_level`), mirroring the Parquet KV entry, and the
  reader recovers a `mapped_schema` from leaf names exactly as
  `recover_mapped_schema` does from a Parquet footer.
- **Integration seams** — `sstable_version_types` gains `lc`;
  `sstable_writer::writer_impl` gets a third implementation; the
  version-keyed dispatch in `sstables.cc` (`make_reader`,
  `make_full_scan_reader`, `validate`, component-name regex) gets `lc`
  branches next to the `pq` ones; `get_highest_sstable_version()` skips
  `lc` like it skips `pq`; `implies_mx_generation()` already exists.
- **Index components** — same trick as pq: mc-shaped `Index.db` whose
  `position()` is a row ordinal. On the Lance side the ordinal maps to a
  page by `Page.priority` binary search and to a miniblock chunk by the
  chunk-metadata prefix sums — the OffsetIndex equivalent is built into
  the format.
- **DDL** — `storage_format = 'lance'` next to `'parquet'`/`'hybrid'`,
  plus a `lance = {...}` options map, gated on a new `lance_sstable_format`
  cluster feature; validation-by-construction like `parquet_parameters`.
- **Write-path integration** — the compaction/flush/streaming/load points
  that already ask "what format does this table write" get a third answer.

### 3.2 The new part: `sstables/lance/format/`

A from-scratch, dependency-free Lance 2.1 codec, standalone-testable with
plain g++, same philosophy and layer discipline as
`sstables/parquet/format/` (no schema/mutation/sstable headers):

- `pb.hh` — hand-written protobuf wire codec (varint, zigzag, length-
  delimited, `Any`), the analogue of the hand-written TCompactProtocol:
  a few hundred lines, bounded allocation on hostile input, no
  libprotobuf dependency.
- `lance_metadata.{hh,cc}` — footer read/write, CMO/GBO tables,
  `ColumnMetadata`/`Page`, `FileDescriptor`/`Schema`/`Field`, version
  pair (2,1).
- `lance_encodings.{hh,cc}` — MiniBlockLayout and FullZipLayout encode/
  decode with Flat + Variable value encodings, def levels (0/1), and
  General(zstd) chunk compression for miniblock. ConstantLayout for
  all-null pages if the golden files show pylance requires nothing more.
- `lance_writer.{hh,cc}` — consumes (`column_spec[]`, `column_data[]`)
  batches, accumulates per column, flushes pages independently at a
  per-column byte target, writes metadata + footer.
- `lance_reader.{hh,cc}` — footer/CMO/metadata parse with parse limits,
  row-ordinal and row-range reads returning `column_data`.
- Conformance: golden-file round-trips against pylance (official Rust
  reader/writer) in both directions, in the style of `crossread.py` /
  `writer_interop.py`.

### 3.3 Layout choice per leaf

Fixed-width leaves (i32/i64/f64 — keys, timestamps, the folded metadata
channels) → miniblock, Flat values, zstd chunks. BYTE_ARRAY leaves →
fullzip Variable when average value size is large, miniblock Variable
when small (threshold decided by measurement, initial cut ~64 B). All
leaves are flat scalars under the reused L1 mapping (`bits_rep = 0`
everywhere), because v1 rejects the only construct that produces
repeated leaves — see §4.

## 4. v1 scope

In: all scalar CQL types the pq mapping handles, frozen collections
(opaque blobs), full tombstone/TTL/static machinery (it is all scalar
channels after folding), flush/compaction/streaming write paths, point
reads and scans with bypass-cache, `nodetool`-visible sstables, the
three-way benchmarks.

Out (documented, DDL-rejected where applicable): **non-frozen collections
and counters** (the one source of repeated leaves; `storage_format =
'lance'` on such tables is a DDL-time error in v1), encryption at rest,
hybrid tiering (`'hybrid'` stays Parquet's), per-column encoding
overrides, dictionary/FSST/RLE/bitpacking encodings (size upside deferred
— v1 measures the *structural* size/latency trade), scylla-sstable
`parquet-export`-style dedicated export (generic `dump-data` works via
the reader dispatch).

## 5. Implementation plan

Phase gates mirror the parquet branch's — each phase lands with its tests
green before the next starts.

1. **Format layer** — pb codec, metadata, miniblock/fullzip encode+decode,
   writer, reader; standalone unit tests; pylance golden-file conformance
   both directions. Gate: a pylance-written file of every supported shape
   reads back value-exact, and pylance reads ours.
2. **Scylla writer** — `lc_writer_impl` reusing `fragment_shredder` +
   `shred()`; Index/Summary/Filter/Scylla.db/TOC components; `lc` version
   enum + guards; statistics + large-data metadata (the B1 lesson: write
   them from day one).
3. **Scylla reader** — `lance::make_reader`/`make_full_scan_reader`
   modeled on `pq_reader`'s window structure (bounded → point windows via
   index ordinal; unbounded → streaming windows), `reassemble()` on top;
   footer-equivalent metadata cache hung off the sstable like
   `_pq_footer`.
4. **DDL + write-path** — `storage_format='lance'`, options map, feature
   gate, compaction/flush/streaming/load dispatch, DDL rejection of
   non-frozen collections/counters.
5. **Tests** — format-layer suite (standalone), boost round-trip suite
   (`sstable_lance_test`), mutation-source conformance, DDL tests, a
   cqlpy end-to-end suite asserting on-disk files are `lc` and re-reading
   with BYPASS CACHE (the "cannot pass on a build that quietly wrote
   native" property).
6. **Benchmarks** (§6), then results land here as dated sections.

## 6. Benchmark plan

Reuse the pq harness, adding a third arm:

- **In-tree**: `test/boost/sstable_lance_perf_test.cc` — three arms
  (native me / pq / lc) over identical mutations in one process: cold and
  steady point reads, full scans, on-disk size. This is the fast
  inner-loop signal, same env-knob style as `sstable_parquet_perf_test`.
- **Node-level size**: `~/pq-lab/measure_native_vs_pq.sh` extended to a
  three-way (`ALTER … storage_format` per arm, major compact, assert
  every Data.db is the expected format, sum bytes). Datasets: the
  synthetic time series (scale_test.py shape) and NOAA ISD — the
  pro-Parquet cases — plus a fat-payload/point-read table shaped for
  Lance's claimed strengths (keys + ~1 KB text payload), which the pq
  roster lacked.
- **Node-level point reads**: `~/pq-lab/pointread_v2.py` methodology
  verbatim (production caching + BYPASS CACHE, first pass reported,
  bypass verified, min-of-N with canary) with an `lc` arm.
- **Scans**: `scanpath.sh`-style bounded/unbounded windows comparison.

Expected outcomes to confirm or refute: lc point reads ≪ pq and
approaching native; lc size between native-zstd and pq (worse than pq on
time series for the §2 reasons, competitive on fat-payload tables); scans
within the same order as pq.

## 7. Results

Dated, like the parquet doc's 10. Environment for everything below: the
shared 32-core lab machine, dev-mode build, single shard.

### 7.1 (2026-09-01) Implementation landed; conformance is bidirectional

Phases 1-5 of 5 are done on the `lance` branch. The container and the
structural encodings were verified against pylance 11.0.0 in both
directions: the official reader decodes our files value-exact (plain and
zstd, miniblock and full-zip, nullable and all-null pages, multi-page
columns), and our reader decodes pylance-written files byte-identically to
pylance's own decode -- including the FastLanes-bitpacked definition levels
and integers the official writer emits unconditionally at any real size
(`sstables/lance/run_tests.sh`, suites 4 and 5). Two deliberate reader
gaps, both rejected loudly rather than misread: FSST (the official writer
picks it for compressible strings; ours never emits it) and dictionary
miniblocks.

pylance also opens the Data component of a real `lc` sstable and sees the
CQL columns plus the folded metadata channels (`__ts`, `__tsx_mask`,
`__tsx_vals`) -- external readability holds end to end, losslessness is
pinned by `test/boost/sstable_lance_test.cc` (tombstones, TTLs, statics,
range tombstones, multi-page files, point reads), and the pq suites stay
green on the same tree.

Three writer/reader decisions measured on the way (7.2):

- Definition levels are FastLanes-bitpacked (1 bit/value), not flat u16.
  Flat def was 16x the bytes and put `lc` at 1.9x the size of the
  *uncompressed native* format on the smoke corpus; packed def plus zstd
  chunk values brought it to 0.55x.
- Decoding slices INSIDE miniblock chunks: a five-row point read
  materialises five values, not a 4096-value chunk. For string chunks the
  value materialisation dominated the read.
- The sstable's metadata entry carries a raw-chunk cache (def + de-zstd'd
  value sub-buffers, 32 MB cap) -- the lc equivalent of pq's decompressed
  page cache. Caching *decoded* values instead was a measured mistake:
  std::string overhead made a 4096-string chunk ~30x its raw size, the cap
  thrashed, and point reads got slower than no cache at all.

### 7.2 (2026-09-01) In-tree three-way: point reads beat native, size between

`sstable_parquet_perf_test`, 20 000 partitions x 5 rows, 5 000 uniformly
random point reads, same mutations in one process, dev build:

| format | write ms | scan ms | point mean us | point p50 | point p99 | bytes |
|---|---|---|---|---|---|---|
| native (me) | 991 | 255 | 56.7 | 23.6 | 1579 | 3 994 586 |
| pq | 1 065 | 247 | 74.7 | 34.5 | 2233 | 1 277 040 |
| lc | 1 153 | 282 | **42.8** | **19.4** | **811** | 2 112 343 |

Ratios: lc/native point **0.75x** (pq: 1.32x), lc size 0.53x native
(pq: 0.32x), lc scan 1.11x native. The format's thesis -- random access --
shows up exactly where claimed, and the costs land exactly where 2
predicted: pq keeps a ~1.65x size edge over lc (delta + dictionary
encodings lc v1 does not have), lc gives up ~11% on scans and wins point
reads outright, including the tail. Both columnar arms benefit from their
internal caches here (as does native from the OS page cache); the
node-level run below adds the bypass-verified and cold-restart controls.

### 7.3 (2026-09-01) Node-level three-way

`~/pq-lab/three_way.py` -- pointread_v2.py's methodology (production
caching + BYPASS CACHE, bypass verified, min estimator, canary,
interleaved arms, cold pass after restart) over three arms and two table
shapes: the 4M-row synthetic time series (pro-Parquet by design) and a
200k-row ~1 KB-payload table (pro-Lance by design). Single node, the lab
machine at load average ~20, --smp 1, dev build; the python client floor
(canary) sat at ~250 us, so point-read differences of ~100 us here are at
the edge of resolution -- 7.2's in-process numbers are the latency
evidence, these are the end-to-end validation plus the size/scan truth.

Time series (4M rows, 20k partitions, latte-loaded, ICS, major-compacted;
all sstables verified `me`/`pq`/`lc` on disk):

| arm | Data.db bytes | vs native | bypass min us | cold min us | scan s |
|---|---|---|---|---|---|
| native | 122 710 761 | 1.00 | 498 | 522 | 4.93 |
| pq | 11 332 974 | **0.092** | 398 | 427 | **3.08** |
| lc | 58 842 176 | 0.48 | 512 | 587 | 6.03 |

Exactly the 2 prediction for the pro-Parquet shape: sorted timestamps and
low-cardinality strings are DELTA/dictionary territory, pq crushes size
(10.8x smaller than native; lc "only" 2.1x smaller), and at these sizes
pq's whole working set fits its internal caches, so it wins the bounded
probes too. Write throughput was equal across arms (71/76/81 s loads).

Blob shape (200k rows x ~1 KB payload): first run, before per-value zstd
landed in full-zip, lc was 209 MB (raw values) against pq 10.1 MB and
native 23.6 MB, with lc bypass point reads at 379 us against native 477
(pq's 287 was flagged SUSPECT -- its 10 MB file serves from internal
caches, the bypass-slower-than-warm check failed). Re-measured with
per-value zstd in full-zip (writer takes it per page only when it shrinks
the bytes; incompressible payloads stay raw):

| arm | Data.db bytes | vs native | bypass min us | cold min us | scan s |
|---|---|---|---|---|---|
| native | 23 579 921 | 1.00 | 459 | 469 | 0.53 |
| pq | 10 122 705 | 0.43 | 285 (SUSPECT) | 293 | 0.39 |
| lc | 19 189 811 | 0.81 | 484 | 480 | 0.85 |

lc dropped 10.9x (209 MB -> 19.2 MB) for a scan tax (0.85 s vs 0.53) --
the transparent-compression trade in one row: every value decompresses
alone (point reads keep their 1-2 IOPs), so a scan pays one small zstd
frame per value. Point reads sit at native parity within the 26% canary
spread; pq's bypass number failed its own validity check both runs (the
10 MB file lives in its internal caches), so it measures those caches, not
the format.

### 7.4 (2026-09-01) Verdict so far

The 2 table holds up against measurement:

- **Point-read-heavy, mixed or fat-payload schemas: lc.** In-process,
  0.75x native / 0.57x pq mean latency with the best tail; at node level,
  native parity through a ~250 us client floor.
- **Scan-dominant sorted time series: pq**, and it is not close on size
  (0.092x native vs lc's 0.48x on the synthetic corpus) -- DELTA +
  dictionary encodings are exactly what that shape wants and lc v1 does
  not have them.
- **Native** keeps the smallest write amplification surface and the best
  cold single-row economics on narrow rows (one contiguous read vs one
  window per column), and remains the only format for
  collection/counter-heavy schemas until lc grows repeated leaves.

## 8. Risks and open questions

- **Byte-level layout fidelity.** Miniblock chunk internals and fullzip
  control words are specified by the Rust implementation + prose docs,
  not the protos. Mitigation: golden-file conformance against pylance in
  CI from day one; any bit-level ambiguity resolved by hexdump, not by
  reading intent.
- **Size regression risk vs pq is real and accepted for v1** (no delta,
  no dictionary, 4 KiB compression windows). The benchmark exists to
  quantify it; encodings can be added behind the same page metadata.
- **Windows vs chunks in the Scylla reader.** pq_reader's 512-row point
  windows were tuned for Parquet page geometry; lc wants "exactly the
  chunks covering the requested rows". Getting this wrong would bury the
  format's advantage under reader overhead — the perf test's job.
- **Metadata parse cost.** Lance pushes complexity from the footer to
  per-column metadata; a 200-column table means 200 small proto parses.
  Mitigated by the CMO table (only touched columns) and the sstable-level
  metadata cache; measure, don't assume.
- **pylance as oracle drift.** Golden files pin pylance 11.0.0; version
  bumps may change writer choices (not the format). Keep goldens in-tree.
