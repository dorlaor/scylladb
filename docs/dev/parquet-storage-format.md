# Parquet as a ScyllaDB Storage Format — Requirements, Plan and Design

**Status:** Draft / design phase. No implementation yet.
**Branch:** `parquet`
**Created:** 2026-08-15
**Owner:** Dor Laor

This document is the working memory for the project. It carries the originating
prompt, the requirements derived from it, the research findings, the design, the
configuration surface, and the (initially empty) result tables to be filled in as
measurements land. Update this file in place as decisions are made — do not fork it.

---

## 0. Originating prompt

> This job is a new project, to plan how to add Parquet format to ScyllaDB. First,
> only plan to add parquet mode, without coding. There was an existing implementation
> to add parquet support - here:
> https://www.scylladb.com/2020/08/05/scylla-student-projects-part-i-parquet/ its code
> is here: https://github.com/michoecho/parquet4seastar there is a thesis work with
> detailed results here: https://www.scylladb.com/wp-content/uploads/zpp_parquet.pdf
>
> You need to write a detailed plan and design. First, research about the key goal.
> Parquet should use less disk space, significantly than sstables. We'll need to
> compare it against dictionary compression sstables and novel parquet compression.
> There may be different levels of compression as a function of the schema.
> Throughput and latency should be similar, random access queries may be a bit
> slower, scans may be fasters.
>
> Suggest a way to control the parquet format configuration, with parameters like the
> number of rows per segment.
>
> The user will have ability to define tablets as parquet or cql and switch
> dynamically between them. Only one of them will work at any given point in time. As
> parquet is ideal with large files, it would be ok to have tablets where the initial
> LSM layers are in sstable format and the bottom most large layers in parquet. What
> would be the creteria for them?
>
> Beyond performance, describe the other considerations. Backup and restore will need
> to work and all other features.
>
> Add the prompt to the md plan and design file. This file will be use as memory for
> this and include the requirements, plan, design and configuration results

---

## 1. Requirements

Derived from the prompt. `R-n` identifiers are referenced throughout the design.

### 1.1 Functional

| ID | Requirement |
|----|-------------|
| R-1 | Parquet is available as an alternative on-disk format for the SSTable **Data** component, alongside the native `mc`/`md`/`me`/`ms`/`mt` formats. |
| R-2 | Format is selectable **per table**, as a schema property. A table is in `sstable` ("cql") mode or `parquet` mode; exactly one is authoritative at any instant. Tablets are an internal distribution unit and play no part in this. |
| R-3 | The mode can be switched **dynamically** on a live table, without downtime and without blocking reads or writes. |
| R-4 | A **hybrid** mode: upper (small, hot) LSM tiers stay in native SSTable format; the bottom-most (large, cold) tiers are Parquet. |
| R-5 | The Parquet format configuration (row group sizing, page sizing, encodings, compression) is user-controllable with sane defaults. |
| R-6 | Round-trip must be **lossless**: cell timestamps, TTL/expiry, all tombstone kinds, static rows, collections, counters (or explicitly excluded — see §7.1). |
| R-7 | Backup, restore, snapshot, repair, streaming, tablet migration/split/merge, scrub, and all other existing features continue to work. |

### 1.2 Non-functional targets

| ID | Target |
|----|--------|
| R-8 | **Disk space: significantly smaller than SSTables.** The honest bar is not LZ4 — it is `ZstdWithDictsCompressor` (§3.2). Target ≥ 25 % additional saving vs. dictionary-compressed SSTables on favourable schemas; **no regression** (> 0 %) on any supported schema. |
| R-9 | Write throughput within 10 % of native; flush/compaction CPU increase bounded and accounted. |
| R-10 | Scan throughput **≥** native; column-projected scans substantially better. |
| R-11 | Single-partition point-read latency regression bounded — target p99 ≤ 1.2× native for cache misses; no change for cache hits (row cache sits above the format). |
| R-12 | No reactor stalls attributable to the format (bounded page sizes, seastar-native I/O). |
| R-13 | Memory: row-group buffering must be explicitly budgeted and back-pressured; it must not be able to OOM a shard. |

### 1.3 Explicit non-goals (v1)

- Replacing the native format as the default.
- Serving as the *only* format for a table — the native path stays.
- Parquet modular encryption (Scylla's file-level encryption is used instead — §7.5).
- Parquet as a *write-ahead* / memtable-flush target for hot tables (the bottom-tier
  case is the value; see §6).

---

## 2. Prior art: what the 2020 project actually delivered

Sources: the [ScyllaDB blog post](https://www.scylladb.com/2020/08/05/scylla-student-projects-part-i-parquet/),
the [thesis](https://www.scylladb.com/wp-content/uploads/zpp_parquet.pdf) (Abrahamyan,
Chojnowski, Czajkowski, Karwowski; Univ. of Warsaw, May 2020), and
[parquet4seastar](https://github.com/michoecho/parquet4seastar).

### 2.1 Deliverables

1. **`parquet4seastar`** — a from-scratch, fully Seastar-native Parquet reader/writer.
   Apache Arrow's C++ Parquet was rejected: ~300 k LoC, blocking I/O throughout,
   incompatible memory/error conventions. The team first *tried* the Arrow route, got
   it down to ~30 k LoC and working, then discarded it as unmaintainable and rewrote.
   That negative result is worth remembering.
2. **`parquet2cql`** — Parquet → CQL `CREATE`/`INSERT` converter (demo/validation only).
3. **A proof-of-concept Scylla module** that wrote memtable flushes to `.parquet`
   *alongside* the normal SSTable, purely to measure size. ~50 lines of change to
   existing Scylla code plus the module. It was never a read path.

### 2.2 Library architecture (still a good shape)

`file_reader` → deserializes Thrift footer, preprocesses schema → spawns
`column_chunk_reader` per column of interest → yields `(value, definition level,
repetition level)` batches → optional `record_reader` performs Dremel record assembly
via a callback `Consumer` (no materialized record object, to avoid allocations).
Writer side mirrors it, minus a high-level `record_writer` (record *shredding* was
done in the Scylla module instead).

Notable implementation choices:
- Reference-counted strings out of the reader, so dictionary-encoded values are shared
  rather than copied. Measured as a **net loss** for short strings (see §2.4) — the team
  flagged it for redesign.
- Reactor stalls: unavoidable in the general case, because Parquet permits pages up to
  4 GB and the external compression libraries have no streaming interface. Mitigation is
  a **reader-side page size cap**. Confirms R-12 must be enforced by policy, not hope.
- No page-level statistics written; no encryption.

### 2.3 SSTable → Parquet schema mapping (thesis Appendix B)

```
message partition {
  required group header {
    required group partition_key { required $X_TYPE $X; ... }
    required group deletion_time { required int32 local_deletion_time;
                                   required int64 marked_for_delete_at }
    required group shadowable_deletion_time { ... same two ... }
  }
  optional group static_row {
    required int32 flags (UINT8); required int32 extended_flags (UINT8);
    required group cells { optional group $X { required int32 flags (UINT8);
        optional int64 timestamp; optional int32 local_deletion_time;
        optional int32 marked_for_delete_at; optional $X_TYPE value } ... }
  }
  repeated group rows {
    required group row {
      required int32 flags (UINT8); required int32 extended_flags (UINT8);
      required group clustering_key { optional $X_TYPE $X; ... }
      required group regular { optional group $X { required int32 flags (UINT8);
          optional int64 timestamp; optional int32 local_deletion_time;
          optional int32 marked_for_delete_at; optional $X_TYPE value } ... }
    }
  }
}
```

**This mapping is the single most important thing to fix.** It explodes every CQL
column into ~5 Parquet leaf columns. For a 100-column table that is ~500 column chunks
per row group, each with page headers and footer metadata. §5.3 addresses it.

Also noted: Scylla writes cell timestamps as VLQ deltas; Parquet has no VLQ, so the
module used `DELTA_BINARY_PACKED`, which achieves the same end by bit-packing.

### 2.4 Measured results (2020)

**Library performance vs. Arrow C++** — ratio of parquet4seastar time to Arrow time,
so < 1.0 means faster. Roughly at parity across `{int32,int64,short/long string} ×
{plain,dict} × {uncompressed,snappy}`, with one bad cell: **dictionary-encoded short
strings ~1.5× (read) and ~2.4× (write) slower**, attributed to `std::unordered_map` +
`std::hash` vs. Arrow's custom table + xxHash, and to the refcounted-string design.
Both are fixable and neither is fundamental.

**Disk usage vs. SSTables** — 1 M rows, SSTable `mc` + `DeflateCompressor` vs. Parquet
+ `GZIP`, strings dictionary-encoded, ints delta-encoded. `Ratio = Parquet / SSTable`,
lower is better for Parquet.

*Scenario 1 — `(int PK, int value)`, varying duplicate count:*

| Copies | SSTable KiB | Parquet KiB | Ratio |
|-------:|------------:|------------:|------:|
| 1 | 9 451.53 | 6 235.06 | **65.97 %** |
| 10 | 10 397.87 | 7 344.08 | 70.63 % |
| 100 | 12 561.91 | 10 150.82 | 80.81 % |
| 1 000 | 13 855.67 | 10 182.99 | 73.49 % |
| 10 000 | 14 050.46 | 10 196.17 | 72.57 % |

*Scenario 2 — `(int PK, text value)`, varying string length and duplicate count:*

| Copies | Str size | SSTable KiB | Parquet KiB | Ratio |
|-------:|---------:|------------:|------------:|------:|
| 1 | 16 | 10 217.27 | 6 243.37 | 61.11 % |
| 10 | 16 | 12 326.25 | 6 658.52 | 54.02 % |
| 100 | 16 | 18 228.22 | 7 111.31 | **39.01 %** |
| 1 000 | 16 | 21 210.09 | 11 254.82 | 53.06 % |
| 10 000 | 16 | 21 539.72 | 14 921.21 | 69.27 % |
| 1 | 100 | 14 658.61 | 6 240.50 | 42.57 % |
| 10 | 100 | 31 736.99 | 6 662.75 | 20.99 % |
| 100 | 100 | 62 857.07 | 7 155.45 | **11.38 %** |
| 1 000 | 100 | 69 532.10 | 41 687.74 | 59.95 % |
| 10 000 | 100 | 70 313.91 | 54 906.48 | 78.09 % |

*Scenario 3 — 100-column table, varying how many columns per row are non-NULL:*

| Non-null cols | SSTable KiB | Parquet KiB | Ratio |
|--------------:|------------:|------------:|------:|
| 0 | 9 306.79 | 7 438.49 | 79.93 % |
| 20 | 55 204.31 | 83 483.61 | **151.23 %** ← regression |
| 40 | 84 657.85 | 93 474.25 | 110.41 % ← regression |
| 60 | 104 103.20 | 94 733.85 | 91.00 % |
| 80 | 110 125.07 | 94 589.21 | 85.89 % |

Thesis conclusion: 20–50 % average saving for numeric and string data; **worse than
SSTables when the NULL fraction is near 0.5**, because their mapping spent up to 2 bits
per NULL (definition levels) where SSTable 3.0 spends 1 bit, and RLE cannot help when
the levels are near-random.

### 2.5 Reading the prior art critically

Three caveats before treating those numbers as our expectation:

1. **The baseline is obsolete.** `Deflate` on `mc` in 2020 is not the 2026 bar. Today
   the comparison must be against `ZstdWithDictsCompressor` (§3.2), which captures
   cross-chunk redundancy — precisely the axis on which Parquet's dictionary encoding
   was winning in Scenario 2.
2. **The Parquet side was also unoptimised.** GZIP, no page index, no
   `BYTE_STREAM_SPLIT`, no page statistics, naïve cell-metadata mapping, default row
   groups. Both sides move.
3. **Scenario 3's regression is a mapping artefact, not a format property.** 151 % on a
   sparse wide table is what the 5-leaves-per-column mapping costs. §5.3 is the fix, and
   re-running Scenario 3 is the first acceptance test.

---

## 3. Research: the disk-space goal

### 3.1 Where a columnar layout structurally wins

- **Homogeneous byte streams.** A column chunk holds one type from one column, so the
  block compressor sees far more regularity than an interleaved row layout ever can.
- **Type-aware encodings applied before the block compressor.** `RLE_DICTIONARY`,
  `DELTA_BINARY_PACKED` (ints/timestamps), `DELTA_LENGTH_BYTE_ARRAY` and
  `DELTA_BYTE_ARRAY` (sorted/prefixed strings), `BYTE_STREAM_SPLIT` (floats/doubles —
  transposes mantissa bytes so zstd sees runs). Zstd-with-dicts on a row layout cannot
  reach these: it is a general-purpose codec over mixed bytes.
- **Scylla's per-cell timestamps.** These are 8 bytes on every cell, currently
  delta-VLQ-encoded *within a partition*. In a columnar layout the timestamps of an
  entire row group form one column and delta-pack across partitions. For write-once
  time-series this column collapses to near-nothing. **This is likely our single
  biggest structural win, and it is Scylla-specific.**
- **Column pruning at read time** — not a space win, but an I/O win that compounds with
  object storage (§7.4).

### 3.2 The real baseline: dictionary-compressed SSTables

Already in tree and shipping:

- `compressor::algorithm` = `{lz4, lz4_with_dicts, zstd, zstd_with_dicts, snappy, deflate, none}`
  ([sstables/compressor.hh](sstables/compressor.hh:23)).
- `ALTER TABLE ... WITH compression = {'sstable_compression': 'ZstdWithDictsCompressor'}`.
- `default_sstable_compressor_factory`
  ([sstables/sstable_compressor_factory.hh](sstables/sstable_compressor_factory.hh:63))
  shares one dictionary across all shards in a NUMA group, with content-hash-derived
  owner shards.
- `sstable_dict_autotrainer` ([sstable_dict_autotrainer.cc](sstable_dict_autotrainer.cc:26))
  retrains per table on a period, gated on `min_training_dataset_bytes` and
  `min_training_improvement_factor`, sampling ~4096 blocks (Hoeffding-bounded).
- REST: `/storage_service/retrain_dict`, `/storage_service/estimate_compression_ratios`
  ([api/storage_service.cc](api/storage_service.cc:1389)).
- Default chunk length is now **4 KiB** (`compression_parameters::DEFAULT_CHUNK_LENGTH`),
  which is what makes shared dictionaries pay off — small chunks compress badly alone.

Consequence for us: **a shared dictionary already recovers most of the cross-row
redundancy that Parquet's dictionary encoding captures.** The remaining Parquet
advantage is the encoding layer (delta, bit-packing, byte-stream-split) and the
column-homogeneity of the input to the codec. That advantage is real but narrower than
the 2020 numbers suggest. Any go/no-go decision must be measured against
`ZstdWithDictsCompressor`, never against LZ4.

There is also a *symmetry* to exploit: nothing stops us from feeding Parquet's block
compressor a **shared zstd dictionary** too, reusing the same trained dictionary and the
same factory. Parquet permits arbitrary zstd streams; a dictionary-primed zstd frame is
still a valid zstd frame provided the reader has the dictionary. That makes the file
non-self-describing to external tools, so it must be an opt-in
(`'compression': 'zstd_with_dicts'`) that flips off external readability. See §5.6.

### 3.3 Where Parquet loses, and what to do

| Loss mechanism | Effect | Mitigation |
|---|---|---|
| Definition levels on sparse columns (~1–2 bits/value vs. 1 bit in SSTable flags) | Regression when NULL fraction ≈ 0.5 | Mark columns `required` when the row group has no NULLs; RLE levels; §5.3 metadata folding removes most level columns entirely |
| Cell-metadata column explosion (5 leaves/column) | Metadata + page-header overhead dominates wide sparse tables | §5.3 — folding levels; omit non-materialised metadata columns per file |
| Small files | Footer + per-chunk metadata is a fixed cost | Only use Parquet where output is large (§6 criteria) |
| Row-group buffering | Memory pressure | Byte-budgeted row groups + semaphore (§5.5, R-13) |
| Large opaque blobs | Already incompressible; columnar buys nothing | Schema eligibility check predicts ~0 gain → policy declines |
| High-cardinality text | Dictionary fails; falls back to plain + zstd ≈ baseline | Expected parity, not regression |
| Rewrite cost on merge | Re-encode + recompress an entire run | Prefer ICS (run fragmentation bounds the rewrite unit) — §6.4 |

### 3.4 Expected saving as a function of schema

The prompt asks for compression levels as a function of schema. This is the working
hypothesis table — **to be replaced with measurements** (§9). Ratios are
`Parquet / SSTable`, lower is better; two baselines given because the choice of baseline
changes the story.

| Schema archetype | vs. LZ4 (default) | vs. Zstd+dicts | Rationale |
|---|---|---|---|
| Time-series / IoT: `(device, ts)` + few numeric cols, append-only, dense | 0.25–0.40 | **0.55–0.70** | Delta-packed clustering key and timestamps; byte-stream-split floats; near-zero tombstones |
| Event log / append-only wide-ish, low-cardinality enums + text | 0.30–0.45 | 0.60–0.75 | Dictionary + RLE on enums; timestamp column collapse |
| User-profile / KV: pk-only, many dense text cols | 0.45–0.60 | 0.80–0.95 | Some column homogeneity; dicts already capture most of it |
| Mixed OLTP orders: numeric + text + timestamps, dense | 0.40–0.55 | 0.70–0.85 | Moderate, broad-based win |
| **Wide sparse (100 cols, ~20 % filled)** | 0.9–1.5 | **1.0–1.6 (regression risk)** | Level + metadata overhead; the Scenario-3 case. Must be fixed by §5.3 or excluded by policy |
| Large-blob store (pk + ≥ 64 KiB blob) | 0.95–1.05 | 0.98–1.05 | Payload dominates and is opaque |
| Counter tables | n/a | n/a | Excluded in v1 (§7.1) |
| Non-frozen collection-heavy | 0.8–1.2 | 0.9–1.3 | Per-cell metadata per element; needs the folding work to be viable |

Read this table as: **the Parquet case is strongest for append-mostly, dense,
type-regular data, which is exactly the data that ends up in the bottom LSM tier of a
large table.** That is the same conclusion the hybrid design (§6) reaches from the
write-amplification direction, which is a good sign.

### 3.5 "Novel Parquet compression" — the 2026 landscape

Worth knowing, and worth *not* chasing in v1:

- Within Parquet, the modern settings are: **zstd** (best ratio/speed trade-off today),
  V2 data pages, `BYTE_STREAM_SPLIT` for floats, `DELTA_*` for ints and sorted strings,
  the **page index** (per-page min/max + null counts, enabling page skipping) and
  optional **bloom filters** per column. parquet4seastar implements none of the last
  three. They are cheap to add and directly serve R-10/R-11.
- Outside Parquet, a wave of formats — **Vortex, Lance, Nimble, BtrBlocks, FastLanes** —
  argue Parquet's 2013 assumptions (batch scans, modest widths, heavyweight codecs) no
  longer hold now that NVMe is fast and decompression is the bottleneck. They push
  cascading lightweight encodings and compute-on-encoded data.
- **Recommendation:** target Parquet, not a novel format. Interoperability is a large
  part of the value (§7.4) and the alternatives are not stable ecosystem targets. But
  keep the writer's encoding selection behind an internal interface so a future
  cascading-encoding backend is a substitution, not a rewrite.

---

## 4. Throughput and latency expectations

Restating the prompt's targets with mechanism, so we know what to measure.

| Path | Expectation | Mechanism |
|---|---|---|
| Write (flush) | Parity to −10 % | Same mutation-fragment stream; extra cost is buffering + encode. Only relevant if Parquet is used at flush time, which the hybrid design mostly avoids |
| Write (compaction) | −5 to −20 % CPU-bound | Encode + zstd on the write side; offset by less I/O |
| Full scan, all columns | Parity to +30 % | Less I/O, more decode |
| **Scan with column projection** | **1.5–5×** | The headline win; reads only the projected column chunks |
| Aggregations / analytics | Large win | Projection + page-index skipping via min/max |
| **Single-partition point read (cache miss)** | **−10 to −40 % latency** | N column-chunk page decodes instead of one contiguous row read. Worse the wider the `SELECT *` |
| Single-partition point read (cache hit) | **No change** | Row cache is above the format entirely |
| Range read within a partition | Parity to slight loss | Page index limits the damage |

Two things keep R-11 achievable:

1. **The row cache is unaffected.** Only misses reach the format. For latency-sensitive
   workloads the cache hit rate is what matters, and it does not move.
2. **We keep Scylla's own index components** next to the Parquet data (§5.4), so
   partition lookup stays O(1) rather than degrading to a row-group scan.

Honest statement of the regression: `SELECT *` on a wide table, cache-missing, will be
measurably slower in Parquet. That is inherent to columnar and is the price of R-8. The
hybrid design (§6) confines it to cold bottom-tier data, and the policy engine (§6.3)
can decline Parquet for point-read-dominated tables.

---

## 5. Design

### 5.1 Where it plugs in

The extension points already exist and are clean:

- **Write side.** `sstables::sstable_writer::writer_impl`
  ([sstables/writer_impl.hh](sstables/writer_impl.hh:21)) is a pure virtual consumer of
  the mutation-fragment stream: `consume_new_partition`, `consume(tombstone)`,
  `consume(static_row&&)`, `consume(clustering_row&&)`,
  `consume(range_tombstone_change&&)`, `consume_end_of_partition`,
  `consume_end_of_stream`. **A Parquet writer is a second implementation of this
  interface.** Nothing above it needs to know.
- **Read side.** `sstable::make_reader` / `make_full_scan_reader`
  ([sstables/sstables.cc](sstables/sstables.cc:3081)) already switch between
  `mx::make_reader` and `kl::make_reader` on version. A third arm dispatches to
  `pq::make_reader`.
- **Version enum.** `sstable_version_types { ka, la, mc, md, me, ms, mt }`
  ([sstables/version.hh](sstables/version.hh:17)). Add `pq` (or better — see below).
- **Schema-property precedent.** `compression` is the model to follow: a table-level
  property that selects how SSTable bytes are encoded, changed with `ALTER TABLE`, with
  existing files converted by a background rewrite. `storage_format` is the same shape
  with a different encoder.

**Design decision — a new sstable version.** Parquet keeps the LSM, the compaction
manager, the sstable set, the index components and the whole mutation-fragment pipeline;
it only replaces the encoding of the Data component. It is therefore modelled as a **new
sstable version, `pq`**, added to `all_sstable_versions` and `writable_sstable_versions`
— not as a new storage engine, which would imply replacing far more than the encoding.
This gives us TOC handling, component naming, backup/restore, streaming and the
sstables registry for free.

### 5.2 Component layout

A "Parquet SSTable" is a normal SSTable whose Data component is a valid `.parquet` file:

| Component | Content |
|---|---|
| `Data.parquet` | The rows, as a standards-compliant Parquet file (§5.3) |
| `Partitions.db` / `Index.db` | Unchanged. BTI trie index (`ms`/`mt` lineage) or classic index — gives O(1) partition lookup. Entries point at `(row_group, row_offset)` instead of a byte offset |
| `Filter.db` | Unchanged bloom filter over partition keys |
| `Statistics.db`, `Scylla.db`, `TOC.txt`, `Digest.crc32` | Unchanged; `Scylla.db` carries the Parquet-specific metadata (folding level, whether a shared dict was used, writer config digest) |
| `CompressionInfo.db` | **Absent** — compression is internal to Parquet |

Keeping our own index is the key pragmatic call: it preserves R-11 and it means the
promoted-index / partition-index-cache machinery
([sstables/partition_index_cache.hh](sstables/partition_index_cache.hh)) keeps working.
The `.parquet` file remains independently valid and readable by external tools; our
extra components are side files they ignore.

### 5.3 Logical mapping — metadata folding

The core of the design, and the fix for the Scenario-3 regression.

The naïve mapping (§2.3) emits, per CQL column: `flags`, `timestamp`,
`local_deletion_time`, `marked_for_delete_at`, `value`. We instead define **folding
levels**, chosen per output file by the writer, recorded in the Parquet key-value
metadata and in `Scylla.db`:

**Level 0 — verbatim.** The 2020 mapping. Always lossless, largest. Fallback only.

**Level 1 — row-folded (default).** *Amended 2026-08-16: exceptions use a sparse
side-channel, not per-column leaves — see §10.3c.*
- One `__ts` column per *row*, not per cell, holding the row's dominant (modal) cell
  timestamp; typically every cell of a row shares it, because the row came from one
  statement.
- Per-cell timestamps are emitted only into a sparse `__ts_exc` column, present only if
  at least one row in the row group actually has divergent cell timestamps.
- `local_deletion_time` / `marked_for_delete_at` / TTL columns materialise **only if
  the row group contains any non-trivial value**. A row group with no TTLs and no
  tombstones carries zero deletion columns.
- `flags`/`extended_flags` are reconstructed from definition levels plus the folded
  metadata rather than stored.

Result for the common case (dense, write-once, no TTL): **one leaf column per CQL
column, plus one `__ts` column for the whole row** — roughly a 5× reduction in column
count vs. Level 0, and the Scenario-3 overhead largely disappears.

**Level 2 — row-group-uniform.** If every row in the row group shares one timestamp and
there are no TTLs/tombstones, `__ts` degenerates to a single key-value metadata entry
and vanishes from the data entirely. The writer detects this and falls back to Level 1
if the precondition breaks mid-group.

**Level 3 — logical / analytics.** Cell metadata dropped entirely; the file is exactly
the user's CQL schema. **Lossy — export only, never a storage mode.** Used by the
`EXPORT` path (§7.4).

Which columns were materialised is recorded explicitly, so the reader knows a missing
column means "trivially absent" rather than "unknown". Omitted-column defaults live in
the file's key-value metadata.

Type mapping (`$X_TYPE`) follows the thesis for scalars and Parquet logical type
annotations for the rest; `decimal`, `varint`, `inet`, `duration`, `date`, `time`,
`timeuuid` map to the corresponding Parquet logical types over `BYTE_ARRAY`/`INT32`/
`INT64`/`FIXED_LEN_BYTE_ARRAY`. Frozen collections and UDTs are opaque `BYTE_ARRAY` in
v1 (they already are, internally); non-frozen collections use Dremel `repeated` groups.

### 5.4 Ordering and random access

A Parquet file used as an SSTable **must be sorted by `(token, partition key,
clustering key)`**, same as native. That is free — the mutation-fragment stream is
already sorted. It buys:

- Correct merge semantics in the sstable set with no changes to the merging reader.
- Row-group min/max statistics that act as a coarse sparse index.
- The **page index** turning into a fine-grained skip index for range scans.
- `DELTA_BYTE_ARRAY` prefix compression actually paying off on the key columns.

Point lookup: `Partitions.db` → `(row_group, row_index)` → page index → decode only the
pages of the projected columns that cover that row index. Compared to native, that is
one extra indirection and *k* page decodes for *k* projected columns. Bloom filters on
the key columns can be written too, but our `Filter.db` already covers partition-key
existence, so the Parquet bloom filter is only interesting for secondary predicates —
defer.

### 5.5 Row groups, pages and memory (R-13)

Columnar writing requires buffering a whole row group before it can be emitted. This is
the main new resource risk and must be designed for, not discovered.

- Row group size is bounded by **both** `row_group_rows` and `row_group_bytes`,
  whichever trips first.
- Classic Parquet advice (128 MB – 1 GB, sized to an HDFS block) is **wrong for
  Scylla**. Concurrent compactions × shards × row group bytes is the memory bill.
  Proposed default: **64 MiB** uncompressed, with `row_group_rows` default 1 000 000.
- Buffering is charged against a dedicated semaphore
  (`parquet_writer_memory_budget`, default a small fraction of shard memory) so that
  compaction back-pressures rather than OOMs.
- Page size default **64 KiB** (thesis used 64 KiB; also what caps decompression work).
  A hard reader-side cap (`max_page_bytes`, default 4 MiB) rejects pathological files —
  this is the R-12 guard the thesis explicitly recommends.
- Trade-off to measure: larger row groups → better ratio, worse point-read latency and
  more memory. This is the primary tuning axis and the reason §8 exposes it.

### 5.6 Compression inside Parquet

Reuse `sstable_compressor_factory` rather than introducing a second compression stack:

- `'compression'` ∈ `{none, lz4, snappy, zstd, gzip, zstd_with_dicts}` mapped onto
  Parquet codec ids.
- `zstd_with_dicts` primes each page's zstd frame with the table's trained dictionary
  from the existing autotrainer. **Flips off external readability** (an external reader
  lacks the dictionary), so it is opt-in and surfaced in `Scylla.db` and in
  `DESCRIBE TABLE`. This is the configuration that competes hardest with
  `ZstdWithDictsCompressor` and is the most interesting cell in the results matrix.
- Encoding selection is automatic per column per row group (dictionary until the
  dictionary page exceeds `dictionary_page_max_bytes`, then fall back to
  plain/delta), with per-column user overrides.

---

## 6. Format control and hybrid tiering

> **Design correction, 2026-08-16.** An earlier draft made the format selectable *per
> tablet*, with a `tablet_storage_format` field in `tablet_info` and group0 plumbing to
> match. That was wrong. A tablet is an internal unit of distribution and rebalancing;
> it is not a thing users reason about, and the encoding of an SSTable has nothing to do
> with which tablet its data belongs to. Coupling the two would have put a user-facing
> knob at an internal abstraction, and would have allowed one table's data to exist in
> two encodings for no reason a user could explain.
>
> **The storage format is a property of the table.** Everything below follows from that,
> and the whole tablet-metadata subsystem the earlier draft required simply disappears.

### 6.1 Model

One setting, on the table, in the schema:

```cql
CREATE TABLE ks.t (...) WITH storage_format = 'sstable';   -- default
ALTER  TABLE ks.t       WITH storage_format = 'hybrid';    -- sstable | parquet | hybrid | auto
```

That is the entire control surface. There is deliberately no per-tablet override, no new
system table, and no group0 work: schema properties already replicate through the schema
tables, so the mechanism exists and is well understood.

**This is exactly how `compression` already behaves**, which is the precedent worth
leaning on. Changing a table's compressor today is an `ALTER TABLE` followed by a
background rewrite; changing its storage format is the same operation with a different
output encoder. Nothing about that reasoning involves tablets.

### 6.2 Switching is a write-side policy

- **The setting decides what new SSTables are written as.** At any instant a table has
  exactly one storage format, and every flush and compaction output obeys it.
- **Reads are format-agnostic.** The SSTable set merges readers regardless of version;
  `mc` and `md` already coexist today. During conversion both encodings are present —
  necessarily, since conversion is not atomic over terabytes.
- A table is **converged** when no SSTable of a non-target format remains.

The sequence:

1. `ALTER TABLE` → schema change propagates. **Instant.**
2. Every replica begins honouring it for new outputs.
3. A background rewrite converts existing SSTables — a compaction variant
   (`compaction_type_options::rewrite_to_format`), low priority, resumable, abortable,
   throttled by the compaction manager like any other. This is the same shape as
   `nodetool upgradesstables`.
4. The table reports converged.

Reverting is the same operation with the target reversed. An abort mid-flight leaves a
mixed set that the next run finishes. No data is at risk at any point.

Compaction groups happen to be per-tablet, so the rewrite naturally proceeds
tablet-by-tablet — but that is an artefact of where the LSM lives, not a design
decision, and nothing in the format layer observes it.

### 6.3 Hybrid: which LSM layers become Parquet

In `hybrid` mode the format is chosen per compaction output rather than per table. An
output is written as Parquet only if **all** of the following hold. Every threshold is a
per-table config knob (§8.3). None of them mention tablets.

**C1 — Bottom tier / terminal position.** The output must be in the largest size tier
(ICS/STCS: top size bucket; LCS: max level). Operationally: *expected remaining rewrites
≤ 1*. Converting data that will be re-compacted three more times pays the encode cost
repeatedly for no benefit.

**C2 — Size.** Output ≥ `parquet_min_output_bytes`, default **256 MiB** — at least four
64 MiB row groups, enough to amortise the footer and per-chunk metadata.

**C3 — Data age.** `max_timestamp` older than `parquet_min_data_age`, default **24 h**.
Prevents converting data that is still being overwritten.

**C4 — Low garbage.** Estimated droppable-tombstone plus shadowed-cell fraction ≤
`parquet_max_garbage_fraction`, default **10 %**. High garbage means an imminent GC
rewrite, and tombstones force the deletion metadata columns to materialise.

**C5 — Schema eligibility.** No counters; no unsupported types; folded leaf count within
`parquet_max_leaf_columns`.

**C6 — Predicted gain.** A sampling estimator predicts ≥ `parquet_min_gain_ratio`
(default **15 %**) saving versus the table's current compressor. **Do not guess —
measure.** Reuses the dictionary autotrainer's sampling path and is exposed as
`/storage_service/estimate_parquet_ratios`.

**C7 — Read-pattern gate (optional).** Decline if the table is point-read dominated and
latency-classified. Off unless `storage_format = 'auto'`.

C6 is what makes this safe: it turns "will Parquet help this schema?" — which §3.4 can
only guess at — into a measurement on the actual data, before any bytes are rewritten.

### 6.4 Compaction strategy interaction

Hybrid mode makes bottom-tier rewrites more expensive, since merging into a Parquet run
means re-encoding and recompressing it.

- **ICS is recommended.** Its runs are fragmented into bounded SSTables, so a merge
  rewrites only the overlapping fragments. That bounds exactly the write amplification
  Parquet makes expensive.
- **TWCS is an excellent fit** — old time windows are already immutable and cold, which
  is C1 and C3 for free.
- **STCS** works but a top-bucket rewrite re-encodes the whole tier; acceptable only for
  genuinely cold data.
- **LCS** works; max-level rewrites are already bounded per level.

The strategy needs one new input: for a candidate compaction, "will the output satisfy
C1–C6?" That is a call into the policy engine when the `compaction_descriptor` is built,
which then selects the writer format.

## 7. Other considerations

### 7.1 Feature compatibility matrix

| Feature | Impact | Plan |
|---|---|---|
| **Backup / snapshot** | Snapshots hard-link components listed in TOC | Works once `Data.parquet` is a known component. Add `component_type::DataParquet` or a format marker; audit `db/snapshot/backup_task.cc`, `db/snapshot/cluster_backup.cc` |
| **Restore** | Restoring into a cluster lacking Parquet support | Gate on the cluster feature; refuse restore with a clear error rather than corrupting |
| **Repair** | Row-level, operates on mutation fragments | Format-agnostic — works unchanged |
| **Streaming / tablet migration** | Some paths stream *files*, not fragments | File-streaming requires the receiver to support `pq` → gate on cluster feature. Fragment-streaming paths are unaffected |
| **Tablet split / merge** | Splits SSTables at the token boundary | Parquet split = row-group-aligned rewrite. Cheaper than native if the split point falls on a row-group boundary; otherwise a normal rewrite |
| **Scrub / validate** | `nodetool scrub`, `scylla sstable` CLI | Must learn Parquet: page CRC verification, footer validation, fragment-stream revalidation |
| **`scylla sstable` tooling** | dump/validate/write subcommands | Extend; also the natural home for the offline estimator (Phase 0) |
| **Counters** | Counter cells have shard-level internal structure | **Excluded in v1.** Policy declines counter tables |
| **CDC** | Separate log table, ordinary schema | No interaction |
| **Materialized views / secondary indexes** | Backed by ordinary tables | Work; each MV table decides its own format |
| **TTL / expiry** | Forces deletion columns to materialise | Supported; reduces the §5.3 folding win. Factored into the C6 estimate |
| **Encryption at rest** | `file_io_extension::wrap_file` wraps components | Wrap `Data.parquet` transparently (§7.5) |
| **Object storage** | S3/GS-backed SSTables | Strong synergy (§7.4) |
| **Downgrade** | Old nodes cannot read `pq` | Same procedure as dict compression: switch format, `nodetool upgradesstables -a`, wait for convergence, then downgrade. Document prominently |

### 7.2 Cluster feature and safety

A `PARQUET_SSTABLE_FORMAT` cluster feature must be enabled everywhere before any table
may switch. Until then, DDL setting `storage_format` is rejected. This mirrors the
`SSTABLE_COMPRESSION_DICTS` precedent, including the API-level guard.

### 7.3 Correctness strategy

A new on-disk format is the highest-risk change class in a database. Non-negotiables:

- **Round-trip property tests**: random schema × random mutation-fragment stream → write
  → read → compare fragment-by-fragment. Reuse the existing mutation-source test suite,
  parameterised over format — this is the single highest-value test.
- **Shadow-read verification mode** (`parquet_shadow_verify: true`): read both formats
  during convergence and compare results, logging divergence. Run in CI and optionally
  in production during rollout.
- **Fuzzing** the reader against malformed/hostile Parquet (untrusted files can arrive
  via restore/upload).
- **Cross-reader validation**: files written by us must be readable by Arrow/DuckDB/Spark
  at folding Level 3, and by `parquet-tools`. Catches spec drift.
- **Upgrade/downgrade matrix** in CI.

### 7.4 Interoperability — the strategic upside

Beyond R-8, a Parquet bottom tier means **Scylla's cold data is directly readable by
the analytics ecosystem**. Combined with object-storage-backed SSTables
([docs/dev/object_storage.md](docs/dev/object_storage.md)), the bottom tier of a table
is a set of `.parquet` objects in a bucket that Spark/Trino/DuckDB/Iceberg tooling can
consume with no export step and no ETL.

That is arguably worth more than the disk saving. It should shape two decisions:

- Keep the physical mapping as close to the logical CQL schema as folding allows (§5.3)
  — an external reader should see mostly user columns plus a couple of ignorable `__ts*`
  columns.
- Keep `zstd_with_dicts` opt-in and clearly labelled, since it forfeits this property.

An explicit `EXPORT` path emitting folding Level 3 (clean logical schema, no cell
metadata) serves users who want a pristine analytics artefact.

### 7.5 Encryption at rest

Parquet's modular encryption is not worth implementing. Scylla already wraps component
files via `file_io_extension::wrap_file`
([sstables/sstables.hh](sstables/sstables.hh:1335)); wrapping `Data.parquet` gives
encryption transparently. It makes the file unreadable to external tools — acceptable,
since EaR users are not the interop audience. Document the interaction: EaR and §7.4
are mutually exclusive.

### 7.6 Observability

Per-format SSTable count and bytes; convergence progress per table; row-group size
distribution; encode/decode CPU; compression ratio achieved vs. predicted (validates
C6); page-index skip effectiveness; row-group buffer memory high-water mark;
reactor-stall attribution.

### 7.7 The library question

Three options for the Parquet implementation:

1. **Fork `parquet4seastar`.** Proven Seastar-native design, but 6 years stale (2020,
   C++17, Seastar API drift), missing page index, page statistics, several encodings,
   and carrying the known dictionary-short-string performance bug.
2. **Apache Arrow C++.** Rejected in 2020 for good reasons that still hold (~300 k LoC,
   blocking I/O, incompatible memory model). The team *reached working code* on this
   path and threw it away — do not repeat that experiment.
3. **New in-tree implementation, borrowing parquet4seastar's structure.**

**Recommendation: option 3.** Write a focused reader/writer under `sstables/parquet/`,
reusing parquet4seastar's architecture (`file_reader` / `column_chunk_reader` /
level-triplet batches / callback-based assembly), but built against current Seastar and
wired into Scylla's own compressor factory, file abstractions, memory accounting and I/O
priority classes. We need a subset of the spec, and in-tree ownership is what makes R-12
and R-13 enforceable. Import parquet4seastar's test suite as a conformance baseline.

### 7.8 Code placement and build integration

**Established fact:** the SSTable implementation lives entirely in the main `scylladb`
repo, not in Seastar. `sstables/` is a `STATIC` CMake library target
([sstables/CMakeLists.txt](sstables/CMakeLists.txt:1)) linking `Seastar::seastar`,
`idl`, `xxHash`, `libdeflate`, `ZLIB`, `readers`, `tracing`. Seastar is a git submodule
(`.gitmodules` → `seastar`, alongside `abseil`, `swagger-ui`, `tools/python3`,
`tools/cqlsh`) and contains no storage-format code — it is the async framework only.
Every existing format lives *inside* the `sstables` target as a subdirectory:
`sstables/kl/`, `sstables/mx/`, `sstables/trie/` (the BTI index — the freshest and most
directly comparable precedent, 7 `.cc` files added as a new on-disk structure).

**Recommendation: `sstables/parquet/` in the main repo, as part of the `sstables`
target**, following the `trie/` precedent exactly. With one refinement — an internal
two-layer split, because the two halves have genuinely different dependency sets and
different testing needs:

```
sstables/parquet/
├── format/          # Layer 1: spec-level codec. NO Scylla schema types.
│   ├── thrift_compact.{hh,cc}     # TCompactProtocol reader/writer
│   ├── metadata.{hh,cc}           # FileMetaData / RowGroup / ColumnChunk structs
│   ├── encoding_*.{hh,cc}         # PLAIN, RLE_DICTIONARY, DELTA_*, BYTE_STREAM_SPLIT
│   ├── page_{reader,writer}.{hh,cc}
│   └── page_index.{hh,cc}
├── schema_mapping.{hh,cc}   # Layer 2: CQL schema ↔ Parquet schema, folding levels
├── writer.{hh,cc}           #          sstable_writer::writer_impl subclass
└── reader.{hh,cc}           #          pq::make_reader
```

Layer 1 depends only on Seastar plus the compression libraries; it knows nothing about
`schema`, `mutation_fragment` or `sstable`. That boundary is what makes it
independently fuzzable — mandatory, because restore and `upload/` can deliver untrusted
Parquet files (§7.3) — and it keeps the option of extracting it later without forcing
that decision now. Enforce the boundary by review and a header-include check
(`check_headers` is already wired up per target).

Both layers compile into the single `sstables` static library initially. A separate
CMake target buys nothing until the codec has an independent consumer.

**Rejected: a `parquet4seastar` submodule.** Scylla's submodules are all components with
lives of their own — Seastar has external users, Abseil is upstream, `cqlsh`/`python3`
are packaging. A Parquet codec coupled to `sstable_compressor_factory`, reader permits,
memory budgets and I/O priority classes has no such independent life, and the coupling
would have to be inverted through abstract interfaces purely to satisfy the repo
boundary. Worse, every design iteration in Phases 1–2 becomes a two-repo change plus a
submodule bump. The thesis named slow iteration (>30 s rebuilds per TU) as the single
biggest drag on that project; a cross-repo loop is the same mistake at a larger
granularity.

**Rejected: a Rust implementation via `rust/`.** The Rust integration exists
(`rust/Cargo.toml`) but is deliberately narrow — `wasmtime_bindings` and a small `inc`
crate, i.e. leaf computations, not I/O-driven subsystems. `arrow-rs`'s Parquet crate
assumes blocking or Tokio I/O and owns its buffer management: the same impedance
mismatch that killed the Arrow C++ attempt in 2020, in a different language.

**The one genuinely new dependency: Thrift.** Parquet metadata (the footer, page
headers) is Thrift `TCompactProtocol`. Scylla has **no Thrift dependency today** — the
Cassandra Thrift API was removed; only vestigial `nodetool-completion` strings remain,
and neither `CMakeLists.txt` nor the packaging scripts reference it. parquet4seastar
required Thrift ≥ 0.11. Three options:

1. `find_package(Thrift)` + generated parser — adds a runtime dependency to every
   distro package Scylla ships, for a handful of structs.
2. Vendor the *generated* C++ from `parquet.thrift` — still needs the Thrift runtime
   library, so it does not actually remove the dependency.
3. **Hand-write a minimal `TCompactProtocol` codec for the Parquet metadata structs.**

**Recommendation: option 3.** TCompactProtocol is a small, frozen spec (zigzag varints,
field-id deltas, ~a dozen type codes) and Parquet's footer is a bounded, versioned set
of structs. This is on the order of a few hundred lines, adds no external dependency,
is trivially seastar-friendly, and — the deciding factor — lets us bound allocation and
recursion depth on hostile input, which a generated parser does not. Scylla already
hand-rolls its serialization layer (`idl/`) and already has build-time codegen
precedents (ANTLR3 for CQL, `utils/s3/gen_aws_service_errors.py`), so this is
idiomatic here. Validate it against footers produced by `parquet-mr` and Arrow as part
of the Phase 1 conformance suite.

---

## 8. Configuration surface

### 8.1 Per-table format selection

```cql
CREATE TABLE ks.t (...) WITH storage_format = 'sstable';   -- default
ALTER  TABLE ks.t       WITH storage_format = 'hybrid';    -- sstable | parquet | hybrid | auto
```

### 8.2 Per-table Parquet parameters

Mirrors the existing `compression = {...}` map property, parsed and validated into a
`parquet_parameters` object analogous to `compression_parameters`:

```cql
ALTER TABLE ks.t WITH parquet = {
    -- segmentation (the prompt's "rows per segment")
    'row_group_rows'            : '1000000',
    'row_group_bytes'           : '64MiB',
    'page_rows'                 : '20000',
    'page_bytes'                : '64KiB',

    -- compression
    'compression'               : 'zstd',        -- none|lz4|snappy|zstd|gzip|zstd_with_dicts
    'compression_level'         : '3',

    -- encoding
    'dictionary'                : 'auto',        -- auto|on|off
    'dictionary_page_max_bytes' : '1MiB',
    'encoding_overrides'        : '{"temp":"BYTE_STREAM_SPLIT","seq":"DELTA_BINARY_PACKED"}',

    -- indexing / statistics
    'write_page_index'          : 'true',
    'statistics_level'          : 'page',        -- none|chunk|page
    'bloom_filter_columns'      : '',

    -- Scylla-specific
    'metadata_folding'          : 'auto',        -- auto|verbatim|row|uniform
    'writer_version'            : '2.latest'
};
```

Validation rejects `row_group_bytes` above the shard budget, `page_bytes` above
`max_page_bytes`, and unknown encodings for a column's type.

### 8.3 Hybrid tiering policy (per table, with yaml defaults)

```cql
ALTER TABLE ks.t WITH parquet_tiering = {
    'min_output_bytes'      : '256MiB',   -- C2
    'min_data_age_seconds'  : '86400',    -- C3
    'max_garbage_fraction'  : '0.10',     -- C4
    'max_leaf_columns'      : '2000',     -- C5
    'min_gain_ratio'        : '0.15'      -- C6
};
```

### 8.4 Operational surface

```
nodetool tablestats --format-status ks.t     # current format + convergence %
nodetool upgradesstables -a ks t             # force convergence now
```
plus the equivalent REST endpoints under `/storage_service/`. There is no per-tablet
command, by design (§6.1) — the format is a table property and tablets are not part of
the user-facing model.

### 8.5 scylla.yaml

| Knob | Default | Purpose |
|---|---|---|
| `experimental_features: [parquet]` | off | Gate for v1 |
| `parquet_writer_memory_budget_fraction` | `0.05` | Shard-memory fraction for row-group buffering (R-13) |
| `parquet_max_page_bytes` | `4MiB` | Reader-side hostile-file guard (R-12) |
| `parquet_default_row_group_bytes` | `64MiB` | Cluster default |
| `parquet_default_page_bytes` | `64KiB` | Cluster default |
| `parquet_tiering_*` | as §8.3 | Cluster-wide policy defaults |
| `parquet_shadow_verify` | `false` | Dual-read verification during rollout (§7.3) |
| `parquet_estimator_sample_blocks` | `4096` | C6 sampling depth |

### 8.6 Estimation API

```
GET /storage_service/estimate_parquet_ratios?keyspace=ks&cf=t
```
Returns, per candidate configuration (row-group size × compression × folding level), the
predicted size relative to the table's current compressor — the same shape as
`estimate_compression_ratios`. This is both the C6 input and the operator's
decision-support tool.

---

## 9. Plan

Each phase gates the next. Phase 0 exists specifically so the expensive phases are
justified by data rather than by §3.4's hypotheses.

### Phase 0 — Measure before building *(no format work)*
- Offline tool: read real SSTables, re-encode into Parquet variants in memory, report
  sizes. Built on the existing `scylla sstable` CLI and the dict-trainer sampling path.
- **Ingest harness** per §9.4 — the validity of everything downstream depends on it.
- Run the public dataset roster (§9.3) and, decisively, **real customer schemas** via the
  ratios-only field tool (§9.5).
- Fix the §5.3 folding design against measured column-count blowups, using D2 (Backblaze)
  as the adversarial case.
- **Exit criteria:** (a) ≥ 25 % saving vs. `ZstdWithDictsCompressor` on ≥ 2 archetypes
  under the *realistic* timestamp regime and in *token order*; (b) the sparse-wide
  regression is either eliminated by folding or reliably predicted by the estimator so
  policy can decline it; (c) ≥ 10 customer tables measured.
- **This phase can start immediately and answers the go/no-go question cheaply.**

### Phase 1 — Library  <span>· **partially delivered 2026-08-16**</span>
**Done:** `sstables/parquet/format/` — `TCompactProtocol` reader *and* writer (no
libthrift), `FileMetaData` + `PageHeader` parsers, RLE/bit-packed hybrid codec,
PLAIN / RLE_DICTIONARY / DELTA_BINARY_PACKED / BYTE_STREAM_SPLIT encoders, V2 data-page
assembly, zstd via the existing dependency, and a `file_writer` producing complete
files. Verified: footer conformance vs. pyarrow (10 files / 685 chunks), writer→pyarrow
interop (7 fixtures), real V2 level decode vs. writer statistics (684 pages), fuzz
(12 714 mutations, 0 crashes).
**Not done:** page index emission, column/offset index, bloom filters, DELTA_BYTE_ARRAY,
per-page dictionary index splitting (a dictionary-encoded chunk is currently one page),
and the seastar-native I/O layer — the writer builds a whole file image in memory.

- `sstables/parquet/` with the two-layer split of §7.8; hand-rolled `TCompactProtocol`
  metadata codec (no libthrift dependency).
- Writer + reader, V2 pages, `PLAIN`/`RLE_DICTIONARY`/
  `DELTA_BINARY_PACKED`/`DELTA_LENGTH_BYTE_ARRAY`/`DELTA_BYTE_ARRAY`/
  `BYTE_STREAM_SPLIT`, page index, page + chunk statistics, page CRC.
- Wired to `sstable_compressor_factory`; page-size caps; memory accounting.
- Conformance: parquet4seastar's suite + cross-reads against Arrow/DuckDB.

### Phase 2 — Single-node format integration *(experimental flag)*  <span>· **started 2026-08-16**</span>
**Done:** `sstables/parquet/schema_mapping.{hh,cc}` — the CQL-schema-to-Parquet-schema
mapper and the row shredder/reassembler, implementing folding levels L0/L1/L2 with an
automatic L2→L1 fallback when the uniform precondition breaks. Losslessness proven over
540 generated cases (§10.3a), including divergent per-cell timestamps, TTLs and
deletions.
**Also done 2026-08-16:** `sstables/parquet/writer_impl.{hh,cc}` — `columns_of(schema)`,
`fragment_shredder` (decorated_key / clustering_row / atomic_cell → rows) and
`pq_writer_impl`, a real `sstable_writer::writer_impl`. Compiles in-tree; covered by
`test/boost/parquet_writer_test` (3 cases, green) which drives 500 rows of real fragments
through to a Parquet image and parses it back with our own footer reader.

Contract discovered here and now asserted: regular columns arrive in **name order**,
which is also the `column_id` order the shredder indexes cells by — not declaration
order. The reader must use the same ordering to invert the mapping.

**Not done:** the produced image goes to a caller-supplied sink, not into the Data
component. Outstanding: component/TOC plumbing, the `pq` version enum, `pq::make_reader`,
index components pointing at `(row_group, row_index)`, the mutation-source property
suite, and — in the shredder — static rows, partition/range tombstones and collections.

- `sstable_version_types::pq`; `writer_impl` subclass; `pq::make_reader`.
- Component layout (§5.2); index components pointing at `(row_group, row_index)`.
- Metadata folding levels 0–2.
- Table-level `storage_format`, single format per table; no hybrid tiering yet.
- Round-trip property tests green; shadow-read verification available.

### Phase 3 — Format control and convergence
- `storage_format` schema property, validated and plumbed through the schema tables
  (no new system table, no group0 work — see §6.1).
- `rewrite_to_format` compaction; per-table convergence tracking; nodetool/REST.
- Cluster feature `PARQUET_SSTABLE_FORMAT`.
- Streaming, migration, split and merge must keep working across a mixed-format
  table; cluster-feature gating and tests.

### Phase 4 — Hybrid tiering  <span>· **mostly delivered 2026-08-16**</span>
**Done:** the policy engine. `evaluate_tiering()` in
`sstables/parquet/tiering_policy.{hh,cc}` implements C1–C7 as a pure function
over plain numbers — no compaction manager, no schema, no I/O — so every criterion
is testable on its own, and each rejection carries a reason string. C6 treats
"not measured" as a rejection rather than an optimistic guess.
`tiering_context.{hh,cc}` is the only file that knows about sstables and schemas:
it fills the inputs from a candidate compaction (size from the input sstables,
age from their `max_timestamp`, eligibility from the schema) and checks the
table's `storage_format` before anything else. 19 standalone cases plus in-tree
schema-eligibility tests.

**Estimator:** `scylla sstable parquet-export --max-rows` serves as the C6
estimator today and its sampling accuracy is validated (§10.3e). The in-node
`/storage_service/estimate_parquet_ratios` REST endpoint is **not** built — it
needs to read rows from a live table's sstables, which is a bigger piece than the
CLI path.

**Not done:** wiring `decide_output_format()` into the compaction strategies so
the verdict actually selects the writer. That is blocked on the Data-component
integration — there is no point selecting a format the engine cannot yet emit.
ICS and TWCS are supported by the policy's inputs but have no strategy-side hook.

### Phase 5 — Ecosystem and hardening
- Backup/restore/scrub/tooling completion.
- Object-storage integration; `EXPORT` at folding Level 3.
- Encryption-at-rest interaction; downgrade procedure documented.
- Performance work against R-9 … R-13.

### 9.1 Benchmark matrix (to be executed and recorded in §10)

**Schema archetypes:** time-series/IoT · event log · user profile (KV) · mixed OLTP
orders · wide sparse (100 cols, 20 % filled) · large blob · collection-heavy.

**Baselines:** `LZ4Compressor` (default) · `ZstdCompressor` · `LZ4WithDictsCompressor` ·
`ZstdWithDictsCompressor`, each at chunk lengths 4 KiB / 16 KiB / 64 KiB.

**Parquet variants:** row group ∈ {16, 64, 256} MiB × compression ∈ {zstd-3, zstd-9,
lz4, zstd_with_dicts} × folding ∈ {verbatim, row, uniform}.

**Metrics per cell:** on-disk bytes · write CPU · full-scan throughput · projected-scan
throughput · point-read p50/p99 (cold) · peak writer memory · reactor-stall count.

### 9.2 Validation methodology — three ways to get a wrong answer

Synthetic random data is unusable: it defeats every encoding and every codec on *both*
sides, so the ratio converges to ~1.0 and measures nothing. But there are three subtler
traps, and two of them systematically **overstate** the Parquet win. All three must be
controlled or the Phase 0 go/no-go number is worthless.

**Trap 1 — Token-order scrambling (understates, and is the most Scylla-specific).**
A public dataset compresses well as a Parquet file partly because its natural file order
has locality: adjacent rows share values, so RLE, dictionary and `DELTA_*` encodings pay
off. Loading that dataset into ScyllaDB **destroys that order** — rows are sorted by
`token(partition_key)`, which is a hash. A row group is then a token-range slice, and the
locality the published 8× ratios depend on is gone. Only locality *within* a partition
(the clustering order) survives.

> Consequence: **never measure by re-encoding a CSV/Parquet file directly.** The harness
> must load into a real ScyllaDB cluster and re-encode the *actual flushed SSTables*.
> Any number produced without a round-trip through token order is not a prediction of
> anything. Report the delta between "natural order" and "token order" ratios explicitly
> — it is a real and quotable finding about columnar storage in a hash-partitioned
> database.

**Trap 2 — Bulk-load timestamp collapse (overstates, and is large).**
§3.1 claims the per-cell timestamp column is likely our single biggest structural win.
A one-shot bulk load gives every cell a near-identical `writetime`, so that column
delta-packs to almost nothing — a result that will not reproduce in production. The
ingest harness must therefore spread write times realistically, and include overwrites,
late-arriving rows and TTLs. Measure with **both** a collapsed-timestamp load and a
realistic-timestamp load, and report both; the gap quantifies how much of the win is an
artefact.

**Trap 3 — Archetype cherry-picking (overstates).**
Pick the dataset roster and the pass/fail thresholds **before** measuring, publish every
cell including the regressions, and report distributions rather than means. §3.4 already
predicts a regression on wide-sparse schemas; a validation run that does not reproduce
one is more likely to be missing the case than to have disproved it.

### 9.3 Public dataset roster

Chosen to cover the §3.4 archetypes with *real* data at ≥ 10 GB scale, and to include
the cases expected to lose. Licences must be reviewed before any number is published
externally.

| # | Dataset | Scale | Archetype it covers | Why this one |
|---|---|---|---|---|
| D1 | **ClickBench `hits`** ([ClickHouse/ClickBench](https://github.com/ClickHouse/ClickBench)) | 100 M rows, **70 GB** raw (~15 GB gz); 105 cols — 19 INT, 6 BIGINT, 48 SMALLINT, 26 TEXT, 1 VARCHAR, 1 TIMESTAMP, 1 DATE | Wide, real, mixed-type event log | The canonical wide real-world table. Real anonymised web analytics with Unicode strings; heavy low-cardinality integer columns (48 SMALLINT) that should dictionary/RLE extremely well. Available as CSV/TSV/JSONL/Parquet |
| D2 | **Backblaze Drive Stats** ([backblaze.com](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data)) | 388 M+ records, +240 k/day; up to 124 columns (raw + normalised for 60–70 SMART attrs) | **Sparse / NULL-heavy — the predicted regression case** | The real-world Scenario 3, and the most important dataset in the roster. Genuinely sparse: *most drive models report only a few SMART attributes*, so NULL fractions land in exactly the ~0.5 danger zone. Real production telemetry, one row per drive per day, plus real schema drift across quarters |
| D3 | **Public BI Benchmark** ([cwida/public_bi_benchmark](https://github.com/cwida/public_bi_benchmark)) | **386 GB** uncompressed / 41 GB compressed; 46 workbooks, 206 tables | Messy real-world heterogeneity, breadth | *The* corpus the columnar-compression literature uses — BtrBlocks, FastLanes and the Vortex work all evaluate on it, so our numbers become comparable to published ones (e.g. BtrBlocks reports Parquet+zstd at 7.06–8.24× on this corpus). Real, dirty, user-generated Tableau data. Subset to ~10–20 tables |
| D4 | **TSBS** devops + iot ([timescale/tsbs](https://github.com/timescale/tsbs)) | Generate to any scale | Time-series / IoT metrics | Synthetic but *purpose-built* for realistic metric cardinality and tag structure: 9 subsystems, 100 metrics per reading, per-host tag sets. The `iot` mode deliberately emits out-of-order, missing and empty entries. Deterministic under a PRNG seed, so runs are reproducible — and it has a Cassandra loader, so it works against Scylla directly |
| D5 | **NYC TLC trip records** | ~1.5 B rows across years, tens of GB; natively Parquet since 2022 | Real time-series, numeric + timestamp heavy | Well-known, genuinely real, and its natural clustering makes it the sharpest illustration of Trap 1 |
| D6 | **OpenAddresses** ([openaddresses.io](https://openaddresses.io/)) | Global; multi-GB | **Names / addresses / people** — the "names" case | Real human-generated strings with real skew: repeated street and city names (dictionary-friendly) mixed with high-cardinality house numbers and unique IDs |
| D7 | **Stack Exchange data dump** | ~35 GB of text | High-cardinality text + user metadata | Real user names, post bodies, tags. Stresses the case where dictionary encoding *fails* and we should land at parity, not regression |
| D8 | Synthetic large-blob table | 10 GB+ | Opaque blob | Confirms the predicted ~parity and that the policy engine declines it |

Caveats worth recording:
- ClickBench's published Parquet files "have no proper logical data types and no bloom
  filter indexes" — use them as *input*, never as the comparison target. Regenerate.
- Public BI at 386 GB needs subsetting; pick tables spanning the width and cardinality
  range rather than the largest ones.
- D2's schema drift across quarters is a feature, not a nuisance: it exercises the
  folding logic's per-file column-materialisation decision (§5.3).

### 9.4 Ingest harness requirements

The harness is a Phase 0 deliverable in its own right, and most of the validity of the
whole exercise lives in it.

1. **Load into a real cluster**, flush, and major-compact to a stable SSTable set;
   measure the SSTables, not the source files (Trap 1).
2. **Two timestamp regimes per dataset** — collapsed (bulk load) and realistic
   (writes spread over simulated time, with a configurable overwrite rate, late-arrival
   rate and TTL fraction). Report both (Trap 2).
3. **Two partition-key choices per dataset** where the schema allows — one giving large
   multi-row partitions (clustering locality preserved) and one giving
   single-row partitions (worst case for locality). This brackets the realistic range.
4. Re-encode the resulting SSTables through every Parquet variant in §9.1 *offline*, so
   the matrix can be swept without re-ingesting.
5. Record the achieved-vs-predicted ratio for every cell, which doubles as validation of
   the C6 estimator (§6.3).

### 9.5 The decisive evidence: customer data

Public datasets validate the *mechanism*; they do not validate the *business case*.
ScyllaDB's real workload mix — time-series, IoT, ad-tech, user profiles, messaging — is
not what a Tableau corpus or a web-analytics table looks like.

The Phase 0 estimator already samples SSTables in place (§6.3, reusing the dictionary
autotrainer's sampling path). Ship it as a **field tool that emits only ratios and
schema-shape statistics — never data**, so it can be run inside a customer's environment
and the output shared freely. A dozen real production schemas measured this way is worth
more than the entire public roster above, and it is cheap: no ingest, no cluster, no
data movement.

Target: ≥ 10 customer tables across ≥ 4 industries before the Phase 1 go/no-go.

Note that `cassandra-stress` and `scylla-bench` profiles are the right tools for R-9…R-11
(throughput and latency) but are **not** valid for compression measurement — their
generated values are effectively random.

---

## 10. Configuration results

**Phase 0 executed 2026-08-16.** Measured on a real ScyllaDB node
(`2026.3.0~dev-0.20260723.ab2bb064ce5c`, `build/dev/scylla`), single shard, one tablet
per table so a major compaction yields exactly one SSTable. Every SSTable figure is
bytes on disk. Parquet figures are pyarrow-written files over rows read back from Scylla
**in true token order** (zstd-3, 64 KiB pages, V2 pages, page index + statistics on).

Harness: `~/pq-lab/harness.py`; roll-up `~/pq-lab/analyze.py`; raw JSON in
`~/pq-lab/out/`. The superseded first run (default tablet count, 64 SSTables per table)
is retained under `~/pq-lab/out/runA_multitablet/` — see the dictionary finding below.

**Headline: Parquet is 52.5–59.3 % of a dictionary-compressed SSTable on all three
datasets under the realistic timestamp regime — 41–47 % disk saved. The ≥ 25 % exit
criterion is met on all three.**

### 10.1 Disk usage by dataset

One row per (dataset × timestamp regime), per §9.4. `TS regime` ∈ {collapsed, realistic}
— the gap between the two is itself a headline result (Trap 2).

All byte counts are SSTable `Data.db`; Parquet column is folding level L1 / zstd-3.

| Dataset | TS regime | Rows | Cols | LZ4 | Zstd | LZ4+dicts | Zstd+dicts | Parquet L1 | vs. LZ4 | vs. Zstd+dicts |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| D2 Backblaze | collapsed | 300 000 | 197 | 57 304 070 | 38 826 603 | 38 037 326 | 19 174 171 | 14 790 180 | 25.8 % | 77.1 % |
| **D2 Backblaze** | **realistic** | 300 000 | 197 | 67 545 539 | 49 063 873 | 52 511 917 | 32 458 096 | 17 055 064 | 25.2 % | **52.5 %** |
| D1 ClickBench | collapsed | 200 000 | 105 | 65 546 950 | 51 622 378 | 41 803 499 | 26 723 258 | 16 009 570 | 24.4 % | 59.9 % |
| **D1 ClickBench** | **realistic** | 200 000 | 105 | 65 737 454 | 52 103 621 | 41 875 902 | 27 295 104 | 16 177 362 | 24.6 % | **59.3 %** |
| D5 NYC TLC | collapsed | 500 000 | 20 | 32 008 211 | 19 562 212 | 24 078 531 | 15 530 296 | 8 633 990 | 27.0 % | 55.6 % |
| **D5 NYC TLC** | **realistic** | 500 000 | 20 | 32 449 753 | 19 787 920 | 24 340 444 | 15 679 868 | 8 634 001 | 26.6 % | **55.1 %** |
| D3 Public BI | — | not run | | | | | | | | |
| D4 TSBS | — | not run | | | | | | | | |
| D6 OpenAddresses | — | not run | | | | | | | | |
| D7 Stack Exchange | — | not run | | | | | | | | |
| D8 Large blob | — | not run | | | | | | | | |

Notes: `LZ4WithDictsCompressor` is erratic — on D2/realistic it is *worse* than plain
Zstd. Zstd+dicts beats plain Zstd by 38–50 %, which is precisely why §3.2 insists it is
the only defensible baseline: quoting against LZ4 would roughly double the apparent win.

### 10.1a Token-order penalty (Trap 1)

Ratio achieved when the dataset is encoded in its natural file order versus in ScyllaDB
token order. Quantifies how much published columnar-compression research over-predicts
for a hash-partitioned database.

Parquet L1/zstd-3, identical rows and settings; only the row order differs.

| Dataset (realistic) | Natural order | Token order | Penalty | Why |
|---|---:|---:|---:|---|
| **D2 Backblaze** | 11 863 976 | 17 055 064 | **+43.8 %** | pk `serial_number` is high-cardinality; hashing shatters the one-row-per-drive-per-day locality |
| D1 ClickBench | 16 183 075 | 16 177 362 | −0.0 % | pk `UserID`; natural order had little locality to lose |
| D5 NYC TLC | 8 971 907 | 8 634 001 | −3.8 % | pk `VendorID` has 3 distinct values, so token order *groups* rows and helps |

**Trap 1 confirmed, and it is selective.** The penalty is a function of partition-key
cardinality and how much locality the natural order carried. It is large where it bites,
absent elsewhere — so it cannot be estimated from the schema and must be measured per
table, which is what criterion C6 does.

### 10.1b Trap 2 — timestamp regime

Parquet folding level L0 (the 2020 verbatim mapping) under the two regimes:

| Dataset | L0 collapsed | L0 realistic | Change | SSTable Zstd+dicts change |
|---|---:|---:|---:|---:|
| **D2 Backblaze** | 15 789 102 | 457 454 962 | **+2 797 %** | +69.3 % |
| D1 ClickBench | 16 388 873 | 33 503 657 | +104.4 % | +2.1 % |
| D5 NYC TLC | 8 763 261 | 8 763 448 | +0.0 % | +1.0 % |

**Trap 2 confirmed, and it is worse than predicted.** A bulk load does not merely
overstate the win — it makes the verbatim mapping look *fine* (18 % smaller than
SSTables on D2). Measuring only that regime would have shipped the wrong schema mapping.
The SSTable baseline moves too (+69 % on D2), so a bulk-load benchmark is fiction on
both sides.

### 10.1c Column-projection I/O

Computed exactly from real Parquet footers by the Phase 1 parser
(`~/pq-lab/projection.py`). Bytes a projected query must read, vs. an SSTable that must
read whole rows.

| Query | Parquet bytes | % of SSTable (Zstd+dicts) | Reduction |
|---|---:|---:|---:|
| D2 `SELECT failure` | 685 | 0.002 % | ~47 000× |
| D2 3 columns | 318 197 | 1.0 % | ~102× |
| D1 `SELECT UserID` | 186 529 | 0.7 % | ~146× |
| D1 3 key columns | 699 000 | 2.6 % | ~39× |
| D5 `SELECT total_amount` | 824 891 | 5.3 % | ~19× |
| D5 4 columns | 2 892 781 | 18.4 % | ~5× |

### 10.1d Estimator accuracy (validates C6)

*Not yet measured — the estimator is not built. Deferred with the rest of C6.*

### 10.2 Sensitivity to row group size

Swept by row count (L1/zstd-3, token order, realistic). Byte-sized sweep at bottom-tier
scale is still to do.

| Dataset | 125 k rows | 500 k rows | 2 M rows | 125 k vs 500 k |
|---|---:|---:|---:|---:|
| D5 NYC TLC (20 cols) | 10 135 982 | 8 634 001 | 8 634 001 | +17.4 % |
| D2 Backblaze (197 cols) | 17 688 614 | 17 055 064 | 17 055 064 | +3.7 % |
| D1 ClickBench (105 cols) | 15 761 792 | 16 177 362 | 16 177 362 | **−2.6 %** |

No universal answer: narrow tables want large row groups (dictionary and delta state
amortise), while ClickBench is *better* with smaller ones. Confirms `row_group_rows` must
stay user-tunable (§8.2) — a single cluster-wide value costs 3–17 % depending on schema.

### 10.3 Sensitivity to metadata folding level

Realistic timestamp regime, token order, zstd-3.

| Dataset | Cols | L0 verbatim | L1 row-folded | L2 uniform | L0/L1 |
|---|---:|---:|---:|---:|---:|
| **D2 Backblaze** | 197 | 457 454 962 | 17 055 064 | 14 788 766 | **26.8×** |
| D1 ClickBench | 105 | 33 503 657 | 16 177 362 | 16 008 547 | 2.1× |
| D5 NYC TLC | 20 | 8 763 448 | 8 634 001 | 8 631 807 | 1.0× |

**The single most important result of Phase 0.** The blow-up scales with column count:
L0 stores the row's write time once *per column*, so a 195-data-column table carries 195
near-identical high-entropy `int64` columns that zstd compresses well individually but
cannot dedupe across column chunks. This is the true mechanism behind the 2020 study's
151 % wide-sparse regression — it is an artefact of the mapping, not a property of
Parquet or of sparse schemas.

**Correction to §3.4:** the "wide sparse ⇒ regression risk" row is wrong as written.
With folding, the widest, sparsest table posted the *best* result (52.5 %), not the
worst — it has the most redundant per-cell metadata to fold away and the most columns to
prune on read.

### 10.3a Metadata folding — losslessness and the divergence cost curve

**Measured 2026-08-16, Phase 2.** `sstables/parquet/test_shred.cc`.

Threat-to-validity #1 in the previous run was that L1 had only ever been measured on
data where every cell in a row shares one write time (`INSERT`-written rows), leaving
`UPDATE`-assembled rows unmeasured. That gap is now closed.

**Losslessness.** 540 cases — folding levels × divergence rate {0, 5, 25, 50, 100 %} ×
null rate {0, 25, 60 %} × TTL rate {0, 30 %} × deletion rate {0, 20 %} × width {1, 5, 40
regular columns}. `shred()` → `reassemble()` returns the input exactly in every case,
including per-cell timestamps carried through the exception column. **0 failures.**

**Cost of divergence.** 20 000 rows × 40 regular columns, 25 % nulls:

| Divergence | L1 bytes | vs. L1 @ 0 % | L1 leaves | L0 bytes | L0 leaves |
|---:|---:|---:|---:|---:|---:|
| 0 % | 1 524 095 | 1.00× | 43 | 4 743 632 | 202 |
| 1 % | 1 574 872 | 1.03× | 83 | 4 743 549 | 202 |
| 5 % | 1 725 241 | 1.13× | 83 | 4 744 623 | 202 |
| 10 % | 1 885 296 | 1.24× | 83 | 4 741 170 | 202 |
| 25 % | 2 247 308 | 1.47× | 83 | 4 740 320 | 202 |
| 50 % | 2 838 144 | 1.86× | 83 | 4 741 590 | 202 |
| 100 % | 4 087 661 | **2.68×** | 83 | 4 738 682 | 202 |

Two conclusions:

1. **L1 degrades gracefully and never loses to L0.** Even at 100 % divergence — every
   row assembled from per-column updates, the pathological case — L1 is 2.68× its best
   but still 14 % *smaller* than the verbatim mapping. There is no workload on which
   keeping the 2020 mapping is the right call.
2. **At realistic divergence (≤ 10 %) folding costs ≤ 24 %**, which is comfortably inside
   the margin measured in §10.1.

**RESOLVED 2026-08-16 (see §10.3c).** The per-column design below was replaced by a
sparse side-channel and re-measured. Original finding retained for the record:

**Design finding — the exception leaves are too coarse.** Leaf count jumps 43 → 83 as
soon as divergence is non-zero, and stays there: with 20 000 rows, "at least one row
diverges in this column" is near-certain even at 1 %, so an exception leaf materialises
for *every* column while carrying almost no data. The per-column exception design in
§5.3 should be replaced by a **single sparse exception structure** — one `__tsx` value
column plus a column-id column, or a (row, column, timestamp) triple set — so the cost
scales with the number of divergent *cells* rather than the number of columns. Filed as
open question 8.

### 10.3c Sparse timestamp exceptions (resolves open question 8)

The per-column exception leaves of §10.3a were replaced by a side-channel that is two
leaves wide regardless of table width:

- `__tsx_mask` — optional `BYTE_ARRAY`, a bitmap over the regular columns marking which
  cells in this row diverge from the row's `__ts`.
- `__tsx_vals` — optional `BYTE_ARRAY`, the corresponding timestamp deltas as
  zigzag varints, in column order.

Both are null for rows with no exception, which costs one definition-level bit each and
compresses away. Same 20 000 rows × 40 regular columns, 25 % nulls:

| Divergence | per-column bytes | leaves | **sparse bytes** | **leaves** | Saving |
|---:|---:|---:|---:|---:|---:|
| 0 % | 1 524 095 | 43 | 1 524 095 | 43 | — |
| 1 % | 1 574 872 | 83 | **1 541 212** | **45** | 2.1 % |
| 5 % | 1 725 241 | 83 | **1 610 139** | **45** | 6.7 % |
| 10 % | 1 885 296 | 83 | **1 695 210** | **45** | 10.1 % |
| 25 % | 2 247 308 | 83 | **1 930 428** | **45** | 14.1 % |
| 50 % | 2 838 144 | 83 | **2 329 374** | **45** | 17.9 % |
| 100 % | 4 087 661 | 83 | **3 128 426** | **45** | **23.5 %** |

Leaf count is now 43 + 2 instead of 43 + 40, and it stays at +2 for a table of any
width. The worst-case divergence penalty drops from 2.68× to **2.05×**.

Losslessness re-proven over 1 080 cases (the §10.3a matrix × both exception encodings),
0 failures. `exception_encoding::sparse` is the default; `per_column` is retained so the
comparison stays reproducible.

### 10.3d End-to-end through our own writer (retires threat #2)

**Measured 2026-08-16.** Real ScyllaDB table → real SSTable → `scylla sstable
parquet-export` → Parquet, with the shredder reading the actual mutation-fragment
stream. Previous size numbers came from pyarrow; these come from our writer.

200 000 NYC-taxi rows, 11 columns, `ZstdWithDictsCompressor`, one tablet, realistic
timestamps:

| | Bytes |
|---|---:|
| SSTable `Data.db` (Zstd + dicts) | 5 317 307 |
| **Parquet, our writer, L1 folding, zstd-3** | **2 562 753** |
| **Ratio** | **48.2 % — 51.8 % saved** |

The per-column breakdown is the interesting part:

| Column | Uncompressed | Compressed | Note |
|---|---:|---:|---|
| `vendor` (partition key, 3 distinct) | 800 280 | **525** | delta-packed to nothing |
| `__ts` (the folded timestamp) | 8 428 | **603** | §3.1's biggest claimed win, confirmed: ~0.003 bytes/row |
| `flag` (low-cardinality text) | 2 974 | 1 528 | RLE_DICTIONARY |
| `pay` | 1 600 330 | 41 574 | delta-packed |
| `rid` (unique sequential) | 341 339 | 341 439 | already minimal after delta; zstd adds 100 bytes of frame |
| `dist`/`fare`/`tip`/`total` (doubles) | 1 600 340 ea. | 332 k–453 k ea. | **not yet using BYTE_STREAM_SPLIT** — an easy remaining win |

The output file is read back correctly by pyarrow (200 000 rows × 12 columns, values
verified), so the interop property holds for files produced from real Scylla data and
not just from synthetic fixtures.

Two follow-ups this exposes: enable `BYTE_STREAM_SPLIT` for `double` columns by default
(the encoder exists but is not selected), and note that `__ts` at 603 bytes for 200 000
rows means the folding overhead is effectively zero on `INSERT`-shaped data.

### 10.3b Writer output — encoding effectiveness

Same 50 000-row fixture, our writer, zstd-3 (`sstables/parquet/format/test_writer.cc`):

| Fixture | Bytes | Note |
|---|---:|---|
| `w_plain_nonull` | 483 049 | PLAIN throughout, no nulls |
| `w_plain_nulls` | 425 711 | 25 % nulls — levels + fewer values |
| `w_dict_nulls` | 390 939 | RLE_DICTIONARY on the string column |
| `w_delta_ts` | 352 954 | **+ DELTA_BINARY_PACKED on `__ts` → 9.7 % smaller** |

DELTA_BINARY_PACKED on the folded timestamp column is worth ~10 % of the whole file on
its own, which is the mechanism §3.1 predicted and the reason the folded `__ts` column is
nearly free.

### 10.3e Sampling accuracy — the C6 estimator works

Criterion C6 rests on being able to predict Parquet's size from a *sample*, cheaply,
before rewriting anything. Measured on the 200 000-row table of §10.3d:

| Run | Rows encoded | Bytes/row | Predicted ratio vs. SSTable |
|---|---:|---:|---:|
| Full re-encode | 200 000 | 12.81 | 0.482 |
| **20 000-row sample** | 20 000 | 12.87 | **0.484** |

**0.4 % error from 10 % of the data.** Compression ratios converge long before row
counts do, which is what makes a cheap estimator viable and therefore what makes C6
enforceable rather than aspirational. Exposed as `--max-rows` on
`scylla sstable parquet-export`.

### 10.4 Throughput and latency vs. targets

| Path | Native | Parquet | Δ | Target (§1.2) | Pass? |
|---|---|---|---|---|---|
| Write (flush) | | | | ≥ −10 % | |
| Write (compaction) | | | | bounded | |
| Full scan | | | | ≥ parity | |
| Projected scan | | | | 1.5–5× | |
| Point read p99 (cold) | | | | ≤ 1.2× | |
| Point read (cache hit) | | | | no change | |

### 10.5 Decision log

| Date | Decision | Rationale |
|---|---|---|
| 2026-08-15 | Model Parquet as a new `sstable_version_types::pq`, not a `storage_engine` | Keeps LSM, index components, TOC, backup/restore, streaming for free; only the Data encoding changes |
| 2026-08-15 | Keep Scylla's own index/filter components alongside `Data.parquet` | Preserves O(1) partition lookup and R-11; the `.parquet` stays independently valid |
| 2026-08-15 | Baseline for all size claims is `ZstdWithDictsCompressor`, not LZ4 | The 2020 numbers compared against Deflate; shared dictionaries have since absorbed much of Parquet's advantage |
| 2026-08-15 | Format switch is a write-side policy + background convergence, not a tablet transition | Conversion is node-local; the transition stages model replica-set changes and would add risk for no benefit |
| 2026-08-15 | New in-tree library, borrowing parquet4seastar's design | Arrow was tried and abandoned in 2020; parquet4seastar is 6 years stale; we need a subset and in-tree control of memory/stalls |
| 2026-08-15 | Hybrid eligibility gated on a *measured* predicted gain (C6), not schema heuristics | Turns an unanswerable design question into a per-table measurement before any rewrite |
| 2026-08-16 | Code lives at `sstables/parquet/` in the main repo, inside the existing `sstables` static-lib target | SSTables are wholly in scylladb, not Seastar; `sstables/{kl,mx,trie}/` is the established precedent. A submodule would force cross-repo iteration and interface inversion for a codec with no independent life (§7.8) |
| 2026-08-16 | Two-layer split: `sstables/parquet/format/` (spec codec, no Scylla types) under the mapping layer | Makes the codec independently fuzzable — required, since restore/`upload` can deliver untrusted files — and keeps later extraction possible without deciding now |
| 2026-08-16 | Hand-write a minimal `TCompactProtocol` codec rather than depend on libthrift | Scylla has no Thrift dependency today; the protocol is small and frozen, and a hand-rolled parser can bound allocation/recursion on hostile input (§7.8) |
| 2026-08-16 | All compression measurements must round-trip through a real cluster and be taken on flushed SSTables, never by re-encoding source files | Token-order partitioning destroys the natural-order locality that published columnar ratios depend on (§9.2, Trap 1). Re-encoding a CSV predicts nothing about ScyllaDB |
| 2026-08-16 | Every dataset measured under both collapsed and realistic cell-timestamp regimes | A bulk load collapses the per-cell timestamp column to near-zero, inflating the win that §3.1 leans on hardest (§9.2, Trap 2) |
| 2026-08-16 | Backblaze Drive Stats (D2) is the designated adversarial dataset | Real production telemetry whose NULL fractions sit in the ~0.5 danger zone — the real-world form of the 2020 Scenario-3 regression, plus real cross-quarter schema drift |
| 2026-08-16 | Ship the C6 estimator as a ratios-only field tool for customer environments | Public corpora validate the mechanism; only customer schemas validate the business case, and a ratios-only tool needs no data movement (§9.5) |
| 2026-08-16 | **Phase 0 GO.** Exit criterion met on all three datasets (41–47 % saved vs. Zstd+dicts, realistic regime, token order) | §10.1. Result is better than §3.4 predicted, and the predicted wide-sparse regression did not materialise once folding was applied |
| 2026-08-16 | Metadata folding is promoted from optimisation to hard prerequisite | 26.8× on a 197-column table (§10.3). Without it the project fails on exactly the widest tables, which hold the most data |
| 2026-08-16 | Both timestamp regimes stay mandatory in every future measurement | Trap 2 turned out to invert a design conclusion, not merely shade a number (§10.1b) |
| 2026-08-16 | Per-SSTable zstd dictionary duplication filed as a separate issue for the compression team | ~110 KB dictionary copied into every SSTable's `CompressionInfo.db`; with tablets producing many small SSTables it reached 45 % of table size. Independent of Parquet |
| 2026-08-16 | Threat-to-validity #1 (folding assumes uniform row timestamps) **retired** | Losslessness proven over 540 cases including 100 % per-cell divergence; cost curve measured at 1.03×–2.68× (§10.3a). L1 never loses to L0 |
| 2026-08-16 | Per-column timestamp-exception leaves are the wrong shape; redesign to a sparse structure | At row-group scale they materialise for every column even at 1 % divergence while carrying nearly no data (§10.3a). Open question 8 |
| 2026-08-16 | CQL `text` must carry the Parquet `UTF8` ConvertedType | Without it every downstream reader sees a blob, not a string — found by the writer↔pyarrow interop test, and it would have silently defeated the §7.4 interoperability case |
| 2026-08-16 | Timestamp exceptions encoded as a two-leaf sparse side-channel, not per-column leaves | Leaf count becomes width-independent; worst-case divergence penalty 2.68× → 2.05×, and 23.5 % smaller at full divergence (§10.3c). Resolves open question 8 |
| 2026-08-16 | Build unblocked by disabling `Seastar_LTTNG` | `lttng-ust-devel` is absent and there is no sudo on this host. The option only gates IO tracing tracepoints, so a dev build is unaffected — but a production build should install the package instead |
| 2026-08-16 | `sstable_version_types::pq` is defined but deliberately left out of `all_sstable_versions` and `writable_sstable_versions` | Those arrays mean "versions the node can read / write". Adding `pq` before the writer exists made `sstable_test` look for a `pq` fixture and fail — a useful signal that the arrays are a support claim, not a list of enum values. It goes in with the Data-component writer and `pq::make_reader` |
| 2026-08-16 | The tiering policy is a pure function; a separate `tiering_context` fills its inputs | Keeps every criterion unit-testable without a compaction manager or a schema, and forces each criterion to be expressible as a number the caller supplies — a criterion that cannot be does not belong in the policy |
| 2026-08-16 | `decide_output_format` checks the table's `storage_format` before any criterion | Opting in is permission to convert, not an instruction: a table set to `parquet` still has to clear the size and gain gates, so a 4 KiB flush is never converted |
| 2026-08-16 | The Parquet re-encoder ships as a `scylla sstable parquet-export` operation rather than a hook in the write path | It needs no access to sstable internals, produces the §10.3d measurements from our own writer, and is the natural home for the C6 estimator and the eventual export path. A temporary dual-write hook would have needed friend declarations for something that gets deleted later |
| 2026-08-16 | **Format is a per-table schema property, not per-tablet.** Reverses the earlier per-tablet design | A tablet is an internal distribution unit, not something users reason about, and an SSTable's encoding has nothing to do with which tablet its data is in. Doing it at table level removes the `tablet_info` field, the `system.tablets` column and all the group0 plumbing — schema properties already replicate, so the mechanism exists. Mirrors how `compression` already works (§6.1) |
| 2026-08-16 | Fixed-width scalars are decoded straight from cell bytes (big-endian) rather than via `deserialize()` | Avoids a `data_value` round-trip per cell on the write path; unmapped types keep their serialised form as opaque `BYTE_ARRAY`, which is lossless but forgoes type-specific encoding |
| 2026-08-16 | The Parquet image is handed to a sink, not written to the Data component yet | Keeps the whole fragment→Parquet path drivable from a unit test without constructing an sstable; component plumbing is a separate, smaller step |
| 2026-08-16 | The writer emits V2 data pages only | Keeps definition levels outside the compressed body, so a reader can skip nulls and locate rows without invoking a codec — the property the level-decode test exercises |

---

## 11. Open questions

1. **Row-group boundaries vs. partition boundaries.** Should a partition be forbidden
   from spanning row groups? It would simplify point lookup and split, but a single
   large partition would then force an oversized row group. Leaning: allow spanning,
   record the partition's row-group span in the index.
2. **Promoted index equivalent.** Does the Parquet page index fully replace the
   promoted index for intra-partition seeks, or do we still need our own?
3. **`sstable_run` semantics.** Does an ICS run mix formats mid-run during convergence,
   or is the run the atomic conversion unit? The latter is cleaner; cost unmeasured.
4. **File streaming.** Is streaming a Parquet SSTable to a peer cheaper than re-encoding
   it? Probably yes; needs the receiver-side feature gate. (This is where tablet
   migration touches the format — but only as one caller of file streaming, not as a
   design input.)
5. **Counters.** Excluded in v1 — is there demand?
6. **Does folding Level 2 survive real data?** It requires row-group-uniform timestamps;
   real write patterns may break it often enough to make it useless.
7. **`zstd_with_dicts` inside Parquet** — worth the loss of external readability? §10.1
   should answer whether the marginal gain over plain zstd justifies it.
8. ~~**How should timestamp exceptions be encoded?**~~ **Resolved 2026-08-16.** Replaced
   the per-column leaves with a two-leaf sparse side-channel (`__tsx_mask` bitmap +
   `__tsx_vals` zigzag-varint deltas). Leaf count is now width-independent and the
   worst-case penalty fell from 2.68× to 2.05× — see §10.3c. §5.3 amended.

## 12. References

- ScyllaDB blog — [Scylla Student Projects, Part I: Parquet](https://www.scylladb.com/2020/08/05/scylla-student-projects-part-i-parquet/)
- Thesis — [Apache Parquet support for ScyllaDB](https://www.scylladb.com/wp-content/uploads/zpp_parquet.pdf) (Univ. of Warsaw, May 2020)
- [parquet4seastar](https://github.com/michoecho/parquet4seastar)
- [Shared-Dictionary Compression for SSTables](https://docs.scylladb.com/manual/stable/operating-scylla/procedures/config-change/sstable-dictionary-compression.html)
- [Compression in ScyllaDB, Part One](https://www.scylladb.com/2019/10/04/compression-in-scylla-part-one/) · [Part Two](https://www.scylladb.com/2019/10/07/compression-in-scylla-part-two/)
- Melnik et al., *Dremel: Interactive Analysis of Web-Scale Datasets*, CACM 54 (2011)
- [Apache Parquet format](https://parquet.apache.org/docs/) · [Encodings](https://github.com/apache/parquet-format/blob/master/Encodings.md)
- In-tree: [docs/dev/object_storage.md](docs/dev/object_storage.md) · [docs/dev/compaction_controller.md](docs/dev/compaction_controller.md)

### Validation datasets (§9.3)

- [ClickBench](https://github.com/ClickHouse/ClickBench) — `hits`, 100 M rows / 70 GB / 105 cols
- [Backblaze Drive Stats](https://www.backblaze.com/cloud-storage/resources/hard-drive-test-data) — 388 M+ records of sparse SMART telemetry
- [Public BI Benchmark](https://github.com/cwida/public_bi_benchmark) (CWI) — 386 GB, 46 workbooks / 206 tables
- [TSBS](https://github.com/timescale/tsbs) — Time Series Benchmark Suite, devops + iot generators
- [NYC TLC Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [OpenAddresses](https://openaddresses.io/) · [Stack Exchange data dump](https://archive.org/details/stackexchange)
- Kuschewski et al., *BtrBlocks: Efficient Columnar Compression for Data Lakes*, SIGMOD 2023 — [paper](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf) (Public BI reference ratios)
- Afroozeh et al., *The FastLanes File Format*, PVLDB 18 — [paper](https://www.vldb.org/pvldb/vol18/p4629-afroozeh.pdf)
