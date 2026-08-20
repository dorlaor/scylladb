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
| R-6 | Round-trip must be **lossless**: cell timestamps, TTL/expiry, all tombstone kinds, static rows, collections, counters. **Met 2026-08-17** — all of it, verified by `sstable_conforms_to_mutation_source_test` (§11 item 11). |
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
| Counter tables | see §11 item 11 | — | Supported 2026-08-17; one element per shard |
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
| Full scan, all columns | Parity to +30 % | Less I/O, more decode. **Met on the streaming path (1.02–1.13×), missed by 2.3× through CQL** — §10.26 |
| **Scan with column projection** | **1.5–5×** | The headline win; reads only the projected column chunks. **Not implemented** — the reader reads every leaf on every path, §10.26 |
| Aggregations / analytics | Large win | Projection + page-index skipping via min/max. Neither reaches a scan yet; §10.26 |
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
the user's CQL schema. **Lossy — export only, never a storage mode.** Implemented and
reachable via `scylla sstable parquet-export --folding logical`. Enforced in three
places: `reassemble()` throws, `folding_is_lossless()` reports it, and the storage path
uses `to_parquet_for_storage()` which rejects any lossy level. Worth ~0 bytes versus L1
(§10.3g) — it exists for interoperability, not size.

Which columns were materialised is recorded explicitly, so the reader knows a missing
column means "trivially absent" rather than "unknown". Omitted-column defaults live in
the file's key-value metadata.

Type mapping (`$X_TYPE`) follows the thesis for scalars and Parquet logical type
annotations for the rest; `decimal`, `varint`, `inet`, `duration`, `date`, `time`,
`timeuuid` map to the corresponding Parquet logical types over `BYTE_ARRAY`/`INT32`/
`INT64`/`FIXED_LEN_BYTE_ARRAY`. Frozen collections and UDTs are opaque `BYTE_ARRAY` in
v1 (they already are, internally); non-frozen collections use Dremel `repeated` groups.

### 5.4 Ordering and random access

**Index design — decided 2026-08-16.** Scylla's index has two jobs, and Parquet answers
them very differently:

1. **Partition lookup** (decorated key → position). Parquet has *no* key index, by design
   — parquet-cpp and arrow-rs are scan engines, and Arrow still has an open issue for
   fast random row-group reads. Nothing to copy; Scylla must supply this, which is why
   §5.2 keeps our own index components.
2. **Intra-partition seek.** Parquet's ColumnIndex gives this natively — see open
   question 2.

For job 1, `index_entry::position()` is a `uint64_t` that today means a byte offset into
Data. Four options were considered; **option A was chosen**: store the **logical row
ordinal**. Parquet's OffsetIndex records `first_row_index` per page, so a binary search
turns an ordinal into the single page that must be decoded — one page decode per projected
column, which is exactly the cost §4 assumed when it promised p99 ≤ 1.2× native.

| | Stored value | Verdict |
|---|---|---|
| **A** | **Row ordinal** | **Chosen.** Uses Parquet's own mechanism; survives row-group resizing. Costs OffsetIndex emission, which §3.5 wanted anyway |
| B | Packed `(row_group << 32 \| row)` | Rejected: without an OffsetIndex the reader decodes from the row-group start, strictly worse than A at 64 MiB groups |
| C | Row group's byte offset | Fallback. Keeps the field a genuine byte offset, but a point read decodes from the group start |
| D | No Scylla index; use row-group min/max | Rejected: viable because data is sorted, but gives up the O(1) partition lookup R-11 depends on, and needs a third branch in `generate_toc` |

**Audit of the semantic change.** Repurposing `position()` was the risk. Grepping every
consumer found the offset arithmetic confined to `mx/bsearch_clustered_cursor.hh`, which a
`pq` reader never uses. One generic consumer looked like it did arithmetic —
`sstable::estimated_keys_for_range`.

*Resolved on implementation (2026-08-16): no branch was needed.* That function has two
paths, and the byte-offset one (`(end - start) / data_size()`) is reached only when the
sstable has a BTI `Partitions.db` footer. `pq` writes a Summary and an Index, so it takes
the other path, which ratios *summary page counts* and never touches `position()`. The
predicted one-line version branch turned out to be zero lines — worth recording because
the audit's conclusion was right (not a blocker) for a reason that was slightly wrong.

**Implementation status (2026-08-16).** Option A is implemented and exercised end to end:

- `pq_writer_impl::consume_new_partition` writes an mc-shaped index entry — a
  `uint16`-length-prefixed key, then a vint — where the vint is the partition's first
  **row ordinal**. The trailing promoted-index-size vint is always `0`, per open
  question 2.
- The writer emits Parquet's OffsetIndex (`write_page_indexes`), and
  `offset_index::page_for_row()` binary-searches `first_row_index` to turn an ordinal
  into a page.
- Summary, Filter, Statistics, `Scylla.db` and the TOC are written, so a `pq` sstable is
  loadable rather than merely parseable. `test/boost/sstable_parquet_test.cc` asserts each
  component exists and that every written key is found by the filter.

The reader shipped in this change does not yet *use* the ordinal: it decodes the whole
image and serves from memory (§11, item 10). The index is written and verified so that the
streaming reader is a drop-in replacement rather than a format change.



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

- Row group size is bounded by **both** `rows_per_row_group` and `row_group_bytes`,
  whichever trips first. (As shipped these are not co-equal: the row count is the dial and
  the byte budget -- `row_group_buffer_bytes` -- is a guard rail that does not bind at the
  default. See §8.2 and §5.5a.)
- Classic Parquet advice (128 MB – 1 GB, sized to an HDFS block) is **wrong for
  Scylla**. Concurrent compactions × shards × row group bytes is the memory bill.
  Proposed default: **64 MiB** uncompressed, with `rows_per_row_group` default 1 000 000.
  (Superseded — `rows_per_row_group` shipped at 1 000 000 and was moved to **5 000** on
  2026-08-18 once the read cost of a large row group was swept; §10.4c.)
- Buffering is charged against a dedicated semaphore
  (`parquet_writer_memory_budget`, default a small fraction of shard memory) so that
  compaction back-pressures rather than OOMs.
- Page size default **64 KiB** (thesis used 64 KiB; also what caps decompression work).
  A hard reader-side cap (`max_page_bytes`, default 4 MiB) rejects pathological files —
  this is the R-12 guard the thesis explicitly recommends.
- Trade-off to measure: larger row groups → better ratio, worse point-read latency and
  more memory. This is the primary tuning axis and the reason §8 exposes it.

### 5.5a Write-side memory — measured 2026-08-17, and R-13 is **not** met here

§5.5 above specifies what should happen. What the writer actually does is emit **one row
group per sstable**: `write_rows()` calls `add_row_group()` exactly once, `rows_per_row_group` is
declared in `pq_writer_config` and never read, `row_group_bytes` does not exist, and there is
no semaphore. So `fragment_shredder::_rows` accumulates every row of the sstable before
anything is encoded.

Measured with `scylla sstable parquet-export` on the D12 table, peak RSS against row count:

| Rows | Peak RSS | SSTable in | Parquet out |
|---:|---:|---:|---:|
| 30 000 | 115 MiB | 356 kB | 179 720 |
| 300 000 | 582 MiB | 3.6 MB | 1 548 377 |

Ten times the rows costs 5.1× the memory, so this is a real linear term and not seastar's
up-front reservation. Solving the two points gives **≈1.77 kB of buffered write memory per
row** on top of a ≈63 MiB baseline, which projects to:

| Rows in one sstable | Buffered |
|---:|---:|
| 1 000 000 | 1.8 GiB |
| 10 000 000 | 17.0 GiB |
| 100 000 000 | 169.1 GiB |

Multiply by concurrent compactions × shards and this is the OOM that R-13 exists to prevent.
The read path is bounded (1.13× memory for 8× rows, §10.4); the write path is not.

**A refinement to §5.5 that the measurement forces.** §5.5 proposes bounding a row group by
`row_group_bytes` (64 MiB uncompressed) and `rows_per_row_group` (1 000 000). Neither is the right
budget on its own:

- The quantity that can OOM a shard is the **shredder's in-memory footprint**, not the encoded
  row-group size. They differ by a lot: 1.77 kB/row in memory against 5.2 B/row of compressed
  output on D12 — **343×**. Budgeting 64 MiB of *encoded* bytes would therefore permit
  something on the order of gigabytes of shredder memory. The budget has to be charged where
  the memory actually is.
- `rows_per_row_group = 1 000 000` is roughly **50× too large** for the current row model: a
  64 MiB shredder budget is about **37 000 rows**. The byte budget would trip first every
  time, which is exactly why §5.5 says "whichever trips first" — the row count is a backstop,
  not the operative limit. **Acted on 2026-08-18:** the default is now 5 000, which makes the
  row count operative and the byte budget the backstop — the sound way round, since the row
  count is what read cost depends on (§10.4c).

**This also bears on point-read latency.** One row group per sstable means one dictionary page
per column covering the entire sstable, and a dictionary must be decompressed in full before a
single value can be decoded — the effect that dominated point reads until the threshold was
tightened (§10.4). Cutting row groups shrinks every dictionary, so it is plausibly a larger
point-read win than caching decoded state, and it should be measured before the caching work
rather than after.

Two further notes on the row model itself, since 1.77 kB/row is high for ten columns: `row`
holds a `std::map<size_t, cell>` and each `value` carries a `std::string`. A flat vector keyed
by column index, and interning or borrowing the value bytes, would cut the constant
substantially — worth doing regardless of where the budget lands.

#### Step 1 delivered: the budget has a number to cut on — calibrated 2026-08-17

`fragment_shredder` now accounts for the heap its buffered rows hold, accumulated as rows
are appended (`buffered_bytes()`). Every row enters through one `push_row()` so the
accounting cannot drift away from the buffer — the same mistake the static-cell replay made
when it had three copies of a two-line loop.

The estimate deliberately **errs high**: under-counting is what OOMs a shard, over-counting
only cuts a row group slightly early. Validated against measured RSS rather than trusted —
`parquet-export` now reports `buffered_bytes_estimate`, so the two can be compared directly:

| Rows | Estimate | Peak RSS |
|---:|---:|---:|
| 30 000 | 56 610 000 | 119 771 136 |
| 300 000 | 566 100 000 | 609 484 800 |

The RSS slope is **1 814 B/row** (the intercept is a ≈62 MiB fixed baseline); the estimator
says **1 887 B/row**. So it is **4 % conservative** — accurate enough to budget on, and wrong
in the safe direction. At a 64 MiB budget that is **≈35 600 rows per row group**. When this was
written the declared `rows_per_row_group` default was 1 000 000, so the byte budget was the operative
limit and the row count a dead letter. §10.4c moved the default to **5 000**, which reverses that:
5 000 rows is about 9 MB of shredder buffer, well under the budget, so the row count now cuts and
the byte budget is what it should be — a safety net against a pathological partition.

`test_parquet_buffered_bytes_accounting` pins the properties the budget depends on — it counts
heap and not just the struct, it scales with rows, every row path feeds it including range
tombstone changes, and `clear()` resets it — and was mutation-checked by making it count
`sizeof(row)` only.

#### Step 2a delivered: the conservative leaf set is a real, tested option

`leaf_set::derived` / `leaf_set::conservative` is now a parameter of `map_schema()` rather than
the throwaway switch the cost was first measured with. `conservative` forces every optional
metadata leaf on, including the divergence channel — without that last one a later row whose
cell timestamp differs from its row timestamp would have nowhere to record the difference.

**The 540-case losslessness suite now runs over both leaf sets** — 1 080 cases became **2 160,
all passing**. That matters more than a bespoke test would: the conservative set exercises a
reassembly path the derived set never reaches, with every optional leaf present but null, and
losslessness has to hold there or cutting row groups is unsafe.

Two guards, because "the suite passes" is not evidence on its own:

- The conservative leaf set must never be *smaller* than the derived one, and must sometimes be
  larger. It is wider in **720 of the 1 080** case pairs; the rest are cases whose data already
  needed every leaf. Without this check a silently-ignored flag would leave all 2 160 cases
  passing and prove nothing.
- Mutation-checked: making `leaf_set::conservative` a no-op fails the suite with
  "conservative leaf set was never wider", which is the diagnosis rather than a vague mismatch.

#### The obstacle to cutting row groups, and the way through it — measured 2026-08-17

Cutting row groups is not just a matter of calling `add_row_group()` more than once. Parquet
requires **one leaf set for the whole file**, fixed before the first row group is written, and
the pq leaf set is *derived from the data*: `scan_rows()` walks every row to decide whether the
file needs `__ttl_<col>`, `__ldt_<col>`, `__rm`, the row/partition tombstone groups, the
divergence channel, and whether L2's uniform-timestamp precondition holds. An incremental
writer does not know, at its first flush, whether row ten million carries a TTL.

Three ways out, and only one survives contact:

1. **Decide from the first row group.** Unsound: a later row with a TTL needs a leaf that does
   not exist, and there is no way to add one.
2. **Two passes.** Defeats the purpose — the first pass is the buffering we are trying to remove.
3. **A conservative leaf set**: emit every optional metadata leaf regardless of use. Sound, and
   compatible with the existing reader without changes, because the reader recovers the flags
   from leaf *names* and will simply see them present. The unused leaves are all-null, so they
   cost definition levels that RLE away plus a fixed per-leaf overhead.

**Measured cost of the conservative leaf set** (temporary switch, since reverted):

| Table | Regular cols | Rows | Derived | Conservative | Leaves | Cost |
|---|---:|---:|---:|---:|---:|---:|
| D12 ISD-Lite | 8 | 300 000 | 1 548 404 | 1 587 490 | 20 → 43 | **+2.52 %** |
| D2 Backblaze | 197 | 20 000 | 1 261 238 | 1 352 258 | 199 → 604 | **+7.2 %** |

The overhead is **≈225 bytes per extra leaf** — page header plus column-chunk metadata in the
footer — so it is a *fixed* cost, not a proportional one. That is the important part, because it
means the cost falls exactly where it needs to:

| Case | Projected |
|---|---:|
| Backblaze at 300 000 rows (16.5 MB) | +0.55 % |
| ISD at 300 000 rows, amortised | +0.33 % |

**So the design is self-adjusting.** Use the derived leaf set when the whole sstable fits inside
one row group — which is when the fixed overhead would hurt, and also when no cutting is needed —
and switch to the conservative leaf set only once the budget forces a cut, which happens only on
large sstables where 225 bytes per leaf is noise. No configuration required, and the small-file
sizes measured throughout §10 are unaffected.

**A single partition larger than the budget stays in one row group — decided 2026-08-18.**

Cutting only at partition boundaries means an oversized partition overshoots the budget rather
than being split. That is the intended behaviour, not a deferred fix. Splitting one would cost:

- the index entry carrying `(row group, ordinal)` instead of a bare ordinal (§5.4 option A), and
- a point read spanning row groups, so the reader would have to stitch a partition together
  across chunk boundaries.

Both are complexity paid on **every** read to bound a rare case, so the budget is a **target,
not a guarantee**.

The residual exposure, stated plainly: at ~1.9 kB of buffered shredder memory per row (§5.5a),
a single partition of one million rows would hold ~1.8 GiB while it is being written, and
nothing stops it. Two things make that tolerable rather than alarming — Scylla already tracks
large partitions and warns about them (`maybe_record_large_partitions`, which the pq writer
feeds), so this is visible rather than silent; and a partition that large is a schema problem
the operator needs to know about independently of the storage format. If it ever needs bounding,
the cheap answer is a hard ceiling at some multiple of the budget that forces a cut only in the
pathological case, paying the index complexity there and nowhere else.

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
ALTER  TABLE ks.t       WITH storage_format = 'hybrid';    -- sstable | parquet | hybrid
```

That is the entire control surface. There is deliberately no per-tablet override, no new
system table, and no group0 work: schema properties already replicate through the schema
tables, so the mechanism exists and is well understood.

**This is exactly how `compression` already behaves**, which is the precedent worth
leaning on. Changing a table's compressor today is an `ALTER TABLE` followed by a
background rewrite; changing its storage format is the same operation with a different
output encoder. Nothing about that reasoning involves tablets.

### 6.2a Convergence: wired, including `'hybrid'` (2026-08-18)

`storage_format` **converts on compaction**, in both directions, for all three settings.
`compaction/compaction_manager.cc` picks the output format once per compaction, before the
sstable creator is installed, because the creator is synchronous and C6 has to read data:

- **`'sstable'`** — native, the preferred writable version.
- **`'parquet'`** — `pq`, taken at face value. The criteria exist to make the *automatic* choice;
  overruling an operator who named the format would make the property mean nothing. Set it
  back to `'sstable'` and the next compaction converts back.
- **`'hybrid'`** — depends on the compaction strategy:
  - **under ICS**, `decide_output_format()` decides per compaction, and the decision is logged
    either way with the criterion that settled it. An operator who sets `'hybrid'` and sees
    nothing convert has no other way to find out which check said no.
  - **under TWCS**, identical to `'parquet'`: the whole table, no criteria, nothing to log
    (§6.3). A closed window is never rewritten, so there is nothing for the criteria to prevent.

Covered by `cql_ddl_test/test_storage_format_converts_on_compaction`, which drives native ->
Parquet -> native and reads every key back after each switch rather than trusting the format
to have changed. Mutation-checked by disabling the branch. **Converting back is tested
deliberately** -- it is the direction nobody checks, and a table that cannot be un-converted is
a trap rather than a feature.

**How C1 reaches the decision.** The tier is the one input only the strategy can supply, so
`compaction_descriptor` now carries a `sstables::parquet::compaction_context` that the strategy
fills in:

| Strategy | What counts as bottom tier |
|---|---|
| any, major compaction | always — one output, nothing larger can follow |
| **ICS** | the picked bucket contains the largest candidate sstable, over the sstables its runs expand to |
| **TWCS** | *not evaluated* — a TWCS table is entirely Parquet, see below |
| STCS | same size rule as ICS (out of scope — §6.4) |
| LCS | the output level is the deepest level currently holding data |

The default is `false`, which is the conservative answer: a strategy that says nothing gets no
conversion. ICS's rule is deliberately stated as "includes the largest candidate" rather than in
terms of tier boundaries, because it buckets by size *ratio* and a bucket index means nothing
across tables.

**TWCS does not use the criteria at all.** Under TWCS, `storage_format = 'hybrid'` means the same
thing as `'parquet'`: every sstable the table writes is Parquet, with no per-compaction decision.

The reason is that the criteria answer a question TWCS does not have. Hybrid tiering exists to keep
Parquet out of the levels that get *rewritten* — re-encoding and recompressing a Parquet run is the
expensive thing this format does, and C1 is how the policy avoids paying it. TWCS has no such
levels: a window is compacted and then closed, so no output is rewritten and there is nothing to
protect. Leaving part of a TWCS table in the row format buys nothing and costs a mixed-format table.

`sstables::parquet::writes_parquet_unconditionally()` is the single home for the rule, because
compaction, memtable flush and streaming all consult it and a table whose flushes disagree with its
compactions never converges. Under TWCS it also skips C6's data sample, which is the most expensive
part of the decision.

This matters because TWCS is the recommended strategy for any schema with a timestamp in the
clustering key, and that shape — sorted timestamps, delta-encodable, wide runs of repeated values
per station or host or device — is what Parquet is best at: NOAA ISD-Lite hourly observations are
the corpus's largest win at **46.8 %** of the SSTable at 300 k rows and **43.2 %** at 16 M (§10.6).

**What is given up, stated plainly.** C6 is the measurement that catches a schema Parquet stores
*worse* than the row format, and skipping it means a table that should not be converted will be.
The 197-column sparse-telemetry dataset measures at **208 %** of its SSTable (§10.4) — put that
schema on TWCS with `'hybrid'` and it will roughly double its disk instead of being declined.
`storage_format = 'sstable'` is the way to hold such a table in the row format; there is no
automatic protection under TWCS any more. The trade is deliberate: TWCS tables are overwhelmingly
time series, which is the shape that wins most, and the decision is now free rather than costing a
data sample per compaction.

An earlier design did evaluate C1 under TWCS, using "the picked bucket is a window other than the
last active one" — an exact answer where ICS's size rule is only a proxy. It is recorded here
because it worked and was removed on purpose, not because it was missing: with the rule above,
nothing reads it.

**Verified on a live node, 2026-08-19** (`~/pq-lab/twcs_hybrid_check.py`). A TWCS table declared
`'hybrid'`, 200 000 rows, flushed to 6 native sstables, then rewritten with
`keyspace_upgrade_sstables` — **all 6 became `pq`**, and all 200 000 rows read back.

The rewrite path is the point. A *major* compaction sets `bottom_tier` unconditionally, so it
converts under any version of this rule and proves nothing; the rewrite descriptor
(`rewrite_sstables_compaction_task_executor::make_descriptor`) leaves `parquet_ctx` at its default,
so `bottom_tier` is false and the old policy declined it at C1. The same script runs an **ICS
control**: identical rows, identical `'hybrid'` setting, identical rewrite — and the files stay
native, because there the criteria still apply. Without the control a pass would only show that
something converted, not that this is what converted it.

**How C6 reaches it.** `sstables/parquet/gain_estimator.cc` runs the real writer over a bounded
sample — up to 100 000 rows or 256 MiB of shredder memory, stopping only at a partition
boundary — of the largest input sstable, and compares the Parquet bytes against
`ondisk_data_size()` scaled by the fraction of partitions read. On-disk, emphatically not
`data_size()`: that returns the data component's *uncompressed* length, and comparing a
compressed Parquet file against it would report the native compressor's savings as ours.

The policy is evaluated twice. The first pass substitutes a passing gain so that only C1 and C5
run: there is no point reading data for a candidate that is in the wrong tier or too wide, and
the sample is by far the most expensive part of the decision. A failed or unusable estimate
returns "unknown", which the policy treats as a rejection — **failing to measure must never be
a reason to convert.** `sstable_parquet_test/test_c6_parquet_gain_is_measured_over_real_data`
asserts all of that, including determinism, so the decision cannot flap between compactions.

**A precondition on every measurement from now on.** `~/pq-lab/ensure_fresh_node.sh` compares
the running node's process start time against `build/dev/scylla`'s mtime and restarts it if the
binary is newer; the three measurement scripts abort if it cannot guarantee a fresh node.
Replacing a binary does not replace a running process, and this cost real time three times in
this project — most expensively when the corrected C2 threshold appeared to do nothing because
the node still held the old one and logged the stale value back. A harness that can silently
describe code no longer in the tree is worse than no harness. Verified in both directions: it
reports a fresh node, and after `touch`ing the binary it detects staleness and restarts.

**Observed running, 2026-08-18.** A 2 000-row table created `WITH storage_format='hybrid'` on a
live node, then flushed and major-compacted. The node logged one decision per compaction:

```
pqlab.hybrid_probe: hybrid storage_format chose native for this compaction:
    output 2079 B is below the 268435456 B minimum
```

Three compactions, three decisions, and the output stayed at version `me`. So the switch runs,
the policy is consulted per compaction, the criterion that settled it is named with the actual
numbers, and nothing converted — which is the correct answer for a 2 kB output.

**Converted, and the C6 estimator checked against its own outcome (2026-08-18).** With C2's
floor corrected (§10.1f-c2), a 300 000-row NOAA ISD-Lite table set to `'hybrid'` and
major-compacted traversed the whole chain and converted:

```
pqlab.isd_realistic: hybrid storage_format chose parquet for this compaction:
    bottom tier, 3713670 B, garbage 0.000, predicted gain 0.540
```

The output is a single `pq` sstable of 1 802 231 B. Every criterion is accounted for: C1 by
"bottom tier" (a major compaction), C2 by 3.7 MB against the 256 KiB floor, C4 by a garbage
fraction of 0.000, C5 silently by 20 leaves against the ceiling of 128, and C6 by a gain the
estimator **measured on the real data rather than guessed**.

**The estimator predicted 0.540 and the conversion delivered 0.515** — 1 802 231 of 3 713 670 B,
so 51.5 % saved against 54.0 % predicted. Off by 2.5 points, under 5 % in relative terms, and in
the optimistic direction. That is the strongest available check on C6: not a unit test against a
fixture but the estimate compared with what the writer then actually produced from the same data.

**C2's floor is measured per compaction output, not per table, and that makes it far harder to
reach than table size suggests.** A 298 MB wide table was loaded specifically to get past it. The
major compaction produced four outputs of 13-26 MB, each of which C2 declined:

```
pqlab.c5_probe: hybrid storage_format chose native for this compaction:
    output 16537625 B is below the 268435456 B minimum
```

Compaction runs per compaction group, so a table is compacted in independent pieces and the
output size is the piece, not the whole. On this host a table would need to be roughly 4 GB
before any single output crossed 256 MiB. Two consequences: **C2 as defaulted is much more
restrictive than section 6.3 implies**, and it means C5, C6 and the estimator still cannot be
observed through the compaction path without a multi-gigabyte table. That is a threshold worth
revisiting -- 256 MiB was argued as "4 row groups at 64 MiB", which was reasoning about a single
file and not about how compaction actually divides work.

**A limit of that verification worth stating:** C2's floor therefore rejects before any later
criterion is reached, so a hybrid table cannot exercise C5, C6 or the estimator until it is
genuinely large. Observing those end to end needs a >256 MiB dataset on this host, which is
also why C6's estimator is covered by a unit test against a real sstable
(`test_c6_parquet_gain_is_measured_over_real_data`) rather than through the compaction path.

**Operational surface, verified end to end 2026-08-18.** All of it exercised against a real `pq`
sstable on a live node rather than reasoned about:

| Operation | Result |
|---|---|
| `nodetool tablestats` compression ratio | **0.0732** on a `pq` table — previously reported nothing at all |
| `ALTER TABLE ... ADD` over existing `pq` sstables | new column reads as `null` from files written before it existed |
| writing the new column, then compacting old and new together | all 3 000 rows correct; pre-ALTER rows keep `null`, post-ALTER rows keep their values |
| `ALTER TABLE ... DROP` | dropped column disappears, remaining columns intact |
| reading a **mixed `me` + `pq`** sstable set | correct throughout — 18 files of both versions during the checks above |
| `sstable dump-data` | 4 partitions, 20 000 rows — the whole dataset |
| `sstable dump-statistics` / `dump-index` / `dump-summary` | parse and emit |
| `sstable validate` | `errors: 0, valid: true` |
| `sstable validate-checksums` | `has_checksums`, `has_digest`, `valid` — the digest and CRC the writer emits are verified, not merely present |
| `nodetool upgradesstables` | converts `me` → `pq` |
| `nodetool scrub` (validate mode) | passes; all 4 000 rows still readable afterwards |

Exit status alone was not taken as evidence: every row above was checked for substantive output,
because a tool that silently does nothing also returns zero.

**Schema evolution, verified 2026-08-19.** A Parquet file fixes its schema when it is written, so
a table that gains a column afterwards has sstables with fewer leaves than the schema now declares.
That resolves correctly: the reader maps by column rather than by position, so a column added after
a file was written reads as `null` from that file, and a later compaction that merges pre- and
post-`ALTER` sstables preserves both. `DROP` behaves symmetrically.

**The incidental result is the more valuable one.** Those reads happened against a *mixed* sstable
set — 18 files, some `me` and some `pq`, because flushes stay native and only compaction converts.
§6.2 claims reads are format-agnostic; this is the first time that has actually been exercised
rather than asserted, and it is the property the whole convergence design rests on. A table
mid-conversion is the normal state, not an edge case.

**Compression ratio, added 2026-08-19.** A `pq` sstable has no `CompressionInfo` component —
Parquet compresses inside the file — and `sstable::get_compression_ratio()` looked only there, so
it returned the "not recorded" sentinel for every Parquet table. `nodetool` and the REST API
therefore showed *no* ratio for a table that has a perfectly good one to report.

The writer now records it from the Parquet footer: numerator the file it wrote, denominator the sum
of the column chunks' uncompressed sizes, which is the serialised volume before the codec. The
accessor falls back to the statistics value when `CompressionInfo` is absent; formats that record
nothing still leave the sentinel, so nothing else changes.

Measured **0.0732** on the three-column time series through the REST API. Worth noting that
§10.1f-raw independently measured the same table at **7.1 %** of its raw SSTable volume by an
entirely separate route — reloading with compression disabled. Two different denominators (Parquet
chunk sizes versus Scylla's uncompressed serialisation) agreeing to within 0.2 points is good
evidence both are right.

**What is still missing:**

- **C7's missing data source: investigated 2026-08-18, solved 2026-08-19, criterion dropped anyway.**
  The original finding was that nothing could answer "is this table point-read dominated":
  `replica::table_stats` had no counter separating single-partition reads from range scans (`reads` is
  a latency histogram over all reads — the count is there, the shape is not), and `live_scanned`, the
  obvious proxy, is **exposed through the REST API and `nodetool tablehistograms` and incremented
  nowhere in the tree**, so a proxy built on it would have silently reported zero — "not point-read
  dominated" — and converted exactly the tables the criterion existed to protect.

  Two corrections since. First, the sweeping form of that claim was wrong: `scylla_sstables_single_partition_reads`,
  `scylla_sstables_range_partition_reads` and `scylla_cql_select_partition_range_scan` all exist and
  all make the distinction — they are merely per *shard*, where a per-table decision needs per-table
  numbers. Second, that gap has since been closed: `single_partition_reads` and `range_scan_reads` are
  now per-table counters (§10.14). `live_scanned` remains an unpopulated stub and nothing uses it.

  So the instrumentation objection no longer applies, and **C7 was still dropped** — see §6.3. Having
  the input is not the same as having evidence that the criterion decides better than C5's column
  ceiling, and the standing rule is that a criterion arrives with a measurement rather than a
  rationale.
- **Flushes are never Parquet**, only compaction outputs. That matches §6 (the bottom tier is
  where the value is) but means a freshly flushed table stays native until it compacts.
- **Streaming now honours the format, fixed 2026-08-18.** `table::make_streaming_sstable_for_write()`
  and `make_streaming_staging_sstable()` created sstables without consulting `storage_format`, so
  everything arriving by stream — repair, bootstrap, decommission, and `nodetool refresh`, which in
  load-and-stream mode re-streams a snapshot's partitions rather than adopting its files — landed
  as native on a table declared `'parquet'`. Data was intact; the format silently regressed until
  natural compaction happened to convert it.

  Found by round-tripping a snapshot: snapshot a `pq` table, truncate, refresh. The rows came back
  correct and the sstables came back as `me`. Verified fixed by re-running the same round trip —
  5 000 rows, spot check correct, versions `pq`.

  `'hybrid'` **under ICS** keeps the native format for streamed data on purpose: it has just
  arrived, so it is not bottom-tier and C1 would decline it anyway. Writing Parquet for data that is
  about to be compacted again is the specific thing the tiering policy exists to prevent.
  `'hybrid'` **under TWCS** streams Parquet, because there it means `'parquet'` (§6.3). Both come
  from `writes_parquet_unconditionally()`, which is also what compaction and flush consult — one
  rule, three callers, so they cannot drift into a table that never converges.
- **Every other write path that bypasses compaction, audited 2026-08-18.** The streaming bug was a
  class, not an instance: any path that creates an sstable *without* going through
  `compact_sstables()` never consulted `storage_format`. All of them enumerated by grepping for
  creator assignments rather than by recall:

  | Path | Before | Now |
  |---|---|---|
  | streaming / repair / bootstrap / refresh | native | honours `'parquet'` |
  | reshard and reshape on load | native, via `get_safe_sstable_version_for_rewrites()` which only knows native versions | honours `'parquet'` |
  | split compaction | native, via `make_sstable(sst->state())` | honours `'parquet'` |
  | sstable-directory rewrite | already correct — uses `desc.version`, so it preserves the source | unchanged |
  | memtable flush | native | unchanged, **deliberately** — see above |

  The pattern worth carrying forward: `compact_sstables()` was the only place anyone thought to
  wire the format, because it is the only place that *chooses* a format. The paths that merely
  *rewrite* data all defaulted to the node's preferred native version, and each was a silent
  downgrade of a table that had asked for Parquet.
- ~~**`nodetool upgradesstables` does not force convergence.**~~ **Wrong — verified 2026-08-18:
  it does.** A native table altered to `storage_format = 'parquet'` and then upgraded, with no
  compaction triggered, came out as `pq`. The creator installed in `compact_sstables()` is shared
  by the rewrite paths, so upgrade got convergence for free; this note predated that wiring and
  was never rechecked. It matters operationally because `upgradesstables` is how an operator
  forces a rewrite without waiting for natural compaction.
- **The estimate is not cached.** One sample per converting compaction. Bounded, but a table
  that repeatedly fails C6 pays for the sample every time it reaches the bottom tier.

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

This applies to **ICS only**. Under TWCS, `hybrid` and `parquet` are the same setting and none of
these criteria are evaluated — a window is compacted once and then closed, so there is no repeated
rewrite for them to prevent (§6.3, §6.4).

**C1 — Bottom tier / terminal position.** The output must be in the largest size tier — for ICS,
the top size bucket. Operationally: *expected remaining rewrites ≤ 1*. Converting data that will be
re-compacted three more times pays the encode cost repeatedly for no benefit.

**C2 — Removed 2026-08-18, subsumed by C6.** This was a minimum output size. A file too small
to pay is exactly one whose *measured* gain is bad: NOAA ISD-Lite at 5 000 rows — one row group —
came out at 111.7 % of the SSTable, a gain of −0.117, which fails C6's 0.15 floor on its own
(§10.1f-c2). So C2 was re-deriving, from a byte count, a conclusion C6 reaches by measuring, and
it was doing so shape-blind: four row groups is 126 kB at 6.3 B/row and 1.4 MB at 69 B/row, so no
single threshold was right for both. Its whole history is a caution — the original 256 MiB was
1000× too high and silently prevented every conversion.


**C4 — Removed 2026-08-18.** This was a maximum droppable-tombstone fraction, default 0.10, on
the reasoning that high garbage density implies an imminent GC rewrite. Two problems. The 0.10 had
no measurement behind it, and a reasoned round number is precisely the category that turned out
wrong for C2, for C3 and for the numeric-dictionary figure. And the cost it avoided was one wasted
encode: a bottom-tier output that is then tombstone-GC'd gets re-evaluated by this same policy on
the rewrite, so the error is self-correcting.


**C5 — Schema eligibility and width, in columns.** Bounded on **CQL columns**, not Parquet
leaves. That is a correction of units, not a change of value: §10.4e's latency curve was
parameterised by columns all along — its schema is pk + ck + 5 values + N extra — and labelling
that axis "leaves" was sloppy.

The substantive reason is that a leaf count cannot be known from a schema. The old
`estimated_leaf_columns()` returned `columns + 3` and read **13** for the NOAA ISD-Lite table
that `parquet-export` reports **20** leaves for, because per-column deletion and TTL leaves
materialise in L1 only when cells actually carry them. The leaf count is data-dependent. With C2,
C4 and C7 gone this criterion is one of three, and a load-bearing criterion should not rest on a
quantity that is guessed — especially one that guessed 35 % low, which would have let a table
of ~195 real leaves through a ceiling meant to stop it.

**C5 — the original wording follows.** Folded leaf count within `parquet_max_leaf_columns`. As of
2026-08-17 nothing else is ineligible: counters and non-frozen collections are both
representable, and every other type falls back to an opaque blob column. The gate is kept
as the place a future encoding gap belongs — see §11 item 11.

**C6 — Predicted gain.** A sampling estimator predicts ≥ `parquet_min_gain_ratio`
(default **15 %**) saving versus the table's current compressor. **Do not guess —
measure.** Implemented in `sstables/parquet/gain_estimator.cc`; see §6.2a for how it samples
and §10.1f for why no formula would do — the corpus spans 0.47× to 0.85× with the same
folding and the same codec.

**C7 — dropped, 2026-08-19. Not deferred: dropped.** A read-pattern gate — refuse conversion when a
table is mostly point-read, however well it would compress — was removed from the policy on
2026-08-18 because nothing could answer "is this table point-read dominated". It is now removed as a
*requirement* as well, and the reasoning is worth stating because the input problem was subsequently
solved and the criterion still did not earn its place.

Per-table `single_partition_reads` and `range_scan_reads` counters exist (§10.14), so the original
objection no longer holds. What does not exist is a measurement showing the criterion would decide
better than what is already there. **C5's column ceiling is the answer, not a stand-in**: point-read
cost is linear in leaf count at ~90 µs each, so past 128 columns a table is too slow to point-read as
Parquet however well it compresses (§10.4e), and that is derived from measurement rather than from a
guess about workloads.

C5's known error is a false *negative* — it declines a wide table that is only ever scanned, the case
where Parquet is fastest. C7 would fix that, and would introduce a false positive of its own: read
mix is measured over a window and a table's mix changes, so a criterion built on it converts on last
week's traffic. Between a conservative error that leaves data in the row format and an optimistic one
that rewrites a table into the wrong format for its present workload, the conservative error is the
one to keep.

**What replaces it for an operator who knows their workload is scan-only.** `storage_format =
'parquet'` is taken at face value and overrides the criteria entirely (§6.2a) — so a wide scan-only
table is one `ALTER` away from being converted, deliberately, by someone who knows something the
policy cannot. The counters are there to inform that judgement. An automatic criterion is not needed
to reach the same outcome, and an operator saying "this table is scanned" is better evidence than a
window of counter samples.



C6 is what makes this safe: it turns "will Parquet help this schema?" — which §3.4 can
only guess at — into a measurement on the actual data, before any bytes are rewritten.

### 6.4 Compaction strategy interaction

Hybrid mode makes bottom-tier rewrites more expensive, since merging into a Parquet run
means re-encoding and recompressing it.

**Two strategies are in scope: ICS and TWCS.** That is the whole supported set — the choice
between them is by data shape, not by preference:

- **TWCS for any time-based clustering key.** It is the best fit and the recommended default for
  time series, and it is the one strategy where the format needs no tiering decision: a window is
  compacted once and then closed, so the merge cost Parquet makes expensive never arises.
  **A TWCS table is entirely Parquet** — `'hybrid'` and `'parquet'` are the same setting there
  (§6.3). Hybrid mode, and the criteria behind it, apply only to ICS.
- **ICS otherwise.** Its runs are fragmented into bounded SSTables, so a merge rewrites only
  the overlapping fragments. That bounds exactly the write amplification Parquet makes
  expensive.

**STCS is out of scope.** It is superseded by ICS for every case that matters here, and a
top-bucket rewrite re-encodes an entire tier — the one access pattern this format handles
worst. The size-tier `bottom_tier` rule still compiles for STCS because ICS shares it, but
STCS + Parquet is neither validated nor measured, and no benchmark in §10 uses it. LCS is
likewise not a target.

The strategy needs one new input: for a candidate compaction, "will the output satisfy
C1, C5 and C6?" That is a call into the policy engine when the `compaction_descriptor` is built,
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
| **Counters** | Counter cells have shard-level internal structure | **Supported 2026-08-17.** One element per shard, keyed by shard id, value and logical clock packed into the element value. Not self-describing to external readers — see §11 item 11 |
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
ALTER  TABLE ks.t       WITH storage_format = 'hybrid';    -- sstable | parquet | hybrid
```

### 8.2 Per-table Parquet parameters

**Status 2026-08-18: working end to end.** `ALTER TABLE ... WITH parquet = {...}` parses,
validates, persists, survives a schema reload, and reaches the writer.

The chain: `cf_prop_defs` validates by constructing `parquet_parameters` and letting it throw;
`user_properties::parquet_options` holds the validated map; `schema_tables` writes it as a
`map<text,text>` column and reads it back; `make_writer()` builds its `pq_writer_config` from
`parquet_parameters(s.parquet_options()).config()`.

The schema stores the **raw map**, not a `parquet_parameters` object, so `schema.hh` does not
have to include the sstable layer above it -- validation has already happened by then.

Covered by `cql_ddl_test/test_parquet_table_property`, which drives CREATE, ALTER, a schema
reload and four rejections, and was mutation-checked by removing the `store_map` call: the test
fails, so it really covers persistence rather than just the in-memory path. That mattered here,
because persistence is where this property had already broken once (see the round-trip note
below).

Two deliberate departures from the original specification:

- **Only knobs the writer can honour are accepted.** `compression` takes `none` or `zstd`
  and rejects `lz4`, `snappy` and `gzip`, because the writer emits only those two. An
  option that silently ignores what it cannot do is worse than one that refuses -- a user
  who sets `gzip` and gets zstd has been told something untrue about their data. Likewise
  `dictionary_page_max_bytes`, `write_page_index`, `statistics_level`, `bloom_filter_columns`
  and `writer_version` are **not** accepted yet; each would be a lie until implemented.
  (`dictionary` and the per-column encoding overrides *are* accepted -- see §8.2b.)
- **`metadata_folding` cannot select L3.** L3 discards write times and TTLs; it is
  export-only, and `to_parquet_for_storage()` refuses it. Accepting it as a table property
  would offer silent data loss as a configuration option.

**The decision function is three criteria as of 2026-08-18: C1, C5 and C6** — tier, width and
measured gain. C2, C3, C4 and C7 were all removed, and the pattern in why is worth stating: three
of the four were thresholds nobody had measured, and the fourth could not be evaluated at all.
Every surviving criterion is either structural (C1), derived from a measurement (C5) or a
measurement itself (C6). The numbering is left alone so that references elsewhere stay valid.

Guard rails, with reasons rather than round numbers:

| Bound | Value | Why |
|---|---:|---|
| `rows_per_row_group` floor | 1 000 | Below this the fixed ~225 B per leaf per row group dominates: at 100 rows on a 20-leaf table that is 45 B/row against a 5.2 B/row total, so the file grows ~9x (§10.4c) |
| `rows_per_row_group` ceiling | 100 000 000 | Sanity |
| `row_group_buffer_bytes` | 1 MiB - 1 GiB | A guard rail, not a dial: *buffered shredder memory* rather than output, sized to stop a shard OOMing on a pathological partition (R-13). At the 5 000-row default it never binds; see §5.5a |
| `page_rows` | 128 - 1 000 000 | The ceiling has to exceed the row-group ceiling for `page_rows` to be expressible as "one page per group"; the floor is where the curve has gone flat — below ~2 048 rows a smaller page buys no latency at all and only costs size (§10.25) |
| `compression_level` | 1 - 22 | zstd's range |

`row_group_buffer_bytes` accepts a `KiB`/`MiB`/`GiB` suffix as well as a plain count.

**Three knobs, three different jobs — measured and separated in §10.25.** The short version, because
the natural reading of the names gets all three wrong:

| knob | what it actually trades | measured |
|---|---|---|
| `rows_per_row_group` | size against **scan memory**, at ~1.1 kB of reader memory per row of group | 5 000 → 20 000 is −4.3/−5.0 % of the file for **3.6×** the memory (5.6 → 19.9 MB) |
| `page_rows` | size against **latency**, at **zero** memory cost | ~130 ns of cold point read per row of page, five independent estimates 114–131; 4× smaller costs +18/+20 % of the file on real data |
| `row_group_buffer_bytes` | nothing, deliberately — it is a guard rail against OOM | binds on sparse wide shapes (cuts Backblaze at ~6 400 rows) and never at the default on narrow ones |

**`page_rows` is a ceiling, not a page size, and that is why 8 192 exceeds the 5 000-row group.** The
writer emits `min(page_rows, rows in the group)` values per page, so at the defaults the page bound
does not bind and the effective page *is* the row group. That is intentional: the setting exists to
cap decode volume as soon as the group is larger, and the cap is worth **2.9×** (a 20 000-row group
costs 3 641 µs per cold point read with one page per group against 1 240 µs at 8 192). Where it is not
needed it costs nothing — Backblaze's 8 192 and 20 000 outputs are byte-identical. Setting
`page_rows` ≥ `rows_per_row_group` is therefore not an error, it is how you ask for one page per row
group; it is also the worst measured configuration on every axis except size.

**The two are not co-equal knobs, and the doc used to imply they were.** `rows_per_row_group`
is the tuning dial: it is what read cost depends on, it is the one that actually cuts at the
5 000-row default, and §10.21 is a sweep of it. `row_group_buffer_bytes` is a **guard rail**.
It measures buffered shredder memory rather than output -- the two differ by ~343x, 1 887 B/row
held against 5.2 B/row written -- and it exists to stop a shard running out of memory on a
pathological partition (R-13). At the default it never binds: a 5 000-row group is about 9 MB of
shredder buffer against a 64 MiB budget. So it is not a dial an operator reaches for to trade
size against latency; raising it does not buy size, and lowering it does not buy latency, it
just moves where the safety net sits. The one open question about it (§10.1f-rg, open question
15) is whether the *guard* should vary by table shape, not whether it should be tuned for
performance.

**`rows_per_row_group` was called `row_group_rows` until 2026-08-20.** The rename is behavioural
only in the sense that the old name was actively misleading: it reads as though it sets the
number of row groups, and it was read that way. The two move in **opposite** directions -- 8 M
rows at 1 000 rows/group is ~8 000 groups, at 20 000 it is ~400 -- so a reader with the direction
backwards tunes the wrong way and concludes the setting does nothing. `rows_per_row_group` cannot
be read the other way.

**`row_group_rows` remains an accepted alias, permanently.** This map is persisted in schema
(`schema::parquet_options()`, stored by `db/schema_tables.cc`) and `parquet_parameters` is
reconstructed from it on every schema read -- the writer, the reader and the compaction manager
all do it -- so a parser that rejected the old key would stop pre-rename tables from loading and
fail their next DDL, not merely warn. Both spellings parse to the same setting; there is no
removal date. Setting **both** in one map is a configuration error naming both, rather than a
silent last-one-wins, because map order would otherwise decide and the operator who wrote both
almost certainly meant the second to replace the first.

`to_map()` emits the canonical name whichever spelling arrived. Note that this does not rewrite
anyone's stored property, because `to_map()` is not on the persistence path: `cf_prop_defs` stores
the raw map as written and `DESCRIBE` echoes that map verbatim, so a table created or altered
with the old name keeps the old name in its schema and in its `DESCRIBE`. What `to_map()` owes is
that its output re-parses to an equal config.

**A round-trip asymmetry the test caught immediately:** `to_map()` first emitted
`to_string(folding_level)`, which produces the internal `"L0"`/`"L1"`/`"L2"`, while the
parser accepts `"verbatim"`/`"row"`/`"uniform"`. The property therefore did not survive
persistence -- write it, read it back, and validation rejected your own output. Serialisation
now uses the user-facing vocabulary. Worth recording because it is invisible to any test that
only parses, and it would have shipped as "ALTER works but the table breaks on restart".

### 8.2b Per-column encodings

An operator can name the encoding for a single column, as an enum, in the same map:

```cql
ALTER TABLE ks.t WITH parquet = {
    'encoding.reading'  : 'byte_stream_split',
    'encoding.sensor'   : 'dictionary',
    'encoding.path'     : 'delta_byte_array'
};
```

The accepted values are `auto`, `plain`, `dictionary`, `delta_binary_packed`,
`delta_byte_array`, `delta_length_byte_array` and `byte_stream_split` -- the encodings the
writer can actually emit, and nothing else. `auto` means "no override", and it is stored rather
than dropped: an `ALTER` replaces the whole map, so an operator scripting a change needs a way to
name a column and cancel its override, and a `DESCRIBE` that silently omitted the cancellation
would not reproduce the table.

Three things are rejected at DDL time rather than at write time:

- an encoding that is not in the enum;
- an encoding that does not apply to the column's type (`delta_binary_packed` on `text`,
  `byte_stream_split` on anything that is not a `double`);
- a column the table does not have.

The last one matters as much as the others. Left alone a misspelled column name is inert, and an
inert performance setting is one nobody notices is doing nothing. All three are checked in
`cf_prop_defs::apply_to_builder()`, which is the first point where both the option map and the
column definitions are in scope.

**How an override interacts with the automatic choice.** The schema mapping proposes a hint from
structure -- `DELTA_BINARY_PACKED` for `bigint`/`timestamp` keys, `DELTA_BYTE_ARRAY` for `text`
and `blob` clustering keys -- and the writer then decides the dictionary on its own cardinality
and size tests. An override replaces the structural hint. It does not replace the dictionary's
*size* cap, which is not a heuristic: a dictionary page larger than the budget cannot be held.
It does replace the dictionary's repeat-ratio test, because that test is a heuristic about when a
dictionary pays and someone naming `dictionary` has overridden the heuristic deliberately.

**The bug this shipped with, because the shape of it will recur.** An sstable is written by one of
two paths, chosen by size and invisible to the operator: `cut_row_group()` drives the file writer
directly once the sstable outgrows the row-group budget, and `write_rows()` emits it in one shot
when the whole thing fits a single row group. Only the first passed the overrides through. The
symptom was not "the option does nothing" -- it was worse than that. The option worked on large
tables and silently did nothing on small ones, so raising `rows_per_row_group` appeared to *enable*
it, when all that did was force a cut. Two hours went into chasing a non-existent dependency on
the scalar options before the second write path was noticed.

This is the second time hints have been dropped on one path and not the other; §10.1g records the
first, where the nested writer ignored them entirely and every column went out PLAIN. The lesson
is specific enough to state as a rule: **a per-column writer setting has to be asserted on both
write paths, because which one runs is a function of data size.** The regression test in
`test/boost/parquet_writer_test.cc` drives the same rows through both.

**The audit that rule prompted is finished, and it found one more thing.** Every piece of file-level
metadata was checked against both paths: `scylla.folding_level` (both), `scylla.counter_columns` and
`scylla.counter_encoding` (both, via `add_counter_metadata()`), and `scylla.uniform_timestamp` --
which only `write_rows()` wrote. That one turned out to be unreachable rather than broken, for a
reason that is itself worth knowing; see §10.15. The remaining differences between the paths are
deliberate: the cutting path uses the conservative leaf set because it must fix the leaf set before
seeing all the rows, and the export-only entry point skips the losslessness check because L3 is
export-only. The divergence set is closed as of 2026-08-20.

**`DESCRIBE` had never emitted the property at all.** Independently of the above, `parquet = {...}`
was missing from `schema::get_create_statement()`, so no parquet setting -- scalar or per-column --
survived a table recreated from its own description. `to_map()` carried a comment explaining that
overrides are deliberately not elided from `DESCRIBE`, which had been true of `to_map()` and false
of `DESCRIBE` for as long as the property had existed. It is emitted now, and the lab check
recreates a table from its own `DESCRIBE` output and re-verifies the encoding in the file.

### 8.2a Original specification (for reference)

Mirrors the existing `compression = {...}` map property, parsed and validated into a
`parquet_parameters` object analogous to `compression_parameters`:

*Quoted as originally written, so the names below are the proposed ones rather than the
shipped ones: `row_group_rows` is now spelled `rows_per_row_group` (§8.2), and
`row_group_bytes`, `page_bytes` and the `lz4`/`snappy`/`gzip` codecs were never
implemented.*

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
and the seastar-native I/O layer proper. The writer streams its output into the Data
component as of 2026-08-18 (`set_sink()`), so peak write memory is no longer O(output).

**Quantified, 2026-08-18.** `check_image_accumulates()` in `format/test_writer.cc` writes eight
row groups and measures the buffer against the finished file: **45 103 B of 45 701 B, or 99 %**.
The footer is 1 % of the file, so peak write memory is essentially the entire output plus the
shredder budget. Extrapolated to a 256 MB bottom-tier output that is ~253 MB resident plus up to
64 MiB of shredder, per concurrent compaction per shard — which is the one place this
implementation is not yet safe to run at production scale, and it is a memory bound rather than a
correctness bug.

Worth separating clearly: the *input* side is bounded and was made so deliberately (R-13 — 5 000
rows or 64 MiB of buffered rows, whichever trips first, §5.5a). The *output* image is bounded by
nothing. Cutting row groups at 5 000 rows fixed the shredder, not the file.

**Layer 1 fixed, 2026-08-18.** `parquet_file_writer::set_sink()` streams the file out as it is
produced: completed row groups are handed to the sink and dropped. Measured on an 8-row-group,
2-leaf file — buffered peak **98 %** of the image against streaming peak **19 %**, and the two
images are **byte-identical**.

That identity is the assertion that matters, and it is what
`check_streaming_matches_buffered()` exists for. Every offset the writer records is a *file*
position, so a drain that forgot the flushed base would still produce a parseable file pointing
at the wrong bytes. Comparing the streamed bytes against the buffered image catches precisely
that class of error, because only one of the two paths can be wrong in a way the other is not.
The mechanism is a single `pos()` accessor returning `_flushed + _buf.size()`; the five sites
that previously used `_buf.size()` as an offset — first page, dictionary page, page start, data
page, offset index — all go through it, and using `_buf.size()` for an offset is now wrong by
construction rather than by convention.

The 19 % is dominated by the *footer*, not by a row group: the largest single drain is the page
index plus footer at the end. So the saving improves with file size, since the row-group chunk
stays constant while the footer grows only with row-group count. Independently validated by
emitting the nine interop fixtures and reading them back with pyarrow, including each row
group's `data_page_offset` — the structure most likely to be wrong if a position were computed
relative to the buffer.

**Storage path wired the same day.** `pq_writer_impl` gives its `_data_writer` to `set_sink()`
at the first row-group cut, so row groups go into the Data component as they are produced and
`finish()` returns nothing. Two details made this safe rather than delicate:

- **Nothing downstream depends on where the data lands.** The Parquet index is by *row ordinal*
  rather than by data-file offset (§5.4, option A), so `finish_open_partition()` and the index
  bookkeeping can keep running after the bytes are already out. Had the index been offset-based,
  this ordering would not have worked.
- **The non-streaming paths are untouched and still needed.** The unit-test sink wants the
  finished image in one piece, and the no-cut path — where the whole sstable fitted the row-group
  budget — still materialises, which costs at most one row group by construction.

**Verified on a real conversion.** A 300 000-row NOAA ISD-Lite table converted through the
server produced a `pq` sstable of **1 802 231 B — the same byte count as the buffered path
produced before this change**. The native baseline differed between the two runs (3 611 320 vs
3 713 670, dictionary training is not bit-deterministic), which makes the identical Parquet
output the stronger result: the Parquet side is a function of the rows, and streaming did not
perturb it.

What is verified and what is not: correctness end to end, and the memory bound at unit level
(98 % buffered against 19 % streamed, §7.2 above). The node's resident-memory saving during a
large compaction is *not* directly measured — that needs RSS sampling against a multi-gigabyte
conversion, which this host does not have the disk for.

**Why this was not a small change.** `parquet_file_writer` computes every offset it
records — page locations, column-chunk starts, the OffsetIndex and the footer's own pointers —
from `_buf.size()`, i.e. from the position within a buffer that holds the file from byte zero.
Draining completed row groups to a sink means those offsets have to come from
`bytes_already_flushed + _buf.size()` instead, everywhere, and an offset that keeps using the
buffer-relative value will produce a file that parses and points at the wrong bytes — the failure
mode is silent corruption, not a crash. That is why the test above pins the current behaviour: it
is the assertion that has to be consciously updated when the drain lands, rather than a comment
that can be read past.

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

**Delivered since, 2026-08-17:** all of what this paragraph used to list as outstanding.
Component/TOC plumbing, the `pq` version enum, `pq::make_reader` and the row-ordinal index
(§5.4) are in; the shredder handles static rows, row markers, partition, row and range
tombstones, non-frozen collections and counters; and the mutation-source property suite runs
against `pq` and passes all 34 sub-tests
(`test_sstable_conforms_to_mutation_source_pq_small`).

**Not done:** statistics and metadata parity — the pq writer barely feeds its
`metadata_collector`, which is what still keeps `pq` out of `all_sstable_versions` and is
correctness rather than reporting, because tombstone GC reads that metadata. See §11 item 11
and item 12.

- `sstable_version_types::pq`; `writer_impl` subclass; `pq::make_reader`.
- Component layout (§5.2); index components pointing at `(row_group, row_index)`.
- Metadata folding levels 0–2.
- Table-level `storage_format`, single format per table; no hybrid tiering yet.
- Round-trip property tests green; shadow-read verification available.

### Phase 3 — Format control and convergence
- `storage_format` schema property, validated and plumbed through the schema tables
  (no new system table, no group0 work — see §6.1). **Done.**
- ~~`rewrite_to_format` compaction~~ — **not needed**: `nodetool upgradesstables` already forces
  convergence, because the sstable creator is shared by the rewrite paths (§6.2a). A separate
  compaction type would duplicate it.
- **Per-table convergence tracking — done 2026-08-19.**
  `GET /column_family/storage_format/{ks}:{cf}` reports `native_bytes`, `parquet_bytes`,
  `native_sstables`, `parquet_sstables`, `parquet_fraction` and a `converged` verdict of
  `native` / `mixed` / `parquet`. Verified through all three states on a live node: all-native
  before the `ALTER`, `parquet` with fraction 1.0000 after a major compaction, and `mixed` at
  0.6285 once fresh writes flushed — because flushes stay native by design, so **`mixed` is the
  steady state of any table still being written to**, not a transient.

  Byte columns use `ondisk_data_size()`, not `data_size()`. The latter reports the *uncompressed*
  length for a compressed native sstable and the file size for a `pq` one, so summing it would
  compare two units in precisely the table where both formats coexist — the same trap that broke
  C1 (§6.2a).
- Cluster feature `PARQUET_SSTABLE_FORMAT`.
- Streaming, migration, split and merge must keep working across a mixed-format
  table; cluster-feature gating and tests.

### Phase 4 — Hybrid tiering  <span>· **mostly delivered 2026-08-16**</span>
**Done:** the policy engine. `evaluate_tiering()` in
`sstables/parquet/tiering_policy.{hh,cc}` implements the criteria as a pure function
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

(The set is now C1, C5 and C6; C2, C3, C4 and C7 were removed — see §6.3 and
`docs/dev/parquet-future-work.md`.)

**Done since:** `decide_output_format()` is wired in. `compaction_manager` evaluates the
policy before installing the sstable creator, so the verdict actually selects the writer,
ICS supplies C1 through the size-tier rule, and TWCS bypasses the criteria
altogether — a TWCS table is entirely Parquet (§6.3). Three things fell out of doing that:

- **Bucketing is unit-consistent in hybrid tables.** Both size-tiered bucketing paths compared
  `data_size()`, which means uncompressed bytes for a native sstable and file bytes for a `pq`
  one, so a converted file bucketed several tiers too low and was rewritten repeatedly. Mixed
  candidate sets now compare `ondisk_data_size()`; all-native sets are untouched (§10.3i).
- **An unsupported strategy says so.** Writing Parquet under anything but ICS or TWCS logs a
  rate-limited warning naming the strategy. It does not refuse — the output is valid — but an
  operator on STCS or LCS should not have to infer from this document that they are outside what
  was tested.
- **The write paths share one rule.** `writes_parquet_unconditionally()` is consulted by
  compaction, memtable flush and streaming. They cannot drift apart, which is the failure that
  would otherwise be invisible: a table whose flushes and compactions disagree never converges.

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

**Both sides of the dictionary question must be measured**, and only one has been so far:

- *Baseline side (done).* SSTables compressed with a trained shared dictionary — the
  §3.2 bar. See Trap 4 for why this has to be verified rather than assumed.
- *Parquet side (**not yet measured**).* §5.6 proposes priming each page's zstd frame
  with the same trained dictionary. This is the symmetric experiment and it is the one
  configuration that competes hardest with `ZstdWithDictsCompressor`, because it gives
  Parquet the same cross-chunk redundancy the baseline already has. Open question 7 asks
  whether the gain justifies forfeiting external readability — that cannot be answered
  until it is measured. Required numbers: plain zstd-3 vs. zstd-3-with-dict, per dataset,
  at the same folding level, with the dictionary trained on that table's own data.

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

**Trap 4 — The untrained-dictionary baseline (overstates, and it has already happened
twice).**

§3.2 says every size claim is measured against `ZstdWithDictsCompressor`. Setting that
property does **not** make a table's SSTables dictionary-compressed. The dictionary has to
be trained and the files rewritten; until then the data is plain-Zstd and the baseline is
roughly **2× too large**, which flatters Parquet by the same factor.

This is not hypothetical. It has bitten twice in one day:

| Incident | Reported | Actual | Cause |
|---|---:|---:|---|
| §10.3h first run, ClickBench | 25.1 % | 47.9 % | table created with the dict compressor, dictionary never trained |
| §10.3i first working estimator | 6.7 % | 44.2 % | `data_size()` is the *uncompressed* size; the SSTable side was measured before compression |

**Required protocol for any size comparison.** Every one of these, every time:

1. `POST /storage_service/retrain_dict?keyspace=&cf=`
2. `GET /storage_service/keyspace_upgrade_sstables/<ks>?cf=&exclude_current_version=false`
3. Major-compact, then **verify** the baseline moved — if the byte count is unchanged after
   step 2, the dictionary did not apply and the number is not a dict baseline.
4. Measure the SSTable with `ondisk_data_size()` or `ls`, never `data_size()`.
5. Sanity-check the result against an existing measurement in §10 before believing it.

`harness.py` does 1–3. The ad-hoc loader used for §10.3h did not, which is how the first
error got in. **Any new measurement path must do the same or explicitly state that its
baseline is plain Zstd.**

*Follow-up (not yet built):* `estimate_parquet_ratios` should report whether the sampled
SSTable is actually dictionary-compressed, so the endpoint cannot silently return a
plain-Zstd comparison. It has the SSTable in hand and can check its compression options.

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

**These are what the *format* permits, and the reader claims none of it (§10.26, 2026-08-20).**
Projection is not pushed down on any path — single-partition read, token-range scan or
coordinator-side aggregate — and that is now measured at the level of bytes read, not inferred:
`count(*)`, `count(temp)` and a three-column aggregate on the same 28-leaf table all read 497 632
extents and 460 MB, identical to 0.005 %. So this table is a target, not a result. It also
understates what the target is worth on Scylla's shredded schemas: the reduction there is mostly in
read *operations* and per-leaf decode setup rather than bytes, because the two dozen `__ttl_*`,
`__ldt_*` and tombstone leaves are all-null, RLE to ~45 kB each, and still cost a read apiece per
window.

### 10.1d Estimator accuracy (validates C6)

*Not yet measured — the estimator is not built. Deferred with the rest of C6.*

### 10.1e Dictionary on the Parquet side — measured 2026-08-17

The baseline uses a trained shared dictionary (§3.2); Parquet did not. That made every
ratio in §10 a comparison of dictionary-compressed SSTables against *non*-dictionary
Parquet — conservative, but it left open how much Parquet was giving away.

**Answer: nothing worth having.** A trained zstd dictionary buys Parquet between −1.4 %
and +7.1 %, and on data the dictionary has not seen it is consistently *negative*.

Measured by `sstables/parquet/format/bench_dict.cc`. A dictionary acts at the codec
layer, below encoding, so the tool does not decode a single value: it walks the pages of
a real Parquet file, decompresses each page body and recompresses it two ways. That is
what makes it runnable over the 105- and 197-column datasets whose logical types our
value decoders do not fully cover. Validity check: recompressing every page at zstd-3
with no dictionary reproduces each file's real on-disk size to within 0.4 %, so the tool
is measuring the same thing the writer produced.

| Dataset | Pages | zstd-3 | + dict (self-trained) | Gain | + dict (held out) | Gain |
|---|---:|---:|---:|---:|---:|---:|
| ClickBench | 1 610 | 15 972 539 | 14 843 578 | **+7.07 %** | 4 727 107 | **−1.41 %** |
| GitHub Archive | 287 | 23 998 275 | 23 263 216 | **+3.06 %** | 15 910 910 | +0.41 % |
| HackerNews | 465 | 21 183 259 | 20 539 807 | **+3.04 %** | 8 704 304 | +0.80 % |
| Backblaze | 3 191 | 16 827 649 | 16 655 506 | **+1.02 %** | 8 930 673 | **−0.25 %** |
| Wikipedia pageviews | 94 | 2 380 194 | 2 296 371 | **+3.52 %** | 1 002 337 | −0.10 % |
| NYC TLC | 623 | 8 593 353 | 8 611 399 | **−0.21 %** | 3 508 449 | **−1.18 %** |

Six datasets, including the three most text-heavy schemas in the roster — free-text titles
and URLs, and JSON event payloads — which are where a dictionary should shine. The best
self-trained result is +7.1 % and the held-out results cluster around zero.

Self-trained means the same bytes train the dictionary and are then compressed with it —
optimistic, and also exactly what Scylla's `sstable_dict_autotrainer` does for a table's
own SSTables, so it is the like-for-like column. Held out trains on the first half of the
pages and measures the second, which is the honest answer for data the dictionary has not
seen. Dictionary size 112 640 B, matching the ~110 KB dictionaries the baseline carries.

**Why, and the control that shows it.** A dictionary pays when you compress many small
*independent* blocks, because each one otherwise starts with a cold window. Scylla's
SSTable chunks are 4–16 KB and row-oriented, so every chunk re-encounters the whole
schema's worth of structure and a dictionary supplies it for free. Parquet pages are far
larger and hold a single column, so zstd's own window has already captured that
redundancy by the time a dictionary could contribute.

To check that this is the mechanism rather than a property of these particular files, the
same page bytes were re-cut into 4 KB blocks and compressed independently:

| Dataset | Page-granular zstd-3 | 4 KB blocks, zstd-3 | 4 KB blocks + dict | Dict gain at 4 KB |
|---|---:|---:|---:|---:|
| GitHub Archive | 23 998 275 | 54 935 431 | 32 988 375 | **+39.95 %** |
| ClickBench | 15 972 539 | 23 112 277 | 16 934 197 | **+26.73 %** |
| HackerNews | 21 183 259 | 24 625 784 | 22 150 389 | **+10.05 %** |
| Backblaze | 16 827 649 | 17 050 136 | 17 108 241 | −0.34 % |
| NYC TLC | 8 593 353 | 8 845 544 | 9 616 586 | −8.72 % |

This is the clearest statement of the mechanism. GitHub Archive is the extreme case: its
JSON payloads carry enormous cross-row structure, so cutting the data into 4 KB blocks
costs 2.3× in size and a dictionary then wins back 40 % of it — while at Parquet page
granularity the same dictionary is worth 3 %. The dictionary is not finding something the
page-granular encoder missed; it is repairing damage that small blocks caused.

The two numeric datasets do not benefit even at 4 KB, so block size is *part* of the story
and not all of it: where a column's values are individually incompressible, neither
layout has redundancy left to exploit. Reported as measured rather than tidied.

**Consequences.**

1. **§10's ratios are fair, not conservative.** Parquet is not being handicapped by the
   absence of a dictionary, so the ratios stand as like-for-like. (The specific percentages
   quoted when this was written have since been superseded by the single-binary
   re-measurement in §10.1f; the like-for-like argument is unaffected.)
2. **Open question 7 is resolved: no.** A dictionary inside Parquet would cost external
   readability — no other implementation could open the file without being handed the
   dictionary out of band — in exchange for roughly zero, and for a loss on unseen data.
   The format keeps `scylla.folding_level` as its only private metadata and stays
   openable by pyarrow, which `test/boost/sstable_parquet_test.cc` asserts.

### 10.3i The MAP annotation made every collection and counter file unreadable

Verifying the typed counter leaves against pyarrow — the whole point of typing them — produced this
instead:

```
ArrowInvalid: Key-value map node must have 1 or 2 child elements. Found: 6
```

The file would not open **at all**. And a plain `map<text,text>` column fails the same way with
`Found: 5`, so this was never about counters: **any `pq` sstable containing a non-frozen collection
or a counter was unreadable by any parquet-cpp-based reader**, which includes pyarrow, DuckDB and
Arrow. That is the exact opposite of the format's premise — §1 says the value proposition is a file
other tools can open — and it had been true since collections were implemented.

**Cause.** The group was annotated `ConvertedType::MAP`, but a Parquet MAP's `key_value` child must
have one or two children. Ours has five — `key`, `value`, `__ts`, `__ttl`, `__ldt` — because each
element carries its own cell metadata, which is what makes the representation lossless (§5.2).
parquet-cpp enforces the arity and rejects the file rather than ignoring the annotation.

**Fix: drop the annotation.** Without it the group is an ordinary nested structure — an optional
group holding a repeated group of typed fields — which is valid Parquet. Readers resolve it as a
list of structs:

```
hits: struct<key_value: list<struct<key: binary, value: int64, __ts: int64,
                                    __ttl: int32, __ldt: int32, clock: int64>>>
tags: struct<key_value: list<struct<key: binary, value: binary, __ts: int64,
                                    __ttl: int32, __ldt: int32>>>
```

pyarrow now opens both and decodes the values — counter shards read out as `value=8 clock=2` and so
on. The Scylla side is unaffected: our reader works from the tree and the Dremel levels, not from
the annotation. All five losslessness modes, `parquet_writer_test` and `sstable_parquet_test` pass
unchanged.

**Closed the gap, not just the bug: every shape is now handed across.**
`~/pq-lab/interop_shapes.py` builds one real table per schema shape the format can emit, converts
it, and opens every resulting `pq` sstable with pyarrow — reading all row groups, since a footer can
parse while its pages cannot decode, and reading *every* sstable a major compaction produced rather
than the first:

| Shape | pyarrow | DuckDB | rows | schema |
|---|---|---|---:|---|
| flat scalars (8 types) | OK | OK | 120 | 11 leaves, 5 files |
| `map<text,text>` non-frozen | OK | OK | 120 | 11 leaves, 5 files |
| `set<int>` non-frozen | OK | OK | 120 | 11 leaves, 5 files |
| `list<text>` non-frozen | OK | OK | 120 | 11 leaves, 5 files |
| `frozen<map>` | OK | OK | 120 | 5 leaves, 5 files |
| static column | OK | OK | 120 | 8 leaves, 5 files |
| expiring cells (TTL) | OK | OK | 120 | 9 leaves, 5 files |
| counter | OK | OK | 120 | 11 leaves, 5 files |
| collection at L0 `verbatim` | OK | OK | 120 | 9 leaves, 5 files |
| collection at L1 `row` | OK | OK | 120 | 11 leaves, 5 files |
| collection at L2 `uniform` | OK | OK | 120 | 11 leaves — fell back to L1 |
| flat at L0 `verbatim` | OK | OK | 120 | **37 leaves**, 5 files |
| flat at L1 `row` | OK | OK | 120 | 11 leaves, 5 files |
| flat at L2, UPDATE-written | OK | OK | 120 | **4 leaves** — L2 genuinely applied |

**11/11, and against two implementations rather than one.** DuckDB has its own Parquet reader rather
than wrapping parquet-cpp, which matters because pyarrow *is* parquet-cpp: the MAP arity check that
rejected every collection file is parquet-cpp's, so a suite built only on pyarrow tests one
implementation's opinion. Both now agree.

**L2 needed four attempts to trigger, and the reason is worth knowing.** Three fixtures all reported
11 leaves — identical to L1 — meaning L2 had silently fallen back every time. The precondition in
`map_schema()` is `all_same_ts && !any_ttl && !any_deletion && !any_marker && ...`, and **a CQL
`INSERT` always writes a row marker**. So no `INSERT`-populated table can ever reach L2, whatever
its timestamps. A collection cannot either, for a different reason: each element carries its own
`__ts` *inside* the repeated group, so there is no row-level leaf to fold and the count cannot drop.

Written with `UPDATE ... USING TIMESTAMP`, which writes no marker, L2 applies and the layout is
plainly different — **4 leaves against L1's 11** — and both readers handle it.

**The practical consequence is a scope note on L2, not a bug.** It is reachable only for
UPDATE-written data with a single write timestamp, no TTLs and no deletions. That is a real pattern
— bulk-loaded or backfilled tables often look exactly like it — but it is much narrower than "the
uniform case".

**Two provenance corrections follow, and both were worth chasing.**

*The export tool was mislabelling.* `parquet-export` printed `_cfg.level`, the folding level
*requested*, so a run that fell back reported `L2` while producing L1. It now prints
`folding_requested` and `folding_effective` separately, which makes the fallback visible:
requesting `uniform` on a harness-written ISD table gives `requested=L2 effective=L1`, identical
bytes to `row`, with `__ts` and `__rm` still in the leaves. Every L2 measurement ever taken with
that tool was labelled wrongly.

*The L2 savings figures in §10.1f are not our writer's.* They are the harness's own folding,
implemented in Python (`write_variant`): "L2" there simply omits the `__ts` column from what it
hands pyarrow. That is why they differ from L1 at all — a fallen-back writer would have produced
identical bytes. So those figures measure **what L2 would save if it applied**, which is a fair
answer to "is folding worth it" and *not* an answer to "what does this implementation produce".
For INSERT-written data this implementation produces L1, and the saving is zero. Both statements
are true and the distinction is the whole point.

**One more reading of the table.** `verbatim` has *fewer* leaves than `row` on a collection (9 against
11) but far more on a flat schema (37 against 11). Both are right: on a flat schema L0 keeps
per-cell metadata for every column, which is where the 37 comes from; on a collection the elements
already carry their own metadata, so L0 adds nothing while L1 adds a folded `__ts` and two sparse
exception leaves. All three non-frozen collection kinds go through the group shape that was broken, so this
gate would have caught the MAP bug on the day collections landed. It is cheap enough to run on
demand and belongs in whatever CI this eventually gets.

**Why the interop suite missed it.** The seven cross-read fixtures in §10.3 are flat schemas, and
the Dremel work in §10.3 was validated by writing a `list<string>` for pyarrow — a *proper* LIST,
not this five-child pseudo-MAP. Nothing in the suite ever wrote a Scylla collection column and
handed it to another implementation. **The lesson is narrow and worth stating: an interop claim is
only as broad as the shapes actually handed across.** Two years of "pyarrow reads our files" was
true and irrelevant for the one shape where it mattered most.

### 10.1m L2 is self-defeating: measured at 1.4 %, not 35 %

L2's saving through this writer had never been measured, because the writer falls back to L1 for
INSERT-written data and every existing figure came from the harness's Python folding (§10.3i). The
three-column ISD shape, written by `UPDATE ... USING TIMESTAMP` so L2 genuinely applies — confirmed
by `folding_effective: L2` rather than assumed:

| Folding | Effective | Bytes | Leaves |
|---|---|---:|---:|
| L0 `verbatim` | L0 | 10 358 | 7 |
| L1 `row` | L1 | 9 856 | 4 |
| L2 `uniform` | **L2** | **9 717** | 3 |

**L2 saves 1.4 %.** The doc has carried a 35-point figure for the same idea, and the gap is not a
measurement error in either — it is a structural fact nobody had noticed:

**The precondition that makes L2 legal is the condition under which the column it removes is
already free.** L2 applies only when every cell shares one write timestamp. A `__ts` column whose
values are all identical is the best case a dictionary or RLE encoder will ever see: in the earlier
export of a harness ISD table it compressed to **294 bytes from 240 111 uncompressed**, 0.8 % of
that file. Dropping a column that costs 0.1 % cannot save 35 %.

The harness's 35 points came from removing a `__ts` column whose values *varied* — its timestamp
regime is a hash per partition key — which is a column that genuinely costs something. But varied
timestamps are exactly what makes L2 illegal. **The two conditions cannot hold at once**, so the
35-point saving describes a file this format cannot produce.

**Consequence for the design.** L2 is close to worthless as a size lever and should not be
presented as one. It remains defensible as a *statement* — a file whose timestamps are uniform
needs no timestamp column, and saying so in the schema is cleaner than storing 300 000 copies of
one number — but the disk argument for metadata folding rests entirely on **L1**, which is the
default and which does carry its weight. §10.1f's folding table should be read as "what L1 buys
over L0", and its L2 column as an upper bound on a case that cannot arise.

### 10.1f-raw Against uncompressed: how much is layout and how much is the compressor

Every ratio in this document compares Parquet against a **trained-dictionary** SSTable, which is
the right adversary but a hard one. Adding the uncompressed baseline — `sstable_compression: ''`,
no LZ4, no dictionary — gives the raw serialised volume of the mutation model and separates two
questions that the single ratio conflates: how much smaller Parquet is than *storing the data at
all*, and how much of that Scylla's own compressor already achieves.

| Dataset | Raw (uncompressed) | Native (Zstd+dicts) | Parquet (`pq`) | `pq` / raw | native / raw | `pq` / native |
|---|---:|---:|---:|---:|---:|---:|
| ISD-Lite, 3 cols | 6 845 109 | 1 428 318 | 483 767 | **7.1 %** | 20.9 % | 33.9 % |
| ClickBench | 178 778 924 | 27 196 714 | 16 271 864 | **9.1 %** | 15.2 % | 59.8 % |
| NYC TLC | 33 163 066 | 6 316 348 | 3 589 983 | 10.8 % | 19.0 % | 56.8 % |
| GitHub Archive | 197 382 026 | 34 139 871 | 23 306 561 | 11.8 % | 17.3 % | 68.3 % |
| ISD-Lite | 14 801 542 | 3 705 650 | 1 888 666 | 12.8 % | 25.0 % | 51.0 % |
| Wikipedia pageviews | 10 184 539 | 2 568 339 | 2 300 769 | 22.6 % | 25.2 % | 89.6 % |
| HackerNews | 47 449 722 | 23 112 165 | 19 028 904 | 40.1 % | 48.7 % | 82.3 % |

**Parquet stores the corpus in 7–40 % of its raw volume**, i.e. it saves 60–93 % against writing
the data uncompressed. That is the number to quote to someone who has not enabled compression, and
it is a much larger number than any ratio elsewhere in this document.

**But the interesting column is `native / raw`.** Scylla's trained dictionary already reaches
15–49 % of raw on its own. So on ClickBench the honest decomposition is: the compressor does
178.8 MB → 27.2 MB, and the columnar layout does 27.2 MB → 16.3 MB. **Most of the total win is
compression that Scylla already has**; Parquet's marginal contribution is real but it is the
smaller half everywhere except the narrow numeric tables.

**The two ends of the corpus say it most clearly.** On the three-column time series the layout
takes 20.9 % down to 7.1 % — it nearly triples what the compressor achieved, because delta-encoded
timestamps and a dictionary-coded station key are things a block compressor cannot see (§10.1g).
On HackerNews it takes 48.7 % to 40.1 %, barely moving, because near-unique text has no structure
for either mechanism to exploit and the two are competing for the same redundancy.

**Method.** `sstable_compression: ''` disables compression entirely; the harness measures it first
in its codec sweep so it is the reference point rather than an afterthought. Uncompressed figures
are the `-Data.db` size of the same table, same rows, rewritten with compression off, read through
`live_table_dir.py` like every other figure here.

**Backblaze is excluded from the table above** because its uncompressed figure came from a batch
run that also produced an anomalous `pq` size. Its raw volume, 242 357 583, is plausible on its
face — 807 B/row across 197 columns — but the same run reported `pq` 84 538 396 where every
controlled run gives 20 803 872. A subsequent `rows_per_row_group` sweep on one loaded table lands on
20 803 872 at the default and varies only 18.0–26.9 MB across settings, so no configuration
produces 84.5 MB. The anomaly has appeared only inside full-corpus batches and never in isolation;
it is recorded in parquet-future-work.md and no figure here depends on it.

### 10.1f-prod The corpus as ScyllaDB actually writes it (2026-08-18)

**This is the table to quote.** No export tool anywhere in the path
(`~/pq-lab/measure_native_vs_pq.sh`): load over CQL, train the dictionary, upgrade,
major-compact, read the native `-Data.db`; then `ALTER TABLE ... WITH storage_format =
'parquet'`, major-compact again so the creator converts (§6.2a), assert every remaining
sstable is version `pq`, and sum them. Production output against production output. Each
dataset converted to exactly **one** `pq` sstable.

| Dataset | Rows | Cols | Native (me, Zstd+dicts) | Parquet (`pq`) | Ratio | Saved |
|---|---:|---:|---:|---:|---:|---:|
| **D13 NOAA ISD-Lite, 3 cols** | 300 000 | 3 | 1 431 489 | 483 767 | **33.8 %** | **66.2 %** |
| D12 NOAA ISD-Lite | 300 000 | 10 | 3 706 835 | 1 888 584 | **50.9 %** | 49.1 % |
| D5 NYC TLC | 200 000 | 20 | 6 314 576 | 3 589 983 | **56.9 %** | 43.1 % |
| D1 ClickBench | 200 000 | 105 | 27 113 847 | 16 271 864 | **60.0 %** | 40.0 % |
| D9 GitHub Archive | 180 386 | 7 | 34 217 880 | 23 306 561 | **68.1 %** | 31.9 % |
| D10 HackerNews | 300 000 | 7 | 23 069 409 | 19 029 182 | **82.5 %** | 17.5 % |
| D11 Wikipedia pageviews | 163 845 | 5 | 2 567 684 | 2 300 769 | **89.6 %** | 10.4 % |
| D2 Backblaze | 300 000 | 197 | 21 709 715 | 20 803 872 | **95.8 %** | 4.2 % |

**D13, added 2026-08-18, is the corpus's best result and settles the width question.** It is D12
cut to `station, ts, temp` — one series key, one regular clustering timestamp, one measure. It
saves **66.2 %**, against 49.1 % for the same data with eight more measures and 40.0 % for a
105-column analytics table. A table that is nearly all *key* compresses better than one carrying a
payload, because both of its keys are ideal cases: `station` is low-cardinality and `ts` is
perfectly regular (§10.1g). **Width is not the variable; value repetition is** — and the extreme
of that is a table with almost nothing but repeating keys.

**Measurement integrity.** Every row above is read from the directory named by the table's own id
in `system_schema.tables` (`~/pq-lab/live_table_dir.py`), after waiting for the sstable set to
stop changing and asserting exactly one sstable. That is not incidental: three successive ways of
locating the directory were wrong, and the second was worse than the first. Selecting by newest
mtime is actively misleading, because *deleting files from a directory updates that directory's
mtime*, so a table being dropped acquires a fresh timestamp and outranks the live one. Under that
selector Backblaze read 263.9 % and 95.4 % on consecutive runs and was briefly withdrawn from the
corpus; under the id lookup it reads 20 803 872 bytes byte-identically on repeated runs, with lz4
byte-identical too. **Both a figure that repeats and a figure that varies are uninterpretable
until the selector is known to be right** — repetition was, in this instance, evidence of reading
the same dead directory every time.

**Every export figure was optimistic, and unevenly so.** Against the single-row-group export
numbers below, production is worse by 1.2 points on NYC TLC and **18.6 points on Backblaze**:

| Dataset | export (1 row group) | production (5 000 rows/group) | gap |
|---|---:|---:|---:|
| NYC TLC | 56.1 % | 56.8 % | +0.7 |
| GitHub Archive | 67.1 % | 68.2 % | +1.1 |
| HackerNews | 80.7 % | 82.5 % | +1.8 |
| NOAA ISD-Lite | 46.9 % | 50.9 % | +4.0 |
| Wikipedia pageviews | 85.0 % | 90.0 % | +5.0 |
| ClickBench | 50.1 % | 60.2 % | +10.1 |
| Backblaze | 77.3 % | 95.9 % | +18.6 |

**The gap scales with leaf count, and that is a finding about the new row-group default, not
about Parquet.** Every row group writes a column chunk header and statistics per leaf. At 5 000
rows per group a 300 000-row table has 60 row groups, so a 199-leaf table pays that fixed cost
11 940 times against 420 for a 7-leaf table. The +7.4 % measured in §10.4c was on a 6-leaf
table; on Backblaze the same default costs 18.6 points. **A single row-group default cannot be
right for both** — the honest reading is that `rows_per_row_group` should scale inversely with leaf
count, and until it does, wide tables should be given a larger value explicitly via the
`parquet` property. Recorded as §11 open item.

**The headline claim has to be narrowed.** "Half the disk" is true for the numeric and
low-cardinality end — ISD-Lite 49 %, NYC TLC 43 %, ClickBench 40 % — and false at the other,
where Backblaze saves **4 %** and pageviews 10 %. The range is **0.51× to 0.96×**, and the
mechanism is unchanged from what §10.1f argued: value repetition decides it. What changes is
that sparse wide telemetry against a *trained dictionary* is now measured as very nearly a
wash, where the export figures made it look like a 23 % win.

### 10.1f-c2 Where Parquet stops paying — C2's floor was 1000x too high

C2 declined every real compaction output (§6.2a), so its 256 MiB floor was checked against
measurement. NOAA ISD-Lite loaded at four row counts, each converted through the server:

| Rows | Native | `pq` | Ratio |
|---:|---:|---:|---:|
| 5 000 | 34 588 | 38 647 | **111.7 %** — Parquet is bigger |
| 20 000 | 237 811 | 146 332 | 61.5 % |
| 60 000 | 726 872 | 381 534 | 52.5 % |
| 300 000 | 3 709 848 | 1 887 530 | 50.9 % |

**The crossover is between 35 kB and 238 kB of output**, and 5 000 rows — the point where
Parquet loses — is exactly *one* row group at the current default. So the original
justification, "at least four row groups", was the right instinct with stale arithmetic: it was
written when the 64 MiB byte budget cut row groups, making four of them 256 MiB. Row groups are
now cut at 5 000 rows, so four of them is about 200 kB on this shape. The floor is now
**256 KiB**.

**The shape-independent form is a row floor, not a byte floor**, and that is the remaining gap.
At 6.3 B/row four row groups is 126 kB; at Backblaze's 69 B/row it is 1.4 MB, so a single byte
threshold admits under one row group on a wide table while correctly excluding it on a narrow
one. The right criterion is `rows >= 4 x rows_per_row_group`, which needs a row count in
`tiering_inputs` that the sstable stats can supply. Recorded as an open item.

### 10.1f-rg Does `rows_per_row_group` need to scale with leaf count? Partly answered

Open question 15 proposes scaling the row-group row count inversely with leaf count. The size
half is answerable without a code change, because `rows_per_row_group` is already a per-table
property: load once, then for each candidate value convert native -> `pq` and measure what the
server writes (`~/pq-lab/sweep_rg_by_width.sh`).

**Narrow table (NOAA ISD-Lite, 20 leaves) — clean, and independently corroborated.** The
5 000-row point reproduces §10.1f-prod's 50.9 % exactly, from a separate run and a separate
script, which is the cross-check that makes the rest of the column trustworthy:

| `rows_per_row_group` | pq bytes | Ratio |
|---:|---:|---:|
| 5 000 (default) | 1 887 314 | 50.9 % |
| 20 000 | 1 809 544 | 48.8 % |
| 50 000 | 1 803 905 | 48.7 % |
| 200 000 | 1 803 905 | 48.7 % |

So on a narrow table the whole size penalty of the small row group is **2.2 points**, and 20 000
recovers 2.1 of them. It saturates by 50 000 — identical bytes at 50 000 and 200 000, because
300 000 rows in one row group is the same file either way.

**Wide table (Backblaze, 199 leaves) — the arm is invalid and no conclusion is drawn from it.**
It reported 264 % at three different settings with **byte-identical** output (85 138 044 three
times), which is the tell: no rewrite happened, so the same files were measured three times. The
cause was mine again — a major compaction leaves superseded sstables on disk until they are
reclaimed, and summing the directory then counts the same data twice over. Both measurement
scripts now wait for the sstable set to stop changing and require exactly one sstable before
recording anything; §10.1f-prod already recorded that count as 1 for every dataset in the corpus, which is
why it was unaffected.

**Answered 2026-08-18, and the first version of this answer was wrong.** It said the row knob has
no effect on a wide table, generalising from one synthetic schema. The guarded sweep on the real
Backblaze table then showed it does:

| `rows_per_row_group` | pq bytes | Ratio |
|---:|---:|---:|
| 5 000 (default) | 20 803 872 | 95.0 % |
| 20 000 | 19 908 060 | 91.0 % |
| 50 000 | 19 908 060 | 91.0 % |
| 200 000 | 19 908 060 | 91.0 % |

**The correct statement is that the effective row-group size is
`min(rows_per_row_group, whatever the byte budget allows)`, and which term binds depends on how much
shredder memory a row costs — which is a property of row *density*, not of column count.** Two wide
tables land on opposite sides of it:

- **Backblaze**, 197 columns but overwhelmingly empty, so a row is cheap in shredder memory. The
  byte budget allows somewhere between 5 000 and 20 000 rows, so 5 000 binds and raising it changes
  the file — until the budget takes over, which is why 20 000, 50 000 and 200 000 are identical.
- **The perf-test wide schema**, 195 *populated* int columns, so a row is expensive. The budget
  allows fewer than 5 000 rows, so the row count never binds and 5 000 and 50 000 produce
  byte-identical output (14 743 560 both times).

That decomposes Backblaze's export-to-production gap. Against the guarded native baseline of
21 887 401: one row group would be ~78 %, the byte budget alone takes it to **91 %**, and the
5 000-row default takes it the rest of the way to **95 %**. So roughly **a quarter of the penalty is
the row default and three quarters is the byte budget** — and the byte budget is a memory-safety
knob (R-13), not a tuning dial.

**So open question 15 is reframed rather than withdrawn.** Scaling `rows_per_row_group` by leaf count
would buy about 4 points on a sparse wide table and nothing at all on a dense one, because on the
dense one it is not the binding constraint. The question worth pursuing is whether
`row_group_buffer_bytes` should vary by shape, which is harder: it trades against OOM rather than
against latency, and the same 64 MiB means very different row counts across the corpus.

The narrow-table result stands and settles the default. Measured at 3 000 random point reads:

| `rows_per_row_group` | point mean | scan memory | pq bytes |
|---:|---:|---:|---:|
| 5 000 (default) | 1 258 us | 5 548 kB | 1 269 816 |
| 20 000 | 1 784 us | 19 900 kB | 1 235 425 |
| 50 000 | 2 233 us | 20 676 kB | 1 230 121 |

Going to 20 000 costs **42 % of point-read latency and 3.6x the scan memory to save 2.7 % of
size**. That is a bad trade in the direction this format needs, so 5 000 stays.

**A far more consequential result came out of the wide run.** On the 199-leaf schema, `pq` point
reads cost **22-32 ms against the native format's 0.15-0.27 ms — 85x to 120x**, and scan memory is
61.5 MB against 256 kB. (The two wide runs disagree on absolute latency, 31.9 ms and 22.3 ms, and
the *native* numbers moved by the same factor, so the machine was differently loaded; only the
byte equality is deterministic and only the order of magnitude of the ratio should be read.) This
is much worse than the 38x measured on the narrow schema, and the mechanism is obvious once
stated: a point read has to locate and decode a page in **every** column chunk it projects, so the
cost scales with width, while the row format reads one contiguous row. **The practical conclusion
is a scope limit, not a tuning problem: wide Parquet tables must not be on a point-read path.**
C5's column ceiling is what enforces it (§6.3). The read-pattern criterion that would have expressed
it directly, C7, was dropped rather than built: the counters it needed now exist, and it still could
not be shown to decide better than the ceiling.

**Superseded framing below.** What the narrow arm shows is that the size cost of a small
row group is small when leaves are few, which is consistent with the leaf-count hypothesis but
does not establish the exponent. The wide arm has to be re-run under the new guard, and the
latency side is still unmeasured at any width — `sstable_parquet_perf_test` now takes
`PQ_PERF_EXTRA_COLS` so that it can be, since a 5-column table cannot tell you what the default
costs a 197-column one. **The default stays at 5 000 until both arms exist.**

### 10.1f The same corpus through the export tool — superseded for absolute sizes

#### Method and provenance of the export measurement

Every figure in this table comes from one binary, `build/dev/scylla`, in one batch run
(`~/pq-lab/remeasure_all.sh`, log in `~/pq-lab/out/remeasure.log`):

- the **SSTable** column is a live Scylla node — load over CQL, train the compression
  dictionary, upgrade the sstables, major-compact, then read the `-Data.db` size;
- the **Parquet** column is `scylla sstable parquet-export --stats-only` over those same
  sstables, i.e. our own shredder, encoders and file writer.

No number here comes from pyarrow. This replaces an earlier version of the table that mixed
three vintages — three rows measured with pyarrow, three with our writer as it stood before
the encoding-hint and numeric-dictionary work, one current — which made the columns not
comparable with each other. Where a row moved, both causes are named below.

| Dataset | Rows | Leaves | Shape | SSTable Zstd+dicts | Parquet L1/zstd-3 | Ratio | Saved |
|---|---:|---:|---|---:|---:|---:|---:|
| D12 NOAA ISD-Lite | 300 000 | 20 | hourly station telemetry | 3 707 948 | 1 737 372 | **46.9 %** | 53.1 % |
| D1 ClickBench | 200 000 | 107 | web analytics | 27 103 967 | 13 588 131 | **50.1 %** | 49.9 % |
| D5 NYC TLC | 200 000 | 22 | numeric trips | 6 288 641 | 3 528 760 | **56.1 %** | 43.9 % |
| D9 GitHub Archive | 180 386 | 9 | event log + JSON payload | 34 506 386 | 23 139 952 | **67.1 %** | 32.9 % |
| D2 Backblaze | 300 000 | 199 | sparse telemetry | 22 256 160 | 17 213 883 | **77.3 %** | 22.7 % |
| D10 HackerNews | 300 000 | 14 | free text: titles, URLs | 23 112 344 | 18 648 016 | **80.7 %** | 19.3 % |
| D11 Wikipedia pageviews | 163 845 | 7 | hourly metrics per page | 2 567 124 | 2 180 777 | **85.0 %** | 15.0 % |

`Leaves` is Parquet leaf columns after L1 folding, which is why it exceeds the CQL column
count: `__ts` plus the two sparse-exception leaves, and one leaf per element for collections.

**The range is 0.47× to 0.85×.** Parquet is smaller on every dataset in the corpus, but the
size of the win varies by a factor of three and a half, and the ordering is not the ordering
of table width — D12 has ten CQL columns and wins most; D2 has 197 and wins second least.

**Two limits on this table, established 2026-08-18 by re-running it.**

- **It measures single-row-group files, not what Scylla writes to disk.**
  `scylla sstable parquet-export` accumulates the whole sstable in the shredder and calls
  `fragment_shredder::to_parquet()`, which emits one row group for everything. The
  row-group cutting that the storage writer does lives in `pq_writer_impl`, which the export
  path never enters. Re-running the whole corpus after changing the `rows_per_row_group` default
  from 1 000 000 to 5 000 produced **byte-identical Parquet figures**, which is how this was
  found. Production files are larger by the cutting cost, measured at **+7.4 %** on the
  perf-test dataset (§10.4c). So every ratio here is optimistic by roughly that much, and the
  fix is to measure a real `pq` sstable — see below.
- **One Backblaze run was an outlier, and the pipeline was at fault, not the dataset.** A second
  batch reported an SSTable baseline of 32 114 511 against the first batch's 22 256 160 — a 44 %
  spread — with a Parquet leaf count of 394 against 199 and a Parquet file *larger* than the
  SSTable. This was first written up here as an unseeded row sampler in the loader. **That was
  wrong**: `load_backblaze` reads `sorted()` CSV files and takes `slice(0, nrows)` with no
  randomness, and the timestamp regime is a blake2b hash of the partition key against a fixed
  base, so both the rows and their write times are deterministic. Re-running it back to back
  reproduced the *first* batch's lz4 figure to the byte (59 351 983) and its dictionary figure to
  2.2 % — dictionary training is not bit-deterministic, which accounts for the rest.

  The outlier came from the measurement script: it located the table directory with
  `ls | head -1`, an arbitrary generation if a stale one survives, and picked a single
  `-Data.db` with `head -1`, which measures one fragment of an unsettled compaction against a
  whole-table baseline. Both are fixed in `measure_native_vs_pq.sh` — newest directory by mtime,
  every sstable summed, and the sstable version asserted. The lesson is the one 10.3g already
  records: a figure that moves 44 % between runs is a bug in the harness until proven otherwise,
  and the first explanation that fits is not evidence.

**What changed against the earlier mixed table, and why.** Four rows got *worse*, and the cause
is **numeric dictionaries defaulting off**, not row-group cutting as first written here — the
export path never cut, as established above. Turning numeric dictionaries off costs size on
exactly the numeric-heavy datasets that moved most (D5 all-numeric +38 %, D12 all-numeric +12 %,
D1 +3.7 %), which is the same effect measured at 10.9 % on a numeric time-series table when the
default was chosen. It buys 10.5 % of point-read latency, which is the trade the format needs
(§10.4d).

- **D2 Backblaze, 50.6 % → 77.3 %.** Mostly the baseline, not us: the *SSTable* fell
  32 671 140 → 22 256 160, a 32 % improvement, while our Parquet output grew 16 547 521 →
  17 213 883 (+4 %). The trained-dictionary baseline got much better at sparse wide
  telemetry — 195 mostly-empty SMART columns give the dictionary an enormous amount of
  repeated structure to learn. Parquet's win here is real but modest, and the earlier 49 %
  was flattering it against a weaker baseline. Backblaze is also the one dataset where
  pyarrow now edges us out, 17 055 064 against 17 213 883 (0.9 %).
- **D5 NYC TLC, 48.2 % → 56.1 %.** Both sides moved: baseline 5 317 307 → 6 288 641 (+18 %),
  ours 2 562 753 → 3 528 760 (+38 %). NYC TLC is the corpus's all-numeric table, so it is
  the one that loses most from numeric dictionaries defaulting off — the same change measured
  at 10.9 % on a numeric time-series table — with row-group cutting on top. The baseline's
  own 18 % growth is not fully attributed and predates this run.
- **D1 ClickBench, 47.9 % → 50.1 %,** and **D12, 41.8 % → 46.9 %.** Baselines essentially
  unchanged (27 327 989 → 27 103 967; 3 706 438 → 3 707 948); ours grew 3.7 % and 12 %
  respectively, from numeric dictionaries defaulting off. Both are reproducible to within
  0.6 % across two runs (D1 13 588 131 twice; D12 1 737 372 / 1 736 838).
- **D9 GitHub 72.3 % → 67.1 %, D10 HackerNews 91.8 % → 80.7 %, D11 pageviews 93.0 % → 85.0 %.**
  These three were the pyarrow rows, and our writer beats pyarrow on all of them — by 5, 11
  and 8 points — for the reason §10.1g gives: we delta-encode the clustering key and hint
  encodings per column instead of letting a reference encoder pick defaults. The weak cases
  are therefore less weak than previously documented, and they are still the weak cases.

**Half the disk is a property of value repetition, not of table width.** This was
originally written as "a property of wide tables", because the three winners had 20–197
columns and the two losers five and seven. D12 refutes the width version: NOAA ISD-Lite has
**ten** columns and still saves 53 %. Width was a proxy, and D12 is the case that separates
it from the real mechanism. The re-measurement strengthens this: with all seven rows produced
by one writer, the widest table in the corpus (D2, 197 columns) is second from bottom.

What actually decides it is whether a column's values **repeat across rows**. Columnar
grouping pays when they do, because the repeats end up adjacent. Near-unique text barely
repeats, so there is little for the layout to exploit, while the row-oriented baseline's
trained dictionary still captures common substrings. That is why the two weakest cases are
dominated by a **high-cardinality text column that is, or is part of, the partition key** —
HackerNews `title`/`url`, pageviews `page` — and why D12 wins despite being narrow: its
eight measures are low-cardinality integers (`temp` has 992 distinct values in 300 000 rows,
`sky` has 11), so dictionary encoding collapses them regardless of how few columns there
are. D9 sits in between: its JSON payloads are individually large but share heavy structure
across rows, which the layout does capture.

Read together with D5 (NYC TLC: 20 columns, numeric, 56.1 %) the rule is: **numeric or
low-cardinality columns win big at any width; near-unique text does not win at any width.**

This spread is also the argument for measuring C6 rather than predicting it (§6.3): no
formula over column counts and type widths orders these seven correctly, so the tiering
decision samples the real data with the real writer instead.

**Replaced by §10.1f-prod.** The method described below is still what that section does for
the *native* baseline; what it replaces is the export step.

**The measurement this table was replaced by.** Now that `storage_format` converts on
compaction (§6.2a), the faithful method needs no export tool at all: load into a table, read the
native `-Data.db`, `ALTER TABLE ... WITH storage_format = 'parquet'`, major-compact, read the `pq`
`-Data.db`. That compares production output against production output, includes row-group cutting
and every other storage-path decision by construction, and cannot drift from what the server does
because it *is* what the server does.

### 10.1g D12 in detail — and a writer bug it exposed

**The dataset.** NOAA ISD-Lite hourly surface observations for 2023, 59 station-years
(`https://www.ncei.noaa.gov/pub/data/noaa/isd-lite/`). One gzip per station-year, 8 757
hourly rows each. Schema is the canonical Scylla time-series table:
`PRIMARY KEY ((station), ts)` with eight integer measures in tenths of a unit. This is the
IoT/telemetry shape none of D1–D11 had: a high-cardinality series key, a **dense regular**
clustering key, and a small all-numeric payload. Loader: `harness.py isd`.

**Missing readings, and why it barely matters.** ISD-Lite codes "not reported" as `-9999`,
at rates from 0.6 % (`temp`) to 96 % (`precip_6h`). Two defensible mappings were both
measured, because binding `NULL` in a CQL `INSERT` writes a *deletion*, so the NULL variant
carries roughly 1.1 M tombstones:

| Variant | SSTable Zstd+dicts | Parquet L1/zstd-3 | Ratio |
|---|---:|---:|---:|
| `isd` — `-9999` → NULL (tombstones) | 3 707 374 | 1 961 695 | 52.9 % |
| `isdraw` — sentinel kept, all cells live | 3 770 433 | 1 973 753 | 52.4 % |

Within half a point, so the headline does not rest on that modelling choice.

**Floats widen the gap rather than closing it.** The obvious objection to a numeric win is
that it is an artifact of integer encodings, and that real IoT tables store floats. Measured
(`harness.py isdfloat`, same observations as doubles in real units):

| Variant | SSTable Zstd+dicts | Parquet L1/zstd-3 | Ratio | Saved |
|---|---:|---:|---:|---:|
| `isd` (int32) | 3 707 374 | 1 961 695 | 52.9 % | 47.1 % |
| `isdfloat` (double) | 4 846 812 | 1 963 712 | **40.5 %** | **59.5 %** |

Parquet's size is **unchanged** (1 961 695 → 1 963 712, +0.1 %) while the SSTable grows 31 %.
The per-column footers explain it exactly: every measure is `RLE_DICTIONARY`, and the stored
size is set by the number of *distinct* values — the index width — not by the declared type.
`temp` costs 358 787 B as `INT32` and 359 003 B as `DOUBLE`; widening the values only grows
the dictionary page, by 992 × 4 bytes. The SSTable stores each cell inline at its full width
and relies on block compression, so it pays the full 4→8 byte increase. **Wider value types
make the Parquet case stronger, not weaker.**

**How `DELTA_BINARY_PACKED` actually works, since this is the largest encoding effect in the
corpus.** The encoder never stores a timestamp. It writes blocks of 128 values split into four
miniblocks of 32:

1. the first value verbatim, once, in the page header;
2. per block, all 128 deltas, and the **smallest** of them as a single zigzag varint;
3. per miniblock, the bit width needed for the largest remainder after subtracting that
   minimum — one byte;
4. the 32 remainders, bit-packed at exactly that width.

ISD-Lite is one reading per station per hour, so every delta is the same number, 3 600 000 ms.
The block minimum *is* the delta, every remainder is **zero**, and a bit width of zero means the
miniblock body is **omitted entirely**. A block of 128 timestamps costs 4 bytes of minimum plus 4
width bytes plus nothing at all: **8 bytes for 128 values, against 1 024 as `PLAIN`** — 0.06
bytes per value.

Measured it is 0.12 bytes per value rather than 0.06, and the gap is the useful part: stations do
miss hours, and a gap makes one delta exceed the block minimum, so that block needs a real bit
width and its miniblocks reappear. Roughly half the blocks here are perfectly regular. **The
encoding degrades in proportion to how irregular the series actually is**, which is what makes it
safe as a default rather than a synthetic-data trick.

Two consequences. A **dictionary** is the wrong tool even though the column is low-cardinality:
8 760 distinct hours still costs an index per row, so dictionary cost scales with *rows* where
delta cost scales with *irregularity*. And the win is **ordering-dependent** — it exists only
because sstables are written sorted by `(token, partition key, clustering key)`, so a partition's
timestamps arrive monotonically. Written in arrival order the deltas would be noise and this
column would be the largest in the file again.

**The clustering key is the largest column, and a writer bug means we are not exploiting it.**
Under pyarrow's defaults `ts` alone is 528 482 B — 27 % of the whole file — dictionary-encoded
over 8 760 distinct hourly timestamps. Re-encoding just that column as
`DELTA_BINARY_PACKED`, which is exact for a regular stride, gives:

| `ts` encoding | `ts` bytes | Whole file |
|---|---:|---:|
| `RLE_DICTIONARY` (pyarrow default) | 528 482 | 1 961 695 |
| `DELTA_BINARY_PACKED` | **36 015** (14.7× smaller) | **1 469 167** (−25.1 %) |

That would put D12 at **39.6 %** of the SSTable, the best ratio of any dataset measured.

`schema_mapping.cc` already asks for exactly this — key columns of type `bigint`/`timestamp`
get `encoding::delta_binary_packed` — and `parquet_writer.cc` honours `column_spec::preferred`
when it sees it. But `scylla sstable parquet-export` produced **2 612 496 B**, worse than
pyarrow, with `ts` at 912 882 B and the footer reporting `PLAIN`. So did `__ts`. **No column
was getting its preferred encoding.**

The cause: `write_rows()` constructed the writer as
`parquet_file_writer(nested_schema{ms.tree}, opt)` — from the schema **tree**, while the hints
live in `ms.columns`. The tree is a list of `schema_element`, which mirrors the Parquet Thrift
`SchemaElement` and correctly has no encoding field, and `walk_leaves()` recovers only path and
Dremel levels. So the hints had been silently dropped since nesting landed, and nothing noticed
because **an encoding is self-describing**: the reader honours whatever the page header says, so
files still round-tripped, just much larger.

**Fixed 2026-08-17.** `nested_schema` gained a per-leaf `preferred` list, passed alongside the
tree — structure from the tree, encoding from the caller, which is the right split given the
Thrift schema cannot carry an encoding. Result on D12, through our own writer:

| | Parquet bytes | `ts` | vs SSTable 3 707 186 |
|---|---:|---:|---:|
| before (hints dropped) | 2 612 496 | 912 882 | 70.5 % |
| **after** | **1 736 856** | **37 445** | **46.9 %** |

A 33.5 % reduction, and 11.5 % smaller than pyarrow's default — so §10.3h's "our writer matches
or beats pyarrow" is restored on this shape too, rather than merely holding where no column
benefits from delta.

Three further things came out of enabling it, which is the real argument for having measured
rather than reasoned:

1. **A latent UB bug in the delta codec, never before exercised.** With the hints dropped,
   `DELTA_BINARY_PACKED` was dead code on this path. Turning it on made the losslessness suite
   fail and UBSan name the reason: signed overflow computing `vals[i] - _prev` and
   `delta - min_delta` in the encoder, `prev += min_delta + v` in the decoder, and a
   shift-by-64 when a block's width came out at 64. All of it is now done in unsigned space,
   which is the wrap the format actually relies on and is well defined. The codec round-trips
   `int64` extremes exactly and UBSan-clean — single values, 200-value runs, and alternating
   `int64_min`/`int64_max`.

2. **The mapping's leaf order is now asserted against the tree's.** Position was already the
   only correspondence between `ms.columns` and the tree's leaves — `build_tree()` writes the
   Dremel levels back by index — and the hints ride the same assumption. A count match does not
   prove an order match, so the names are now compared too; a silent mismatch would attach one
   column's levels and encoding to another.

3. **`__ts` is deliberately left on PLAIN.** Asking for delta on the folded row timestamp makes
   `test_pq_corpus_shaped_schema` fail: cell write timestamps near `int64`'s minimum come back
   with the top bit relocated — `-2**63 + 74` reads back as `2**57 + 74`, so the low bits
   survive and the high ones do not. That is **not** the codec, which is now clean on exactly
   those values, and not leaf misalignment, which is now asserted; something in the interaction
   remains unaccounted for. Enabling it would trade a real correctness failure for about 4 KB —
   `__ts` is 2 456 bytes on PLAIN against 3 772 on delta, so it is not even a size win. Left
   off, documented, and tracked as open question 13.

Both fixes are locked in by `test_parquet_key_encoding_and_timestamp_unit`, which asserts the
*encoding* rather than a size (a size assertion would drift with any unrelated change) and was
mutation-checked against both regressions.

**A second, unrelated bug the same file exposed: timestamp columns were annotated in the wrong
unit.** A CQL `timestamp` *column value* is milliseconds since epoch; a cell's *write* timestamp
— and our `__ts` leaf — is microseconds. `converted_of()` conflated the two and annotated
columns `TIMESTAMP_MICROS` while writing millisecond values, so pyarrow read every date as
1970: the 2023-01-01 value 1 672 531 200 000 came back as 1970-01-20T08:35:31.2Z. Our own reader
inverts the mapping from `cql_type` and never consults the annotation, which is exactly why a
full round-trip suite could not catch it and a foreign decoder could. Now `TIMESTAMP_MILLIS`;
pyarrow reads 2023-01-01 through 2023-12-31. Metadata only, so no size change — but it directly
undermined §7.4's interoperability claim, which is one of the format's main justifications.

**Numeric dictionary encoding — measured, then implemented, 2026-08-17.** Dictionary
encoding was gated on `spec.type == phys_type::byte_array`, so numeric columns got neither a
dictionary nor (before the hint fix) a delta. The per-column comparison against pyarrow showed
it is not a uniform win, which is why it was measured per column rather than switched on:

| column | distinct | PLAIN + zstd | dictionary | change |
|---|---:|---:|---:|---:|
| `temp` | 992 | 353 285 | 354 757 | **+0.4 %** |
| `dewp` | 819 | 341 858 | 336 720 | −1.5 % |
| `slp` | 976 | 252 283 | 223 706 | −11.3 % |
| `wind_dir` | 362 | 286 864 | 254 403 | −11.3 % |
| `wind_speed` | 220 | 230 304 | 161 807 | **−29.7 %** |
| `sky` | 11 | 80 872 | 59 550 | −26.4 % |
| `precip_1h` | 128 | 20 433 | 15 965 | −21.9 % |
| `precip_6h` | 135 | 25 667 | 21 719 | −15.4 % |

Net **−162 939 bytes**, taking the file from 1 736 856 to **1 548 377** — 10.9 % smaller, and
D12 from 46.9 % to **41.8 %** of the SSTable. Verified externally: pyarrow reads all 300 000
rows back with the null and distinct counts intact, which is the part worth checking because
the heavily-null columns (`precip_6h` is 96 % null) are where a present-value cursor goes wrong.

`ts` stayed at 37 445 bytes: an explicit encoding hint now wins outright over the dictionary
decision. Without that precedence a monotonic clustering key qualifies for a dictionary on
cardinality alone — 8 760 distinct in 300 000 rows — and loses its delta encoding, which is the
difference between 37 kB and 528 kB.

**Cardinality does not predict which way it goes**, and that is worth recording rather than
tuning around: `slp` has 976 distinct values and gains 11 %, while `temp` has 992 and loses
0.4 %. The threshold is the same `num_distinct × 8 < rows` the byte_array path uses, and it is
a heuristic that happens to net out well here, not a rule. §10.3f reached the same conclusion
from the other direction — both obvious type-based encoding rules lost — and open question 9
already names the real answer: try both per column and keep the smaller. The remaining prize
is the 1 472 bytes `temp` gives up, so this is a correctness-free refinement, not a gap.

**Why this dataset and not the earlier ones.** It took a table whose largest column is a
monotonic clustering key to expose any of this. §10.3h's conclusion held on D1/D2/D5 because
none of their columns benefits much from delta encoding, so dropping the hints cost almost
nothing there and the loss stayed invisible. A time-series table is the shape that makes the
clustering key dominate — 27 % of the file — and so the shape that turns a silently ignored
hint into a third of the output.

**The per-column breakdown makes the mechanism concrete.** Compressed bytes per column,
from the real footers:

| D11 Wikipedia pageviews | | | D10 HackerNews | |
|---|---:|---|---|---:|
| `page` (pk) | **57.7 %** | | `url` | **38.5 %** |
| `__ts` | 37.6 % | | `title` | **37.5 %** |
| `views` | 2.9 % | | `time` | 10.1 % |
| everything else | 1.8 % | | everything else | 13.9 % |

`page` averages **1.79 repeats** across 163 845 rows at 15.2 characters — near-unique text,
even though it is the partition key. It is not the *repetition* of the key that costs
(pyarrow does dictionary-encode it); it is that there is almost nothing to repeat. The
same is true of HackerNews `url` and `title`. In both tables three quarters or more of the
file is a column whose values are individually distinct, which is the one thing a
columnar layout cannot help with, and the one thing a trained dictionary over row bytes
still can.

**A narrow row also cannot amortise its per-row metadata.** D11 is the clearest case: at
13.6 bytes per row in L1, the one mandatory `__ts` leaf is **37.5 %** of the file, and
folding it away with L2 takes D11 from 93.0 % to **58.1 %** of the baseline (both measured
on the pyarrow vintage of D11; §10.1f now puts L1 at 85.0 % with our writer, so the absolute
ratios shift while the 35-point size of the L2 saving is what matters here). The same
folding is worth 13.3 % on D2 and under 3 % on the wide tables. So metadata folding matters
most precisely where the format is otherwise weakest — which makes L2, and better
timestamp encoding generally, a bigger lever for narrow tables than any layout change.
(Read that comparison only where L2 actually applied: on D5 and D1 the uniform-timestamp
precondition failed and L2 silently fell back to L1, so their near-zero deltas mean
"not measured", not "no cost".)

**This is the case for the design as specified, not against it.** §1 asked whether Parquet
uses significantly less disk. The honest answer, updated for the eight-dataset corpus, is
*"between a third and nearly all of it, depending on whether the values repeat — measure the
table"*. It was written here as *"on wide tables, roughly half; on narrow text-keyed tables,
under 10 %"*, and the width half of that is now falsified: the **three-column** ISD cut is the
best result in the corpus at 33.8 % and the **197-column** drive telemetry is the worst at
95.8 % (§10.1f-prod). Width was a proxy for value repetition and an unreliable one. A cluster-wide switch would be
wrong. The design already assumes this: `storage_format` is a per-table property (§6), and
criterion C6 refuses to convert anything until the sampling estimator has predicted the
gain for that specific table (§10.3e validated it at 0.4 % error from 10 % of rows).

**The worked example changed when the corpus was measured properly, and in an instructive
direction.** This used to read "D10 and D11 would both be measured at 7–8 % and correctly left as
SSTables". On production output (§10.1f-prod) D10 HackerNews saves **17.5 %** and D11 pageviews
**10.4 %**, against C6's 15 % floor — so pageviews is declined and **HackerNews is converted**. One
of the two examples of "correctly declined" is now an accept.

That is not a flaw in C6; it is C6 working. A 17.5 % disk saving on a text-heavy table is worth
having, and the earlier 7–8 % figures came from pyarrow-written files that understated our writer
(§10.3h). The lesson is narrower and worth stating: **an example chosen to illustrate a threshold
has to be re-checked when the measurements move, because it can quietly start illustrating the
opposite.**

Threats to validity: D9 truncates each payload to 4 000 characters; one hour of GitHub
Archive and three hours of pageviews are each one traffic pattern.

### 10.2 Sensitivity to row group size

Swept by row count (L1/zstd-3, token order, realistic). Byte-sized sweep at bottom-tier
scale is still to do.

| Dataset | 125 k rows | 500 k rows | 2 M rows | 125 k vs 500 k |
|---|---:|---:|---:|---:|
| D5 NYC TLC (20 cols) | 10 135 982 | 8 634 001 | 8 634 001 | +17.4 % |
| D2 Backblaze (197 cols) | 17 688 614 | 17 055 064 | 17 055 064 | +3.7 % |
| D1 ClickBench (105 cols) | 15 761 792 | 16 177 362 | 16 177 362 | **−2.6 %** |

No universal answer: narrow tables want large row groups (dictionary and delta state
amortise), while ClickBench is *better* with smaller ones. Confirms `rows_per_row_group` must
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

### 10.3i In-node estimator, and a trap in `data_size()`

`GET /storage_service/estimate_parquet_ratios?keyspace=&cf=&rows=` samples rows from the
largest SSTable of a table, runs them through the real shredder and writer, and reports a
predicted ratio per folding level. This is criterion C6 in the form the policy needs it:
in-node, cheap, and answered from the data.

Measured against the known result for ClickBench (§10.3h full re-encode, ratio 0.479):

| Folding level | Sampled | Parquet bytes | SSTable on disk | Ratio |
|---|---:|---:|---:|---:|
| L0 | 20 000 | 2 171 086 | 13 611 500 | 0.798 |
| **L1** | 20 000 | 1 202 255 | 13 611 500 | **0.442** |
| L2 | 20 000 | 1 202 255 | 13 611 500 | 0.442 |

0.442 against 0.479 — comfortably good enough for a 15 % decision threshold, though less
accurate than the 0.4 % seen in §10.3e because this samples the *first* 20 000 rows
rather than uniformly.

**The trap.** `sstable::data_size()` returns the **uncompressed** logical size, not the
bytes on disk. The first working version of this endpoint used it and reported a ratio of
**0.067** — Parquet looking 7× better than it is, because the SSTable side was being
measured before compression. `ondisk_data_size()` is the correct accessor. It was caught
only because 0.067 was implausible next to every other measurement in this document, and
confirmed by `ls`-ing the actual Data.db: 13 611 500 bytes against the 89 496 640 that
`data_size()` reported.

That is the third baseline error in this project (after the untrained dictionary in
§10.3h and the local-time timestamp digest in the cross-read harness). Every one produced
a number that flattered Parquet, and every one was caught by comparison against an
existing measurement rather than by review.

**The same trap inside compaction, resolved 2026-08-19.** `data_size()` is not one unit —
uncompressed bytes for a compressed native sstable, file bytes for a `pq` one — and both
size-tiered bucketing paths were built on it. In a hybrid table a converted sstable therefore
reports 5–10× smaller than its true peer and buckets several tiers too low, so compaction keeps
choosing to rewrite the format that is most expensive to rewrite. This is in the live path for
both supported strategies: ICS directly, and TWCS through its per-window size-tiered fallback.

The fix makes the unit a property of the candidate *set*, not of the format: an all-native set
keeps `data_size()` — self-consistent there, so no existing cluster's bucketing moves — and a
mixed set uses `ondisk_data_size()`, which means the same thing for both. See
`mixed_formats()` / `bucketing_size()` in `compaction/size_tiered_compaction_strategy.cc` and
`compaction/incremental_compaction_strategy.cc`, plus `sstables::sstable_run::ondisk_data_size()`
for the run-level case.

The symptom would have been silent — compaction quietly declining to group the converted files,
nothing logged and nothing failed — which is why it is worth stating that the general fix
(bucket everything on `ondisk_data_size()`) was rejected: it is correct in principle and a
behaviour change for every existing cluster, so it is not a decision this project should make on
its own.

**Where mixed candidate sets actually arise, after §10.7.** This was first written to say that
making TWCS entirely Parquet turned mixed sets into the steady state of a TWCS active window, since
native flushes would sit alongside Parquet compaction outputs. That stopped being true the same day:
flushes now write Parquet too on any table that writes Parquet unconditionally (§10.7), so a TWCS
table is Parquet end to end and its candidate sets are homogeneous.

The mixed case is therefore what it was before: **ICS under `'hybrid'`**, where the criteria convert
some outputs and not others by design, plus any table for the duration of an `ALTER` between
formats. Both are real and both are unmeasured at scale — but neither is a steady state, which is a
weaker claim than the one this paragraph used to make.

### 10.3h §10.1 re-measured through our own writer

§10.1's Parquet column came from pyarrow. All three datasets have now been re-measured
with `scylla sstable parquet-export`, i.e. the real shredder, encoders and file writer,
reading the real SSTables.

| Dataset | Rows | SSTable (Zstd+dicts) | **Parquet, our writer** | **Ratio** | pyarrow (§10.1) | pyarrow ratio |
|---|---:|---:|---:|---:|---:|---:|
| ClickBench | 200 000 | 27 327 989 | **13 099 368** | **47.9 %** | 16 177 362 | 59.3 % |
| Backblaze | 300 000 | 32 671 140 | **16 547 521** | **50.6 %** | 17 055 064 | 52.5 % |
| NYC TLC (§10.3d schema) | 200 000 | 5 317 307 | **2 562 753** | **48.2 %** | — | — |

> Superseded for absolute sizes by §10.1f, which re-measures the whole corpus in one run
> of one binary. This table is kept for the writer-versus-pyarrow comparison it makes, which
> still holds: §10.1f puts our writer ahead of pyarrow on five of the six datasets where both
> have been measured, behind by 0.9 % on Backblaze.

**Our writer is not worse than pyarrow, and on ClickBench it is materially better** —
47.9 % against 59.3 %. The headline claim of §10.1 therefore holds when measured with the
implementation rather than a reference encoder. The threat-to-validity "sizes come from
pyarrow" is fully retired.

The ClickBench margin is not yet explained and should not be over-claimed: the two runs
differ in dictionary thresholds and in page splitting (our dictionary-encoded chunks are
currently a single page), and either could account for it.

**A near-miss worth recording.** The first attempt at this measurement reported 25.1 %
for ClickBench. That baseline was wrong: creating a table with
`ZstdWithDictsCompressor` does *not* mean its SSTables are dictionary-compressed — the
dictionary has to be trained and the files rewritten. The untrained baseline was
52 103 621 bytes, which is the plain-Zstd figure, and it flattered Parquet by roughly 2×.
It was caught only because that number exactly matched the plain-Zstd column of §10.1.
Any future measurement must `retrain_dict` and `upgrade_sstables` before comparing —
`harness.py` already does; the ad-hoc loader did not.

### 10.3g L3 export: for interoperability, not for size

Folding level L3 (§5.3) emits the user's CQL schema and nothing else — no `__ts`, no
exception channel, no TTL or deletion columns. Measured on the same real table:

| Level | Bytes | Leaves | Columns an analytics reader sees |
|---|---:|---:|---|
| L1 (storage) | 2 562 753 | 12 | the 11 CQL columns + `__ts` |
| **L3 (export)** | **2 562 097** | **11** | the 11 CQL columns |

**656 bytes smaller — 0.03 %.** L3 buys essentially nothing in size, because the folded
`__ts` column already compresses to ~603 bytes for 200 000 rows (§10.3d). Its value is
entirely that the file *is* the table: a Spark or Trino user sees the schema they expect
with no Scylla-internal column to explain or filter out.

That reframes L3. It is an interoperability feature, not a compression one, and it should
be described that way — the §7.4 case for it stands, the implied size argument does not.

L3 is lossy and can never be a storage format. `reassemble()` refuses it rather than
inventing write times, `folding_is_lossless()` reports it, and the storage path goes
through `to_parquet_for_storage()` which rejects any lossy level. All three are tested.

### 10.3f Type-based encoding rules do not work

§3.1 lists `BYTE_STREAM_SPLIT` and `DELTA_BINARY_PACKED` as structural advantages of
columnar storage. §10.3d then noted the double columns were still `PLAIN` and called
enabling byte-stream-split "an easy remaining win". **It was tried and it lost.**

Same 200 000-row real table as §10.3d, measured against the same SSTable:

| Encoding rule | Bytes | vs. SSTable | Change |
|---|---:|---:|---:|
| PLAIN for regular columns (baseline) | 2 562 753 | 48.2 % | — |
| + `BYTE_STREAM_SPLIT` on all doubles | 3 968 805 | 74.6 % | **+54.9 %** |
| + `DELTA_BINARY_PACKED` on all bigints | 2 569 567 | 48.3 % | +0.3 % |

The double regression is dramatic: `dist`/`fare`/`tip`/`total` went from 1 571 497 bytes
combined to 2 970 733. The bigint regression isolates to one column — `payment_type`
went 41 574 → 48 389, which is the entire delta.

**Mechanism.** Both transforms destroy whole-value repetition. Money-shaped doubles
(`0.0`, `12.50`, `2.00`) and low-cardinality integers (`payment_type` ∈ {1,2,3,4}) repeat
*exactly*, and zstd compresses exact repeats far better than it compresses the residuals
that byte-splitting or delta-ing leave behind. These encodings win on high-entropy
continuous data — sensor readings, monotonic counters — and lose on categorical data
that merely happens to be stored in a numeric type.

**Consequence.** Type is the wrong signal. The physical type says nothing about whether a
column's values repeat, and that is the property that decides the encoding. Regular
columns therefore default to PLAIN.

Delta *is* kept for the key columns and the folded `__ts`, where monotonicity is a
property of construction rather than of the data: §10.3d measured `vendor` at 525 bytes
and `__ts` at 603 bytes for 200 000 rows.

This also revises §3.1: those encodings are *available* advantages, not automatic ones.

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

### 10.4 Throughput and latency vs. targets — measured 2026-08-17

`pq` and the default format written from the same mutations in the same process, by
`test/boost/sstable_parquet_perf_test.cc`. 20 000 partitions × 5 rows, 6 columns, values
with realistic redundancy (a small vocabulary for the text columns) rather than random
bytes, which would make every format look alike.

| Path | Default (`me`) | `pq` | Δ | Target (§1.2) | Pass? |
|---|---:|---:|---:|---|---|
| Write | 580 ms | 609 ms | **1.05×** | ≥ −10 % | **yes** |
| Full scan, streaming | 143 ms | 136 ms | **0.95×** | ≥ parity | **yes** — at parity |
| Full scan, through CQL | 15 200 ms | 34 866 ms | **2.29×** | ≥ parity | **no** — §10.26 |
| Point read | 26–136 µs | 1.15–18.3 ms | **44–134×** | ≤ 1.2× | **no** |
| Data size | 3 994 586 B | 1 275 614 B | **0.32×** | — | — |
| Peak scan memory | 256 kB | 5 548 kB | 22× | bounded | **yes** — see below |

The scan row was one row until 2026-08-20 and it hid the result that matters. The reader has two scan
paths and takes the fast one only for an unbounded partition range, which is what compaction and
streaming pass and what this in-process test passes; a range scan from a client is split at tablet
boundaries and every piece of the split is bounded, so it gets the point-read path instead. The
figures in the second row are 8 M rows through the coordinator (§10.26), not the 100 000 in the
first, so read the ratio and not the milliseconds. **The format scans at parity; the reader does not.**

**R-13 (bounded memory) holds on the read path.** Peak *scan* memory against sstable size,
same schema. The **write** path is a different story and is not bounded at all — see §5.5a:

| Rows | Data bytes | Peak scan memory | Point read |
|---:|---:|---:|---:|
| 20 000 | 248 619 | 13 712 kB | 1 026 µs |
| 160 000 | 1 994 243 | 15 532 kB | 3 055 µs |

8× the rows costs **1.13×** the memory. The absolute figure is higher than the row-format
reader's because the unit of work is a decode window plus the pages behind it, but it does
not grow with the file, which is what R-13 asks.

#### How it got here, and what each step was worth

The first reader that made a pq sstable readable decoded the whole Parquet image before
emitting a fragment. Keeping the intermediate numbers is worth more than the final ones,
because they separate the format's costs from the implementation's:

| | Whole image | + streaming | + paged reads, dictionary fix, page sizing |
|---|---:|---:|---:|
| Write | 1 357 ms | 572 ms | 609 ms |
| Full scan | 232 ms | 144 ms | 136 ms |
| Point read | 183 857 µs | 5 120 µs | **2 421 µs** |
| Memory growth at 8× rows | 8.13× | 1.13× | 1.13× |

*Streaming.* Load the footer alone, turn the partition range into a row-ordinal window via
the index entry (§5.4 option A), decode one row group at a time in fixed windows, and use
the V2 page header's `num_rows` to step over pages without decompressing them. This is
what bought bounded memory, write parity, and a full scan level with the row format — 1.02–1.13x on
the streaming path, re-measured across nine arms in §10.26. It was recorded here as 0.82x and as
"an understatement of the target"; both are withdrawn. "Scan parity" (§1.2) is met on this path and
missed by 2.3x on the one CQL uses, for the reason §10.26 gives.

*Paged reads.* For a bounded range, fetch two extents per column chunk — the dictionary
page and the contiguous run of data pages the OffsetIndex says covers the wanted rows —
instead of the whole row group. Worth little on its own here, because the cost had already
moved elsewhere; it matters at scale, where a row group is far larger than a partition.

*The dictionary was the real cost, and profiling was the only way to find it.* Three
plausible explanations were each measured and rejected before the right one turned up: the
summary being too sparse (no change), the page size (5 291 → 4 928 µs for a 20× reduction),
and per-string allocation when materialising the dictionary (no change). Phase timing then
showed 235 ms of 250 ms sitting in `decode_columns`, and disabling dictionaries entirely
dropped it to 13 ms. The cause is structural: **a point read must decompress a column
chunk's entire dictionary page before it can decode a single value**, so a
near-unique dictionary is paid for in full on every read. The writer's heuristic admitted
any column with under 50 % distinct values; requiring 8× repeats instead

| Dictionary threshold | Point read | Data bytes |
|---|---:|---:|
| `num_distinct × 2 < rows` (old) | 5 001 µs | 1 355 705 |
| `num_distinct × 8 < rows` (new) | 1 771 µs | 1 312 381 |

— made point reads 2.8× faster *and* the file slightly smaller, because zstd was already
finding those repeats and the dictionary page was mostly overhead.

*Page sizing.* A point read decodes whole pages, so page size trades bytes for latency:

| `page_values` | Data bytes | Point read |
|---:|---:|---:|
| 1 024 | 1 349 499 | 1 728 µs |
| 2 048 | 1 312 381 | 1 791 µs |
| **8 192** | **1 275 614** | **2 151 µs** |
| 20 000 | 1 252 083 | 2 836 µs |

8 192 is the default: +1.9 % size over the largest page for 24 % lower latency. Like
`rows_per_row_group` (§8.2), it stays tunable, because the right point on that curve depends
on whether a table is scanned or probed.

#### The target still missed

**This row cannot honestly be a single number, and reporting it as one was itself a defect.**
The point-read ratio is linear in leaf count at ~90 µs per leaf (§10.4e): 44× on a 10-leaf table
and 134× on a 200-leaf one. Earlier revisions of this table quoted 55×, then 62×, then 78×,
each time from one schema and one row-group setting, and each time as though it were a property
of the format. It is a property of the format *and the table*.

The mechanism is inherent to columnar layout rather than to this implementation: a point read
touches *k* column chunks, and for each one it pays a page decode and, where a dictionary is
used, a dictionary-page decompress — against a row format that reads one contiguous row. That
is why the cost tracks *k* so cleanly, and why no amount of caching changes the slope, only the
intercept. The row-group default (§10.4c) bought 1.65× of the intercept. Closing the rest of the
gap means caching decoded dictionary and page state across reads, which is what
parquet-cpp and arrow-rs do and what Scylla's cache tracker exists for; that is a caching
change, not a format change. Until then the honest statement is 2.4 ms against 39 µs, and
§4's promise of p99 ≤ 1.2× does not hold for cold point reads.

This is also the strongest argument for the hybrid design in §5.6: point-read-heavy tables should
stay on SSTables. Note that **nothing refuses them automatically** — an earlier version of this
paragraph claimed criterion C7 did, which was never true after C7 left the policy and is not true
now that it has been dropped outright (§6.3). What enforces the scope limit is C5's column ceiling,
which catches wide tables and not narrow point-read-heavy ones; for those, the protection is that
`'hybrid'` on an ICS table still has to pass C6, and that an operator can set `'sstable'`.

Threats to validity: single shard, warm page cache, one synthetic schema, and the 0.32×
size ratio is better than the 0.34–0.96× measured on real datasets (§10.1, §10.1f-prod) because
generated values repeat more than real ones. The *timing* ratios are the point of this
table, not the size.

### 10.4f Page size was dead too — and it is worth 1.41x

The writer emits `min(page_values, row group size)` values per page. With `page_values` at 8 192
and row groups cut at 5 000 rows (§10.4c), the page bound never bound: every data page covered a
whole row group, so a point read decoded 5 000 values to return one row. The same defect as
`rows_per_row_group` before it was changed — a knob whose value could not reach the code — and it went
unnoticed because fixing the *other* one is what made this one dead.

Re-swept at the current row-group default, 3 000 random point reads, one pinned core:

| `page_rows` | point p50 | point mean | size | Δ size |
|---:|---:|---:|---:|---:|
| 8 192 (one page per row group) | 1 158 us | 1 213 us | 1 269 816 | base |
| **2 048** | **820 us** | **903 us** | 1 349 691 | **+6.3 %** |
| 1 024 | 839 us | 832 us | 1 419 666 | +11.8 % |
| 256 | 731 us | 790 us | 1 967 652 | +54.9 % |

**2 048 dominates 1 024** — faster p50 and 5.5 points smaller — so the curve is not monotone in the
way one would assume, and 1 024 would have been the wrong pick from theory alone. Scan time
(136-139 ms) and scan memory (~5.6 MB) are flat across the sweep, so this is size against
point-read latency and nothing else. **Default is now 2 048.**

**Two dead knobs, compounding.** Point-read p50 was 1 915 us with both at their shipped values;
it is now **820 us** — **2.3x** — for about 14 % of size in total. Neither change required new
machinery, only noticing that a configured value could not reach the code. Against the native
format the gap narrows from 62x to roughly 29x on this schema.

**Dictionary chunks now page too, and the result is instructive for being small.** They used to
be emitted whole whatever `page_values` said, because the index stream was encoded once for the
chunk. `dict_result` now retains the raw indices and each data page carries its own RLE stream
over its slice; the dictionary page itself stays per chunk, which is what Parquet specifies.

Measured at the 2 048 default: **p50 820 -> 782 us, 4.7 %, for +0.15 % size.** Much less than the
1.41x that paging the *plain* columns bought, and the gap says something about where point-read
time actually goes: **it is dominated by per-column-chunk fixed work -- footer parse, offset index
lookup, page location -- rather than by how many values get decoded.** Halving decode volume on
two of five value columns moved the total by 5 %. That is also why pushing to 512 rows per page
buys only a further 13 % of latency for 17.8 % of size, and why the ~90 us per column in §10.4e
should be read as mostly locating, not decoding.

So the value of this change is less the 5 % than the pathology it removes: the old behaviour was
unbounded in the row-group size. At the 5 000-row default a dictionary chunk was 5 000 values
against a 2 048-value page, a factor of 2.4; a table configured with 100 000-row row groups had
dictionary columns decoding 100 000 values per point read while its plain columns decoded 2 048.
The asymmetry scaled with a knob, which is the kind of thing that looks fine in a benchmark and
bites in production.

**Verified at value level, not just structurally.** A mis-sliced index stream produces a file that
parses cleanly and yields *wrong values*, so parsing is not evidence. The checks that matter:
`sstable_parquet_test` writes mutations through and compares them back; the nine interop fixtures
are re-read by pyarrow with every row group fully decoded rather than only their metadata
inspected; and the streamed-vs-buffered images remain byte-identical.

### 10.4l The footer cache — built 2026-08-20, and two of its three obstacles were avoidable

§10.21 measured a cold point read at **4.42 µs per row group per file**, and §10.22 showed why: the
footer is 1 420 bytes per row group, so fetching it and walking it is the term that scales. Both are
pure functions of an immutable file, so both need happen once per sstable rather than once per read.
Numbers in §10.24; this section is the design.

**What is cached, and where it lives.** One entry per sstable, held by the `sstable` object itself.
The entry type is declared through a deliberately Parquet-free header
(`sstables/parquet/footer_cache.hh`) so that `sstables.hh` can hold, size and drop it without
pulling in the format layer:

- the **plaintext** footer bytes — the whole Thrift blob, because the lazily-parsed metadata is a
  set of byte extents into it;
- the lazily-parsed `file_metadata`: the schema plus each row group's `num_rows` and column-list
  extent, and no per-column metadata;
- the cumulative first-row table, which every read needs in order to map a row ordinal to a row
  group and which is itself O(row groups);
- for an encrypted file, the crypto *envelope* — algorithm, `aad_file_unique`, AAD prefix, key id —
  and **not the key**.

**Obstacle 1, avoided: it lives on `sstable`, not in `shareable_components`.** The warning this
section carried stands — `_components` is a `foreign_ptr<lw_shared_ptr<shareable_components>>`, may
belong to another shard, and mutating a parsed footer there would be a cross-shard race that
single-shard testing cannot show. But the `sstable` object is shard-local, and the reclaim machinery
already operates on it, so putting the entry there makes publishing and dropping it shard-local by
construction. The cost is that two shards holding the same file hold two entries; the reclaim budget
is per-shard anyway, so they are accounted separately and correctly.

**Obstacle 2, avoided: the format-layer refactor called "the bulk of the work" was not needed.** The
objection was that `read_row_range()` indexes `md.row_groups[rg].columns` itself, so a reader holding
its materialised columns separately cannot use it. True — but a reader can hold a *one-group*
`file_metadata`: the schema, which is all `walk_leaves()` needs, plus a copy of the single row group
it is reading, materialised into that copy and passed as row-group index 0. Fixed size, no threading
of column lists through the format layer, and the shared entry is never mutated. That last property
is what keeps a full scan from growing the entry into the eager parse that lazy mode exists to
avoid.

**Obstacle 3, changed rather than avoided: it is not reloaded, it is re-parsed.** Bloom filters are
read at open time and nothing else will put one back, which is why `sstables_manager` keeps a
`_reclaimed` set and a reload fiber. A footer cache repopulates itself on the next read that wants
it, and reloading it eagerly would re-read footers for sstables nobody is reading. So
`reclaim_memory_from_components()` now distinguishes *released* bytes from *owed* bytes: it returns
the former and adds only the latter to `total_memory_reclaimed()`, and the manager inserts into
`_reclaimed` only when something is actually owed. Everything else — being reported through
`total_reclaimable_memory_size()`, being picked by "reclaim from the sstable holding the most", being
governed by `components_memory_reclaim_threshold` — is the existing policy untouched. There is no
second policy.

Two incidental faults in that loop were fixed while reading it: it dereferenced `_active.end()` if
the accounting ever claimed more was held than any live sstable holds, and it spun without a yield
when nothing could be reclaimed — a reactor stall rather than a bad decision. Both are now bounded.

**Memory is measured, not estimated.** `memory_size()` sums container capacities: the footer vector,
the row-group and schema vectors, every heap-allocated string in them, and the cumulative-row table.
Nothing is inferred from the on-disk footer length. Measured at **1 522 bytes per row group** against
1 425 on disk (§10.24) — the difference is the 88-byte lazy row-group record plus its 8-byte
cumulative-row entry, which is the structure the number should have and is the reason to measure it
rather than assert it. A unit test asserts the figure is non-zero, because an entry reporting zero
would be invisible to the reclaimer and the cache would silently stop being evictable.

**Encryption.** The entry holds the *decrypted* footer and never the key. That split is the point:
the plaintext is exactly as sensitive as the row cache's contents and dies with the sstable, whereas
a cached key would outlive the read that was authorised to fetch it and would sit outside the
provider's control. A reader on a cache hit asks the provider again; what it saves is the footer
read, the decrypt and the parse. Verified on live `PARE` tables — reads hit the cache and return
identical rows (§10.24).

**Metrics**, on the `sstables` group: `pq_footer_cache_hits`, `_misses`, `_populations`, `_evictions`
— drops under memory pressure only, since an sstable closing is not an eviction — and the `_bytes`
gauge, which is the one to watch against the reclaim threshold.

### 10.4k Lazy footer parsing: 12 %, because Thrift skip is O(content)

§10.4j argued the fix for a footer parse that scales with file size was to parse only the row
group a read touches. Implemented: `metadata_mode::lazy` decodes the schema and each group's
`num_rows` — needed regardless, since mapping a row ordinal to a group requires every count —
records the byte extent of each group's column list, and decodes none of them.
`materialise_row_group()` decodes one on demand.

**It bought 12 %, not an order of magnitude:**

| row groups | eager | lazy |
|---:|---:|---:|
| 100 | 440.1 us | 388.4 us |
| 20 | 89.4 us | 82.2 us |

**The reason is a property of the format, not of the implementation.** TCompactProtocol writes no
length prefix on a struct or a list, so skipping one means walking its contents to find where it
ends: field headers, varints, nested structs, all of it. `skip()` is O(content), so a lazy parse
still reads every byte of every column chunk it means to ignore. What it avoids is *constructing*
the objects — and that turns out to be only 12 % of the cost, with the byte walk being the rest.

**Which is worth keeping anyway, for a reason that is not latency.** An eager parse of an
8 000-group, 18-leaf footer allocates roughly **144 000 `column_chunk` objects, transiently, on
every point read**. Lazy parsing allocates a `num_rows` and two `uint32_t` per group and builds
chunk objects only for the group being read. The allocation cost was never separately measured and
is not in the 12 %, since the profile times parse rather than allocator pressure; but a per-read
allocation that scales with file size is a hazard on its own.

**And it makes caching viable, which §10.4j had rejected.** The objection there was that caching
parsed `FileMetaData` means holding tens of megabytes per sstable. Lazily-parsed metadata is a
different object: 8 000 groups times about sixteen bytes is **~128 kB**, three orders of magnitude
smaller, and it is exactly what would eliminate the repeated byte walk. So the sequence is
lazy-then-cache: lazy parsing alone does not fix the latency, but it turns the cache from
prohibitive into cheap. That is the next step, and it needs a per-sstable store rather than
per-reader state.

**Validation is deferred, not dropped.** `validate()` cannot check chunk-per-leaf counts on a group
whose chunks are not decoded, so those checks moved into `materialise_row_group()` and run when a
group is decoded. Every chunk a reader actually looks at is still checked; the schema and
row-count checks — the ones that catch a truncated or fabricated footer — still run up front. All
34 conformance sub-tests pass.

### 10.4m `page_values = 2048` reverted — the one-schema cost estimate was 2.5x low

§10.4f set `page_values` to 2 048 for a 1.41x point read at a measured +6.3 % size. That +6.3 %
came from the perf schema. Measured on real data, varying only `page_rows` on current code so
that dictionary paging is not confounded with it:

| dataset | 8 192 | 4 096 | 2 048 |
|---|---:|---:|---:|
| NOAA ISD-Lite, `pq` bytes | 1 803 440 | 1 922 751 (+6.6 %) | 2 105 580 (**+16.8 %**) |
| Backblaze, ratio to SSTable | 95.8 % | 103.8 % | **111.9 %** |

**The real cost is +16.7 %, not +6.3 %** — consistent across a 10-column numeric table and a
197-column sparse one, so it is not a quirk of one shape. And at 2 048 Backblaze crosses from a
marginal win to a **net loss**.

The latency was genuine and, measured again on current code, larger than first reported: p50
971.6 → 587.4 µs, **1.65×** (the batched-read fix of §10.4i amplifies it). **Reverted anyway.**
A point read is 20–33× the row format at either setting, so the trade spent a sixth of the
format's entire reason for existing to move a metric that stays uncompetitive. 4 096 is not a
compromise worth taking either: 1.14× for +7.5 %, and Backblaze still loses at 103.8 %.

**Two things this says beyond the one default.** A size cost measured on one schema transferred
badly by a factor of 2.5, which is the same failure that produced the C2 threshold and the 78×
point-read figure; a per-dataset check is cheap and should precede any default that trades size.
And the direction of travel for point-read latency should be the footer cache (§10.4l), which at
production scale dominates everything measured in §10.4 and costs no size at all — as against page
and row-group tuning, which are now both spent and both paid in disk. Built and measured on
2026-08-20: 1.5× to 12.6× on cold point reads for no disk at all (§10.24).

### 10.4j Footer parse scales with sstable size — and it invalidates the ranking above

`footer_parse` was 89 us and 10 % of a point read, and the plan was to cache it. Before doing
that, one check: does the cost depend on how many row groups the file has? It does, linearly.

| row groups | `footer_parse` | point p50 |
|---:|---:|---:|
| 100 | **440.1 us** | 807.9 us |
| 20 | 89.4 us | 573.5 us |
| 1 | 15.3 us | 515.7 us |

Least squares over those three points: **7.4 us + 4.32 us per row group.** Extrapolated at the
5 000-row default:

| | row groups | footer parse |
|---|---:|---:|
| the perf test (100 000 rows) | 20 | 0.09 ms |
| 1 M rows | 200 | 0.87 ms |
| 256 MB of ISD-Lite at 6 B/row | 8 000 | **34.6 ms** |

**So the 89 us is an artifact of measuring a 1.27 MB file.** A real bottom-tier sstable — the only
kind hybrid mode converts — would spend tens of milliseconds parsing its footer on *every point
read*, dwarfing every other cost in §10.4h and making the format unusable for point reads at
production scale. It is a per-read cost that grows with file size, which is the worst shape a cost
can have.

**This invalidates the ranking, not the measurements.** Every figure in §10.4c through §10.4i is
correct for a 100 000-row sstable, and the four fixes are all real: the knobs really were dead,
the dictionary chunks really were unpaged, the reads really were serial. But `decode_cpu` at 31 %
is only the largest phase *at this file size*, and the conclusion "what remains is decode" does not
survive extrapolation. **Everything in §10.4 should be read as measured on a file two to three
orders of magnitude smaller than the target workload.** That caveat should have been attached from
the start.

**And it makes caching the wrong fix.** Caching parsed `FileMetaData` per sstable would hold a
structure that also grows with file size — 8 000 row groups times ~18 leaves is ~144 000
column-chunk entries per sstable, tens of megabytes resident, multiplied by every sstable being
read. That is precisely the "cached components that fill memory" the review warned against, and it
would trade an unusable latency for an unusable memory bill.

**The fix is to stop parsing what is not needed.** A point read touches exactly one row group. The
footer's schema section is small and fixed; its row-group section is the part that scales, and
almost all of it is irrelevant to any single read. Parsing row-group metadata lazily — locating the
wanted entry and decoding only that — makes the cost independent of file size without caching
anything. That is a change to `parse_file_metadata` and its callers, and it is now the highest
priority item in the read path by a wide margin.

### 10.4i 35 serial reads per point read — fixed, 1.35x

Sub-splitting `decode_paged` as §10.4h said it needed gave the answer immediately, and it was not
a decode problem at all:

| sub-phase | before | |
|---|---:|---|
| `page_fetch` | 338 us over **35 calls** | 43 % of the point read |
| `decode_cpu` | 287 us | |

**Thirty-five sequential awaited reads to return one row.** Two extents per leaf — the dictionary
page and the run of data pages covering the wanted rows — over roughly eighteen leaves, each
`co_await`ed inside the loop that computed it. The reads have no dependency on one another; the
serialisation was purely an artifact of planning and fetching in the same pass.

Split into plan-then-fetch: build the extent list from the OffsetIndex with no I/O, then issue
every read at once and `when_all_succeed`.

| | before | after | |
|---|---:|---:|---|
| `page_fetch` | 338 us, 35 calls | **81 us, 1 batch** | 4.2x |
| `decode_paged` | 646 us | 378 us | |
| point p50 | 782 us | **581 us** | **1.35x** |
| point mean | 867 us | 616 us | |
| ratio to native | ~29x | **22.5x** | |

**Cumulative, across four changes:** point-read p50 has gone 1 915 -> 1 158 (row-group default)
-> 820 (page size) -> 782 (dictionary paging) -> **581 us**, a total of **3.3x**, for about 14 % of
size. Two of the four were knobs whose values could not reach the code, one was a missing
implementation, and this one was a loop that awaited what it could have batched. None of them
needed a cache, which is what §10.4b assumed the answer would be.

**What is left, and it is now decode.** `decode_cpu` is 279 us and 31 % — the largest single phase
— followed by `footer_parse` at 89 us and 10 %, which is the cacheable one identified in §10.4h.
Everything else is under 5 % individually.

**A risk worth recording rather than discovering later.** Thirty-five concurrent reads per point
read is easier on latency and harder on the I/O queue: under many concurrent point reads this
multiplies queue depth by the leaf count. The reads are small and against one file, and nothing
here measures a loaded node, so if this shows up as queueing under concurrency the fix is to cap
the batch rather than to go back to serial.

### 10.4h The reader, profiled — and three of four hypotheses were wrong

§10.4g named four candidates for the 586 us fixed floor and said instrumentation was needed
rather than another sweep. `PQ_READER_PROFILE=1` adds per-phase timers to the real reader (always
compiled, so the profile is of the shipped path). 2 000 random point reads:

| phase | us/call | share |
|---|---:|---:|
| **page_decode** | **628.0** | **81.8 %** |
| footer_parse | 87.3 | 11.4 % |
| footer_io | 13.5 | 3.5 % |
| offset_index | 15.8 | 2.1 % |
| schema_recover | 5.8 | 0.8 % |
| index_lookup | 3.7 | 0.5 % |

**Three of the four candidates are negligible.** The partition-index lookup (0.5 %), per-reader
schema-mapping construction (0.8 %) and the OffsetIndex read (2.1 %) come to **3.4 % combined**.
Caching any of them — which the notes have proposed since §10.4b — would be invisible. That is
three ideas retired by one measurement, and none of them would have been retired by reasoning.

**Reconciling with §10.4g rather than discarding it.** The fit said 586 us fixed and 234 us decode;
the profile says 628 us in `decode_paged`. Both are right, because `decode_paged` is a composite:
it fetches the pages, parses a page header per column chunk, *and* decodes values. Only the last
of those scales with `page_values`. So the fit's 234 us is the value decode inside that 628 us, and
the remaining ~394 us is per-column-chunk fetch and header parse — which is exactly what §10.4g
guessed the fixed floor was, sitting in a phase it did not expect to find it in. The lesson is
that phase boundaries chosen for instrumentation convenience can hide the very split being looked
for; `decode_paged` needs sub-splitting before this goes further.

**One clean win is already visible.** `footer_parse` is 87 us per reader, 11.4 %, and it is a pure
function of bytes that do not change between readers of the same sstable. Caching parsed
`FileMetaData` per sstable rather than per reader would remove essentially all of it. Note this is
*not* what §10.4g's row-group experiment measured: shrinking the footer 20-fold moved the total
only 7 %, because most of the parse is schema elements rather than row-group entries, so the win
comes from not re-parsing at all rather than from parsing something smaller.

### 10.4g Where the point read actually goes: 71 % is a fixed floor

Two knob fixes in a row (§10.4c, §10.4f) and a real implementation change (dictionary paging)
took p50 from 1 915 us to 782 us. This decomposes what is left, so that further work goes where
the time is rather than where the last win was.

**Method.** A two-point linear fit on the page-size sweep — `T = F + k x page_values`, taking the
5 000 and 2 048 points — then checked against a third point that did not inform it:

| | fit | measured |
|---|---:|---:|
| decode slope `k` | 0.114 us/value | — |
| fixed floor `F` | 586 us | — |
| p50 at 512 values | 645 us | 680 us |

Within 5 % at a point the fit did not see, which is about as much as a two-point fit earns.

**So at the current default: ~586 us fixed, ~234 us decode. 71 % of a point read is work that
does not scale with page size at all.** That is the number to attack, and it also means page and
row-group tuning is now exhausted — both act only on the 29 %.

**It is not footer parse.** The obvious candidate was the footer, which is rebuilt per reader and
grows with row-group count. Tested directly by varying row groups at a fixed 2 048-value page, so
decode volume is held constant while the footer shrinks 20-fold:

| `rows_per_row_group` | point p50 | scan memory |
|---:|---:|---:|
| 5 000 | 776 us | 5 612 kB |
| 20 000 | 731 us | 19 900 kB |
| 100 000 | 722 us | 20 676 kB |

**7 % across a 20x change in footer size**, bought with 3.6x the scan memory. Footer parse is real
but small, and this also re-confirms 5 000 as the right row-group default: the alternative spends
most of the memory budget for almost nothing.

**What the 586 us must therefore be**, none of it yet measured individually: the partition-index
lookup that turns a key into a row ordinal, per-reader construction of the schema mapping, the
OffsetIndex read for each projected column, and then a seek plus page-header parse per column
chunk. The native format does the whole point read in 26-36 us, so **the fixed floor alone is ~19x
native**. Attributing it needs instrumentation inside the reader rather than another external
sweep, and that is the honest next step — every cheap external experiment is now spent.

### 10.4e Point-read cost is linear in leaf count — ~90 us per leaf

Measured to choose C5's ceiling. One batch, one pinned core, 1 000 random point reads per
row, and the native column measured in the *same* run as the `pq` column each time, so a row
is internally consistent even if the machine drifts between rows
(`~/pq-lab/width_curve.sh`). This method exists because an earlier wide-vs-narrow comparison
was spread across runs and the native numbers moved by the same factor as `pq`'s, which made
the ratio unreadable.

| Leaves | native p50 | `pq` p50 | Ratio | `pq` p99 | `pq` p50 per leaf |
|---:|---:|---:|---:|---:|---:|
| 10 | 26.2 us | 1 149.6 us | 43.9x | 1 560.9 us | 115 us |
| 30 | 36.3 us | 2 822.3 us | 77.7x | 3 274.0 us | 94 us |
| 60 | 56.4 us | 5 448.6 us | 96.6x | 6 460.9 us | 91 us |
| 110 | 81.1 us | 9 373.0 us | 115.6x | 10 577.7 us | 85 us |
| 200 | 136.3 us | 18 287.7 us | 134.2x | 20 016.4 us | 91 us |

**The per-leaf cost is flat at ~90 us**, so `pq` point-read latency is linear in width while
the native format's grows gently (26 -> 136 us over the same range, 5x for 20x the columns).
The mechanism follows directly: a point read must locate and decode a page in every column
chunk it projects, and there is no per-row locality to amortise that against, whereas the row
format reads one contiguous row whatever its width.

**There is no knee.** The ratio degrades smoothly from 44x at 10 leaves to 134x at 200, so no
threshold falls out of the data — any ceiling is a policy choice about acceptable absolute
latency. `max_leaf_columns` is therefore now derived from a budget rather than picked: at
~90 us per leaf, **128 leaves is a p50 of roughly 11.5 ms**, which is the default. On this
corpus it admits everything that saves meaningfully (ClickBench, 110 leaves, 40 % saved) and
excludes only Backblaze (200 leaves, 4 % saved, 134x point reads).

**What this is really standing in for.** C5 refuses on width, where the property that actually
matters is access pattern. When this was written the read path could not answer, so the width proxy
was all there was; per-table point-read and scan counters now exist (§10.14) and the proxy was kept
anyway, because nothing showed a read-mix criterion deciding better (§6.3). Refusing on width alone
is cruder — it declines a wide table that is only ever scanned, and a scan is Parquet's *best*
relative showing, though at parity rather than at the 0.82x once claimed here and 2.3x slower than
the row format through CQL until the reader stops treating a bounded range as a point read (§10.26).
Width is if anything the wrong proxy in a second way: the ~497 000 reads a scan costs today scale
with leaf count, so the wide table this refuses is the one that would gain most from the projection
pushdown §10.26 asks for. That false negative is now a deliberate choice rather
than a limitation: it errs towards leaving data in the row format, and at 134x on the worst shape
that is the error to prefer. An operator who knows better sets `storage_format = 'parquet'`.

### 10.4c Row-group size is the cheap lever — swept 2026-08-17

Review pushed back on adding caches to fix point-read latency: too much complexity, too much
resident memory, and only worth it if critical. The proposed alternative was to shrink the row
group instead, accepting a size cost. Swept it. 20 000 partitions x 5 rows, 2 000 random point
reads, byte budget lifted so the row count is what cuts.

| rows/group | point mean | point p50 | scan memory | size | write | scan |
|---:|---:|---:|---:|---:|---:|---:|
| 1 000 | **1 026 us** | 966 us | **2 156 kB** | 1 573 659 (+36 %) | 630 ms | 139 ms |
| 5 000 | 1 190 us | 1 121 us | 5 620 kB | 1 271 504 (+10 %) | 546 ms | 123 ms |
| 10 000 | 1 460 us | 1 492 us | 11 176 kB | 1 219 822 (+5.5 %) | 553 ms | 126 ms |
| 35 000 (today) | 1 967 us | 1 915 us | 20 160 kB | 1 182 530 (+2.3 %) | 605 ms | 136 ms |
| 1 000 000 | 2 508 us | 2 486 us | 21 688 kB | 1 156 110 | 606 ms | 131 ms |

**The review's instinct was right, and by more than latency alone.** Going from one row group to
1 000 rows per group gives **2.4x lower point-read latency and 10x less scan memory** for +36 %
size. Scan memory matters as much as latency here: `pq` sits at ~21 MB against the row format's
256 kB, and that is decode windows, so it falls with the row group.

**Write and scan throughput are flat across the whole sweep** (546-630 ms and 123-139 ms, noise).
So this is not a throughput trade at all -- only size against latency and memory.

**Default changed to 5 000 rows on 2026-08-18.** At 5 000 the point read is 2.1x faster and scan
memory 3.9x smaller for +10 % size; at 10 000, 1.7x and 1.9x for +5.5 %. Either is a better bargain
than caching, and neither adds a cached component or a line of new state. 5 000 was chosen because
it is the measured point rather than an interpolated one, and because going further costs
disproportionately: 1 000 rows buys another 14 % of latency for another 26 % of size.

**Confirmed against the real binary at the new default** (`sstable_parquet_perf_test`,
`PQ_PERF_POINTS=10000`, single shard, one core pinned) — the sweep above used 2 000 reads, so this
re-runs it at the corrected 10 000-read standard of 10.4b:

| Metric | old default (byte-cut, ~35 600 rows) | new default (5 000 rows) | change |
|---|---:|---:|---|
| point p50 | 1 915 us | **1 157 us** | 1.65x faster |
| point mean | 1 967 us | **1 213 us** | 1.62x faster |
| point p95 | — | 1 479 us | — |
| scan memory | 20 160 kB | **5 556 kB** | 3.6x smaller |
| file size | 1 182 530 | 1 269 816 | +7.4 % |
| scan time | 136 ms | 129 ms | 1.05x faster |
| write time | 605 ms | 614 ms | flat |

The sweep predicted p50 1 121 us, scan memory 5 620 kB and size 1 271 504 at this setting; the
binary produced 1 157 us, 5 556 kB and 1 269 816. Within 3 %, 1 % and 0.1 % — so the sweep was
measuring what it claimed to measure.

**Two secondary results worth recording.** Against the native format on the same data, `pq` is now
0.82x scan time and **0.96x write time** — <a id="scan-082"></a>~~Parquet reads a full scan *faster*
than the row format~~ and writes it no slower, at 0.318x the size. And the point-read gap narrowed
from ~62x to **38.4x mean / 41.4x p50**.

> **The 0.82x is withdrawn — see §10.26 (2026-08-20).** It is wrong in two independent ways and it
> was quoted as this format's headline advantage, so it is struck rather than left to be read. First,
> it measured `query::full_partition_range` in process, which is the path *compaction and streaming*
> take; no scan a client can issue reaches it, because the coordinator splits a range scan at tablet
> boundaries and `pq_reader::next_window()` treats any bounded range as a point read. The same table
> at the same defaults, scanned through CQL, is **2.29x** the row format — the sign flips. Second,
> even on its own path 0.82 does not reproduce: nine arms measured under §10.21's method put the
> streaming ratio at **1.02–1.13x**, i.e. parity, and §10.4's own table reported 0.95x for the same
> quantity from the same binary. 0.82 was one unreplicated pair of 121–144 ms probes with no canary.
> Read it as: **the format scans at parity on the streaming path, and the reader is 2.3x slower on
> the path CQL actually uses.** The p99 ratio is only 6.5x, because the native format's p99 (239 us) is eight
times its own p50 while Parquet's (1 551 us) is 1.3x — the columnar path is far more predictable,
it is just uniformly slower.

**A consequence worth stating:** with `rows_per_row_group` at 5 000, a row group holds about 9 MB of
shredder buffer, far under the 64 MiB budget -- so the row count becomes the operative limit and
`row_group_buffer_bytes` reverts to being purely a safety net against a pathological partition.
That is the right division of labour between the two knobs, and it is the opposite of today's
situation where the byte budget does all the cutting.

**What this does not fix.** Even at 1 000 rows per group the point read is 1 026 us, still ~35x
the native format's 29 us. Row-group size roughly halves the gap; the remaining ~1 ms is the
per-reader footer parse and schema recovery, the OffsetIndex read, and page decode (10.4b). So
caching is not made unnecessary, only much less urgent -- which is the correct order to do them
in.

### 10.4b The point-read measurement was unsound — corrected 2026-08-17

The point-read figure quoted throughout §10.4 came from **50 partitions at evenly-spaced
strides**, reported as a single mean. Two things wrong with that, both raised in review:

- **Too small a sample to describe a latency**, and reported without a distribution.
- **A stride is not a point-read pattern.** Walking indices in order gives the partition
  index and summary locality that a real random-key workload does not have.

Now: **10 000 uniformly random distinct partitions** (seeded, so it is reproducible), each read
timed individually on a fresh reader, reporting mean and percentiles. Both formats get the
identical key list. Overridable with `PQ_PERF_POINTS`.

Two runs, 20 000 partitions x 5 rows, single shard:

| Format | mean | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| default (`me`) | 28.7 / 29.5 us | 24.9 / 25.6 us | 31.4 / 32.2 us | 235 / 236 us |
| `pq` | 2 227 / 2 304 us | 2 166 / 2 177 us | 2 967 / 3 416 us | 3 354 / 3 824 us |
| ratio | **77.5x / 78.1x** | **86.9x / 85.0x** | 94x / 106x | **14.3x / 16.2x** |

**Correcting the method made `pq` look worse, not better.** The native format got *faster* with
a proper sample — 29 us against the 39 us previously reported — so the honest gap is **~78x on
the mean and ~85x at p50**, not 55x. The earlier number flattered both formats and the row
format more.

**The distribution is the new information, and it is the useful part.** `pq` is *tight*: p95 is
only 1.3-1.6x its own p50. The native format is long-tailed: its p99 is 9x its p50. So at p99
the gap narrows to **14-16x**. `pq`'s cost is a consistent fixed overhead per read — open a row
group, decompress a dictionary page — rather than variance. That matters for the fix: caching
decoded page and dictionary state attacks a constant, which is the most tractable kind of
latency problem.

**A caveat this test cannot escape, and it runs against `pq`.** The point reads execute
immediately after a full scan of the same file, so everything is warm in the page cache; the
numbers are pure CPU/decode cost. Cold, `pq` has to read **3.4x fewer bytes** (1 182 602 against
3 994 586), so I/O would move in its favour and none of that is visible here. A cold-cache
variant is the next methodology fix, and until it exists these figures should be read as
`pq`'s worst case rather than its expected one.

**Scan timing on this machine is too noisy to quote.** The same binary gave 0.97x and 1.61x on
consecutive runs, with reactor stalls logged in both. Write is stable at 1.14-1.16x and size at
0.296x; the scan ratio needs a quiet machine before it means anything.

### 10.4a Row groups, statistics and numeric dictionaries — the throughput bill, measured 2026-08-17

Re-run of `sstable_parquet_perf_test` after this session's changes (statistics collection,
numeric dictionary encoding, row-group cutting). 20 000 partitions x 5 rows.

| Path | Default (`me`) | `pq` | Ratio | Was (§10.4) |
|---|---:|---:|---:|---:|
| Write | 588 ms | 712 ms | 1.21x | 0.98x |
| Scan | 142 ms | 159 ms | 1.12x | 0.98x |
| **Point read** | **39.1 us** | **2 614 us** | **66.9x** | 55x (2 173 us) |
| Size | 3 994 586 | 1 182 602 | **0.296x** | 0.319x |
| Scan memory at 8x rows | — | — | **1.04x** | 1.13x |

Size and bounded-memory both improved; throughput and point-read latency both got worse.
**Attributed by measurement, not by reasoning** — rebuilt with the numeric-dictionary branch
disabled and re-run:

| | Point read | Size | Write | Scan |
|---|---:|---:|---:|---:|
| numeric dictionaries **on** | 2 614 us (66.9x) | 0.296x | 1.21x | 1.12x |
| numeric dictionaries **off** | 2 146 us (60.6x) | 0.308x | 1.15x | 1.04x |

So numeric dictionaries alone cost **+22 % point-read latency** and buy **−3.9 %** size here
(−10.9 % on D12). With them off, the point read is 2 146 us against the 2 173 us measured
before this session — i.e. the whole point-read regression is theirs, and the residual write
(1.15x) and scan (1.04x) cost belongs to the statistics collection and row-group cutting.

**This is the same mechanism §10.4 already identified**: a dictionary page must be decompressed
in full before a single value can be decoded, which is why the threshold was tightened from 2x
to 8x repeats in the first place. Extending dictionaries to numeric columns re-applied that
cost to many more columns.

**So the trade is now explicit and should be decided, not defaulted.** Spending 22 % of the
weakest metric to gain 4–11 % of the strongest one is a poor exchange for a table serving point
reads, and a good one for a bottom-tier analytical table that is scanned. Three ways to resolve
it, in order of preference:

1. Tune the threshold by *measured benefit per column*, which open question 9 already proposes —
   with a read-cost term added. On D12 `sky` (11 distinct) gains 26 % of its column while `temp`
   (992 distinct) gains nothing, so a stricter numeric threshold would keep most of the size and
   little of the latency.
2. Make it follow the tiering intent: the hybrid policy (§6) already knows whether a table is
   bottom-tier.
3. Cache decoded dictionary state across point reads, which is the pre-existing plan and would
   reduce the cost of *all* dictionaries rather than trading them away.

Until one of those lands, the honest statement of single-row-read latency is **2.6 ms, 67x the
native format** — and 2.1 ms / 61x with numeric dictionaries off.

### 10.6 Scale: three-way size at GB scale, under TWCS — measured 2026-08-19

Everything in §10.1–§10.4 is measured at 200–300 k rows, on a compacted single sstable, and
compares two numbers: the Zstd+dict SSTable against the `pq` file. Two things were missing.

First, **the raw denominator.** "Parquet is 47 % of the SSTable" is a ratio against a baseline
that is itself already compressed 5–10x, and it hides how much of the total win is Scylla's
compressor versus how much is the columnar layout. So every scale run now reports three sizes
from the same rows — uncompressed, `ZstdWithDictsCompressor` with a trained dictionary, and
`pq` — and the three ratios between them.

Second, **scale and strategy.** These runs use TWCS, in line with §6.4: a timestamp clustering
key gets TWCS, everything else gets ICS, and nothing uses STCS.

**Synthetic series, 6 M rows, TWCS 1-day windows** (`~/pq-lab/scale_test.py`). Five columns:
station text, ts timestamp, temp int, and two low-cardinality text columns.

One thing to know before reading the table: this generator sets the mutation timestamp to a
**monotonic counter**, not to the data timestamp, so all 6 M rows land within a few seconds of each
other and TWCS therefore holds them in a *single* window. The 1-day window setting is real but does
no work here. That is unlike the ISD run below, which sets the mutation timestamp to the observation
time and genuinely spans 13 windows. It also matters more than it looks for the Parquet number: L1
folding stores a per-row `__ts`, so 6 M distinct timestamps are a real column, where the ISD run's
shared per-hour timestamps delta-encode to almost nothing.

| Stage | Bytes | GB | Files | vs. raw |
|---|---:|---:|---:|---:|
| Uncompressed | 927 212 549 | 0.86 | 16 | 1.000 |
| Zstd + trained dict | 97 678 421 | 0.09 | 16 | **0.105** |
| Parquet (`pq`) | 29 237 010 | 0.03 | 16 | **0.032** |

`parquet / dict = 0.299`. Read carefully: this is a statement about the generator as much as
about the format. `status` has 6 distinct values and `note` has 64 across all 6 M rows, so
Parquet's dictionary reduces two of five columns to nearly nothing — tidier than production
data. It is a real `pq` file from the real writer, and it is not a corpus result.

**Real data, NOAA ISD-Lite 2023, TWCS 30-day windows** (`~/pq-lab/scale_isd.py`). The same
shape as corpus row D12 — station, hourly timestamp, eight integer measures — over 2 459 real
station-years instead of 59, with the sparsity real instrumentation has (temp absent 0.6 % of
the time, 6-hour precipitation 96 %). Monthly windows, because a year of retention at 1-day
windows is 365 buckets. Mutation timestamps are set to the observation time so the windows
line up with the data and not with the moment the backfill ran.

| Stage | Bytes | GB | Files | vs. raw |
|---|---:|---:|---:|---:|
| Uncompressed | 859 755 549 | 0.80 | 13 | 1.000 |
| Zstd + trained dict | 281 861 328 | 0.26 | 13 | **0.328** |
| Parquet (`pq`) | 121 655 884 | 0.11 | 13 | **0.142** |

16 248 967 rows, 52.9 B/row uncompressed, 13 windows, `versions=pq`.
**`parquet / dict = 0.432`.**

**This is the load-bearing number, and it validates the corpus.** D12 measured 46.8 % at 300 000
rows on 59 station-years; the same shape at **54x the rows** and 42x the stations gives 43.2 %.
The ratio did not merely survive the scale-up, it improved slightly — which is the direction to
expect, since more rows per row group means the per-column dictionaries and the delta-encoded
timestamp run amortise better.

Two things move in opposite directions and are worth separating:

- **The dictionary compressor does relatively *worse* here** than in the corpus: 0.328 of raw
  against 0.250 for D12. The table is split into 13 window sstables, each trained and compressed
  independently, and the real station population is 42x larger — so each dictionary covers more
  distinct content with the same budget.
- **Parquet's total win is 7.1x over raw** (0.142), of which the compressor accounts for 3.0x and
  the layout for the remaining 2.3x. That split is the whole point of measuring raw: quoting only
  "43 % of the SSTable" understates the absolute saving, and quoting only "7x smaller than
  uncompressed" would take credit for what Zstd already does.

**What the raw column is for.** It separates two claims that are easy to conflate. The
dictionary compressor is doing most of the absolute work — it is the reason the baseline is
hard to beat, and the reason the 2020-era Parquet-vs-Deflate comparisons no longer apply
(§0). What Parquet adds is a further multiple *on top of* an already-good compressor, from
the layout: values of one column adjacent, so a codec sees a homogeneous stream, plus
DELTA_BINARY_PACKED on the monotonic clustering timestamp and dictionary+RLE on repeated
series keys.

**Caveats on both runs.** One node, one shard-count, `tablets = {min: 1, max: 1}` so a single
compaction group holds everything. The `pq` figures come from a major compaction. Since a TWCS table is now
entirely Parquet with no per-compaction decision (§6.3), that is the same output an ordinary
per-window compaction would produce — but these runs still do not exercise a genuinely mixed
candidate set, which is the ICS case. The
disk budget is 10 GB total, which caps the load at ~3.5 GB written because the ALTER between
compressors transiently holds both the old and the new sstables.

### 10.7 A TWCS table did not converge on its own — found and fixed 2026-08-19

Making TWCS entirely Parquet (§6.3) settled *which* format each compaction writes. It did not, on its
own, make the table actually become Parquet, and the difference only showed up once something
measured automatic operation rather than an operator-triggered rewrite.

Everything before this used `keyspace_compaction` or `keyspace_upgrade_sstables` — an operator asking
for a rewrite. `~/pq-lab/twcs_converge.py` calls **no compaction endpoint at all**: it writes rows,
flushes (an 8 GB dev node never fills its memtable with a 150 MB dataset, so the flush stands in for
memtable pressure), and then only watches. 3 M rows, TWCS 1-day windows, 25 windows,
`storage_format = 'hybrid'`.

It settled at **21 native sstables against 5 Parquet — 2.7 % of bytes — and stayed there** for the
whole watch, unchanged sample after sample.

**The cause is structural, not a timing artefact.** TWCS's per-window major requires two sstables in
the window, and its size-tiered fallback requires `min_compaction_threshold` (4). A closed window
holding exactly **one** sstable is therefore terminal: there is nothing to merge, compaction
correctly never runs on it again, and it keeps whatever format it was flushed in — for ever. The five
that did convert were windows that happened to receive two or more flushes.

This is the failure mode a write-side-conversion design is prone to, and it is invisible: every
sstable is valid, every row reads, no error is logged, and a `nodetool upgradesstables` makes it all
look fine. It just never happens by itself.

**The fix is that flushes honour the same rule.** `table::make_sstable(state)` returned the preferred
*native* version unconditionally, so even a table declared `'parquet'` flushed native and relied on a
later compaction. It now consults `writes_parquet_unconditionally()`, so a table that writes Parquet
unconditionally flushes Parquet — which makes a one-sstable window already Parquet and needing no
compaction at all. It also removes a native-to-Parquet rewrite of every flush.

Note what this says about the earlier reasoning. When the TWCS rule was written, flush was left
native deliberately, on the argument that it should match what `'parquet'` already did. That argument
was self-consistent and wrong: the parity it preserved was with a behaviour that also does not
converge. The measurement is what distinguished them.

### 10.8 Multi-node: replication, repair and streaming — measured 2026-08-19

The first coverage beyond one node, which was the largest untested surface. Three nodes on distinct
loopback addresses with their own data directories and API ports (`~/pq-lab/cluster_setup.sh`),
RF=3, a TWCS table declared `'hybrid'`, 300 000 rows written at `CL=ALL`, 16 tablets.

| Check | Result |
|---|---|
| Cluster forms | 2 peers from the coordinator |
| Every replica converges to `pq` independently | n1/n2/n3 each 129 files, all `pq` |
| Count at `CL=ALL` | 300 001 (300 000 + a warm-up row) |
| Each replica reads its own `pq` data, pinned at `CL=ONE` | 300 001 on all three |
| Repair completes across `pq` replicas | 4 s |
| Rows intact after repair | 300 001 |
| Fourth node joins | yes |
| Bootstrapped node holds `pq` sstables | 96 files, all `pq` |
| Rows intact after streaming to a new node | 300 001 |

**Re-run on the latte loader, 2026-08-19: 9/9, and this time repeatably.** The first pass at 9/9 was
partly luck — the check was not idempotent, because its last step joins a fourth node and that node
*stays* joined. A second run therefore found four nodes, failed "cluster formed" (which expected two
peers), and passed the bootstrap check trivially against sstables n4 already had. It now decommissions
n4 and wipes its data first, which makes every run start from three nodes and exercises decommission
as a side effect — legal from four nodes to three, unlike the three-to-two case above. The genuine
re-bootstrap moved 96 Parquet sstables onto the wiped node.

The same re-run exposed a second harness fault of the same shape: the warm-up row became unnecessary
under latte and was deleted, but `EXPECTED = ROWS + 1` was left behind, so four checks reported
`300 000 vs 300 001` against a table that was correct. Both faults produced confident wrong verdicts
rather than errors. A test that only works the first time is worse than no test, because the second
run's green is indistinguishable from a real pass.

**What each one is for.** Independent convergence matters because if only the coordinator converted,
the format would not really be a per-node property. Repair is the one most likely to expose a
shredding or folding asymmetry — it compares digests between replicas, so a `pq` replica and a native
replica disagreeing on identical data would show up as endless streaming and mismatches that are not
real, rather than as a crash. Bootstrap makes the existing replicas *stream from* Parquet sstables,
and the new node's writes come from `streaming_version_for()`; the tablets that moved to it arrived
as 96 Parquet files, and the three original nodes dropped from 129 to 97 as they gave them up.

**Two test-design errors worth recording, because each produced a confident wrong answer.**

- **Decommission cannot work here.** The streaming test was originally a decommission of n3. RF=3 on
  a three-node cluster leaves nowhere for the third replica, so the request is accepted and then
  waits — it sat in `decommission: waiting for completion` for an hour with no error. The constraint
  is arithmetic, not a Parquet problem. Replaced with bootstrapping a fourth node, which exercises
  the same streaming path with no such constraint.
- **One tablet made the streaming test vacuous.** The table was pinned to a single tablet to keep the
  data directory simple. One tablet with RF=3 has exactly three replicas, so on four nodes the new
  node correctly receives *nothing* — "no sstables on n4" was the right answer, and it looked like a
  failure. With 16 tablets there are 48 replica placements to spread over four nodes, so joining has
  to move some.

And one measurement error: the first version read n4's directory six seconds after it joined, found
nothing, and reported a failure. Joining and receiving data are minutes apart. The check now polls.

### 10.10 The loader was the bottleneck — replaced, and validated byte-for-byte

Every scale run in §10.6 spent 85–90 % of its wall clock loading rows, and the load was **client**
bound, not server bound. Measured on this node: the Python driver loop managed 11 376 rows/s, three
concurrent copies of it managed ~31 500 rows/s in aggregate — and the node was using **54 % of one
core** the whole time. Most of Scylla was idle while a Python loop fed it.

Replaced with [latte](https://github.com/pkolaczk/latte), a Rust load generator with Rune workload
scripts (`~/pq-lab/latte/timeseries.rn`, driven through `~/pq-lab/latte_load.py`).

| Loader | Rate | Node CPU | 6 M-row run |
|---|---:|---|---:|
| Python driver, 1 process | 11 376 rows/s | ~50 % of a core | 10.6 min |
| Python driver, 3 processes | ~31 500 rows/s | 54 % of a core | — |
| **latte, prepared, `-t 4`** | **58 000–75 000 rows/s** | 114 % of a core | **3.7 min** |
| latte, unprepared | 21 910 rows/s | — | — |

Four threads is the knee: 8 gives 67 831 rows/s and 16 gives 71 870, so past 4 the node is the limit
and extra threads only add client-side scheduling. The bottleneck is now Scylla, which is where it
should be.

**And the ceiling is cells per second, not rows per second** — worth knowing before quoting any
rows/s figure at a different schema. On the 3-column synthetic table latte reaches 74 780 rows/s,
which is ~224 000 cells/s. The 10-column ISD table, loaded by six sharded Python processes, reaches
20 795 rows/s — which is ~208 000 cells/s. The two agree to within 8 % while the row rates differ by
3.6x, so a wide table costs proportionally more and a time estimate has to be made per schema. (The
sharded reader is not what limits it: a stride read of all 16.2 M lines costs 2.9 s of a 778 s
shard.)

**Validation, which is the part that matters.** A loader swap is only sound if it reproduces the
figures it replaces, on the same schema and the same binary:

| Stage | Python loader (published) | latte | Delta |
|---|---:|---:|---:|
| Uncompressed | 927 212 549 | **927 212 549** | **0.000 %** |
| Zstd + trained dict | 97 678 421 | 97 392 266 | −0.29 % |
| Parquet (`pq`) | 29 237 010 | 29 237 603 | **+0.002 %** |
| Files | 16 | 16 | — |
| `parquet / dict` | 0.299 | 0.300 | — |

Uncompressed is byte-identical and Parquet differs by 593 bytes in 29 MB. The dictionary figure
differs by 0.29 % because dictionary training is not bit-deterministic, which is the one place
run-to-run variation is expected.

**It took three corrections to get there, and each was found by comparing against a published number
rather than by reading the workload.** Recorded because the failure mode is the same every time: the
run completes, nothing errors, and the figure is simply about different data.

1. **Short payload.** The workload wrote `note-57` where the Python loader writes a ~130-character
   sentence. Result: 250 MB where the same 6 M rows had measured 927 MB — 41.6 B/row against 154.
2. **Modulo instead of noise.** `temp` was `i % 801 - 400` where the loader used a seeded PRNG. A
   modulo cycle is far more compressible than random values; `latte::hash_range(i, 801) - 400` is the
   deterministic equivalent (verified: 40 distinct values in 40 consecutive rows, no pattern).
3. **The mutation timestamp, which was the subtle one.** `scale_test.py` uses a *monotonic counter*,
   `1 700 000 000 000 000 + i` — one distinct value per row. The workload derived it from the data
   timestamp instead, giving 300 values shared across 20 000 rows each. Uncompressed size moved only
   2.1 %, because timestamps are fixed width either way, and **Parquet fell 40 %**: L1 folding stores
   a per-row `__ts`, so 6 M distinct timestamps are a real column while 300 shared ones delta-encode
   to nothing. The near-match on the uncompressed figure is exactly what made the wrong number look
   plausible. Now a `ts_mode` parameter, and the reason §10.6's synthetic run has a single TWCS
   window rather than the 13 the ISD run has.

**Two other things separated while chasing that.** Neither is a loader property:

- **File count moves the dictionary figure.** Each sstable carries its own trained dictionary, so the
  same 6 M rows measured 113 143 244 across 224 files and 101 966 702 across 14. An unpinned table
  splits tablets as it grows, so two runs of the same size can land on different counts and be
  silently incomparable. `SCALE_TABLETS` pins it.
- **Uncompressed size is insensitive to file count** — byte-identical (946 506 250) across 224-file
  and 14-file runs of the same data, which is also what proves latte's output is deterministic.

**Where latte is not used, and why.** The NOAA ISD load stays on the Python driver, sharded six ways
(`~/pq-lab/isd_shard_load.py`) — 20 795 rows/s aggregate, **13 min against 30**. That is 2.3x rather
than the 3x a 3-column test predicted, and the cells/s ceiling above is why: at ten columns the same
server work buys fewer rows. Each shard reads the flat TSV that `extract_isd.py` produces and takes
every Nth line, which costs 2.9 s of a 778 s shard, so the sharding is not what limits it.

The reasons latte cannot do this one:

- **latte 0.29 cannot bind a true NULL.** Rune `None` maps to `CqlValue::Empty`, a *live zero-length
  cell* — verified on this node, a row written that way reports a `writetime()` where a Python `None`
  bind reports none, because binding NULL writes a deletion. The two read identically through CQL and
  differ in the sstable, which is what a size measurement and Parquet's definition levels are
  sensitive to. ISD's realistic sparsity *is* those tombstones (0.6 % of temperatures, 96 % of
  six-hour precipitation). A literal `null` in unprepared CQL does reproduce it, at 21 910 rows/s.
- **A latte workload cannot hold the corpus.** `fs::read_lines` is rejected in a const context and
  `Context` exposes no field a script may set, so there is nowhere to keep 16 M parsed rows between
  cycles.

`harness.py`, the eight-dataset real corpus, also stays on the Python driver: it feeds arbitrary real
schemas from Parquet files including collections and counters, its loads are 200–300 k rows against a
run dominated by compaction and dictionary retraining, and its figures are published. High effort,
low payoff, real risk.

### 10.11 The dictionary baseline was never controlled — found 2026-08-19

Every "Parquet is X % of the SSTable" figure in this document divides by the size of the same table
compressed with `ZstdWithDicts` and a trained dictionary. That denominator turns out to have been
measured under a dictionary that was free to change *during the measurement*, and it moves by up to
12 %.

**How it surfaced.** Two runs over the same 16 248 967 NOAA ISD rows, same schema, same binary:

| | first run | re-run | delta |
|---|---:|---:|---:|
| Uncompressed | 859 755 549 | 858 066 418 | −0.20 % |
| **Zstd + trained dict** | **281 861 328** | **316 320 823** | **+12.2 %** |
| Parquet | 121 655 884 | 126 861 466 | +4.3 % |

The uncompressed figures agreeing to 0.2 % rules out the loader and the data. Only the compressed
size moved.

**The mechanism: `sstable_dict_autotrainer`.** Scylla trains dictionaries on a background tick and
publishes a new one whenever a validation sample says it beats the current one. The production
default tick is **900 s**. The lab node was configured at **10 s**, with a 30 s retrain period — 90x
more aggressive. On an 858 MB table a dictionary phase takes 2–4 minutes, so **12–24 ticks** can fire
inside one measurement, each training on a different 16 MB sample of the *current* sstables, and
whichever dictionary is live when the final major compaction runs determines the bytes on disk. The
harness never controlled which one that was.

**What was ruled out first, and why the wrong answer was convincing.** The obvious suspect was sample
variance in training, since `do_sample_sstables(id, 4096, 4096)` samples 16 MB regardless of table
size. Measured directly (`~/pq-lab/dict_variance.py`): five explicit retrains over identical data
spread **0.65 %** — 46 771 304 to 47 076 267. Retraining is stable, so the hypothesis looked dead.
It was the *table size* that made it look dead: that table was 67 MB and each round took ~14 s, so
almost no autotrainer ticks intervened. Small table, stable number; large table, minutes per phase,
dictionary churn. (`rest_retrain_dict` is also synchronous — it samples, trains and publishes before
returning — so there is no race with the explicit call either.)

**Which figure is right: not the published one.** Re-measured three times on the already-loaded data,
so no reload and no loader involvement:

| | bytes | vs first run | vs re-run |
|---|---:|---:|---:|
| carried dictionary | 316 260 073 | +12.2 % | −0.0 % |
| after a retrain | 316 009 740 | +12.1 % | −0.1 % |
| after a second retrain | 301 392 811 | +6.9 % | −4.7 % |

The re-run's 316 M reproduces to within 0.1 %. The first run's 281 M has not reproduced once, so it
is the outlier — and a retrain alone still moved the figure 4.7 % on unchanged data.

**Fixed for measurement:** the lab node now sets the autotrainer tick and retrain period to 86 400 s,
so only an explicit `retrain_dict` changes the dictionary. `~/pq-lab/node/conf/scylla.yaml` records
why, and `scylla.yaml.bak` keeps the old values.

**And with that in place the baseline is both stable and lower than anything measured before.** Same
16.2 M rows, same data on disk, autotrainer off:

| | bytes | vs the published 281 861 328 |
|---|---:|---:|
| carried dictionary | 285 877 971 | +1.4 % |
| after one explicit retrain | 272 183 098 | −3.4 % |
| after a second retrain | 272 874 557 | −3.2 % |

Two consecutive retrains agree to **0.25 %**, so the procedure is now deterministic — and it converges
on ~272.5 MB, **3.3 % below the published figure and 14 % below the churned one**. That direction
matters: a properly trained dictionary is a *stronger* adversary than any earlier run credited it
with, so the autotrainer churn had been flattering Parquet rather than penalising it.

**The Parquet side had to be re-measured too, and it moved as well.** Under the same deterministic
config the `pq` file is **116 495 610** bytes — byte-identical across two independent rewrites
(+0.00 %), and 4.2 % below the first run's 121 655 884, 8.2 % below the churned run's 126 861 466. So
Parquet's absolute size was also drifting between runs, for reasons that have nothing to do with
dictionaries: it tracks the internal layout the inputs happened to have. Two consecutive rewrites now
reproduce exactly, which is the fixed point.

Deterministic ISD figures, all three measured with the autotrainer off:

| | value | previously claimed | move |
|---|---:|---:|---:|
| raw | 858 066 418 | 859 755 549 | −0.2 % |
| dict | 272 528 828 | 281 861 328 | **−3.3 %** |
| parquet | 116 495 610 | 121 655 884 | **−4.2 %** |
| `dict / raw` | **0.318** | 0.328 | |
| `parquet / raw` | **0.136** | 0.142 | |
| **`parquet / dict`** | **0.427** | 0.432 | −1.2 % |

**And the headline ratio survives.** Both the numerator and the denominator were inflated by similar
factors, so `parquet / dict` lands at 0.427 against the 0.432 this document carried — a 1.2 %
relative move, inside the noise. The conclusion "Parquet stores this table in ~43 % of a
dictionary-compressed SSTable" stands.

**A mistake worth recording, because it is the same one this project keeps making.** The first
recomputation here paired the *new* deterministic dictionary figure with the *old* churned Parquet
figure and produced 0.466 — a claimed 3.4-point regression that did not exist. Both sides of a ratio
have to come from the same configuration. Mixing a re-measured denominator with a stale numerator
manufactures a result, and it is exactly the error class as the untrained-dictionary baseline and the
mutation-timestamp payload bug: a number that looks plausible because only one half of it was
checked.

**Consequences, stated plainly.**

- **`parquet / dict` ratios carry an error bar of several percent** that was never quoted, and the
  ISD headline of 0.432 is better read as ~0.40 with a denominator that was measured low.
- **Two corpus rows are close enough to parity that this changes the conclusion, not the precision:**
  Backblaze at 95.8 % and Wikipedia pageviews at 89.6 %. A 12 % move in the denominator is the
  difference between Parquet winning slightly and losing slightly on those shapes.
- **Uncompressed and Parquet sizes are unaffected.** Both are insensitive to the dictionary, and both
  reproduce across runs — uncompressed to 0.2 %, Parquet to 4.3 %. So `parquet / raw` and every
  absolute Parquet figure stand.
- The §10.1 corpus table needs re-measuring under the deterministic config before its ratios are
  quoted again. That is now the highest-value open measurement in the project, ahead of the
  production-scale read-path work.

### 10.12 Maintenance tooling and object storage — the last two untested surfaces, 2026-08-19

Phase 5 listed "backup/restore/scrub/tooling completion" and object-storage integration, and neither
had been exercised at all. Both are now covered, and object storage turned out to have been written
off for the wrong reason.

**Maintenance tooling on a `pq` table: 10/10** (`~/pq-lab/pq_tooling_check.py`, 200 000 rows). These
are not exotic paths — they are what an operator runs on a Tuesday — and each one reads or rewrites
every sstable, so a reader that is wrong in a way ordinary queries do not reach would surface here.

| Operation | Result |
|---|---|
| `scrub` VALIDATE | 200 000 rows, versions `{pq: 1}` |
| `scrub` SKIP / SEGREGATE / ABORT | all pass, row count and format unchanged |
| `cleanup` | passes, stays `pq` |
| `snapshot` | contains `pq-` files |
| `truncate` | empties the table |
| `refresh` (load-and-stream) from that snapshot | all 200 000 rows back |

`scrub VALIDATE` is the one that carries weight: it reads every sstable and reports errors without
rewriting, so it is a whole-file integrity check of the Parquet reader. The snapshot round trip is the
backup story end to end — snapshot, truncate to zero, stage the components into `upload/`, refresh,
everything back.

**Object storage: 7/7** (`~/pq-lab/objstore_check.py`, minio). Recorded as blocked on Docker because
the repo's GCS tests cannot pull their image on this machine. That was wrong twice over: **`minio` is
installed as a plain binary**, so no registry is involved, and `podman` works even though `docker`
does not. The configuration shape is taken from the repo's own harness
(`test/pylib/minio_server.py`) rather than invented: endpoints in `object_storage_endpoints`,
credentials from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, and a keyspace opting in with
`STORAGE = {'type':'S3','bucket':...,'endpoint':...}`.

| Check | Result |
|---|---|
| Keyspace on S3 storage created | pass |
| `pq` table created on it | pass |
| sstable objects land in the bucket | 80 objects, 8 of them `Data.db` |
| All rows readable from object storage | 200 000 / 200 000 |
| Row content intact | pass |

And the decisive one, checked directly against the bucket rather than through Scylla: the `Data.db`
object carries **`PAR1` at both head and tail** — a genuine Parquet file, 114 467 bytes, with a TOC
listing all nine components. What was in question was never the encoding, since the writer does not
know where its bytes land; it was every path that assumes a local file — the storage layer,
`data_read()` fetching a footer over HTTP, and the fact that S3 objects cannot be renamed
(`sstables/storage.cc` refuses to change the state and generation of one), which is how the local
path moves sstables out of `upload/`.

**One thing this exposes, and it is now traced rather than left open.** On object storage the key is
`sstables/<uuid>/Data.db` — the sstable *version is not in the object name*, where locally it is the
`pq-` filename prefix. §10.9's downgrade safety rests on that prefix, so the obvious worry was that
object storage has no equivalent and an older node might silently mis-read rather than refuse.

Traced through the code, the outcome is the same by a different route. The version lives in the
`version` column of `system.sstables_registry`, and `system_keyspace::sstables_registry_list()` parses
it with the **same** `sstables::version_from_string()` that the filename path uses — which throws
`std::out_of_range("Unknown sstable version")` rather than returning a default. That throw is raised
inside the registry scan during `table_populator::start()`, and `distributed_loader` logs the failure
and **rethrows everything except `compaction_stopped_exception`**, so population fails rather than
continuing with a partial file list. An older node should therefore refuse to start on either storage
type.

**Read, not observed, and the distinction matters.** The local case was verified by planting a file
with an unknown version prefix and watching the node exit. The object-storage case cannot be provoked
the same way: the registry is not reachable through CQL (`unconfigured table sstables_registry`), so
there is nowhere to plant a bogus version without writing to a system table directly.
`sstable_parquet_test/test_unknown_sstable_version_is_rejected` now pins the shared primitive — every
known version round-trips, and `zz`, `pqq`, `p`, `""` and `PQ` all throw — because if that ever
started returning a default instead of throwing, **both** paths would degrade from "refuses to start"
to "silently mis-reads", which is the difference between a safe downgrade and data loss.

What would settle it end to end: an error-injection point in the registry scan, or a build with `pq`
removed from `version_string` reading a bucket written by this one. Until then §10.9's procedure is
observed for local storage and reasoned for object storage.

### 10.13 DELTA_BYTE_ARRAY — implemented 2026-08-19, and it found a decoder bug

`DELTA_BYTE_ARRAY` was the last unimplemented encoding in the format-gaps list. Text columns fell back
to a dictionary or to PLAIN, and PLAIN spends a fixed four bytes per value on lengths before the codec
sees anything.

**What it is.** Front coding: each value records how many leading bytes it shares with the value
before it, then only its own suffix. The page body is three concatenated streams — prefix lengths
(DELTA_BINARY_PACKED), suffix lengths (DELTA_BINARY_PACKED), suffix bytes — each of which compresses
well independently. `DELTA_LENGTH_BYTE_ARRAY` is implemented too, since it *is* the second half of
that layout and is useful alone for values with no shared prefixes but similar lengths.

**Measured, and the only figure that counts is the compressed one**
(`parquet_writer_test/test_delta_byte_array_after_compression`). The encoder-level numbers are
reported alongside because the gap between the two columns is the whole point — §10.3f's failure was
an encoding that looked obviously right before zstd and cost 55 % after it.

| input | raw vs PLAIN | **zstd vs PLAIN** |
|---|---:|---:|
| sorted keys with a long shared prefix | 35.0 % | **5.7 %** |
| monotonic numeric ids | 17.2 % | **7.2 %** |
| **real GitHub `event_id`, in clustering order** | 37.8 % | **73.5 %** |
| sorted UUID-like keys (minimal sharing) | 78.1 % | **85.7 %** |
| *unsorted* random strings | 70.7 % | **121.8 % — loses** |

**The last row corrects a claim this section previously made.** "The downside is bounded because it
still beats PLAIN" was measured before compression and is false after it: on unsorted input front
coding is **22 % worse** than PLAIN, because splitting values into prefix and suffix streams destroys
the byte patterns zstd was already exploiting. That is the same mechanism that sank BYTE_STREAM_SPLIT
on doubles, and it means the encoding is not safe to apply on the strength of a type alone.

**It is safe on the strength of order, which is why the hint is restricted to clustering keys.** Every
sorted case above wins after compression, including the adversarial one — sorted UUID-like values
share only one or two characters and still come out at 85.7 %. The single losing case is unsorted
input, and a clustering key is sorted by construction.

That also settles a decision made earlier on structural grounds: text *partition* keys were excluded
because token order is not key order. Measured, that exclusion is worth about 22 % on such a column —
applying the hint there would have made those files bigger.

**Where it is applied, and where it deliberately is not.** §10.3f is the relevant warning: type-based
encoding rules were tried on real data and *lost* — BYTE_STREAM_SPLIT on doubles cost 55 %,
DELTA_BINARY_PACKED on bigints cost 0.3 %. So this is hinted only where the premise is **structural**
rather than a guess about what a type usually contains: a **text or blob clustering key**. Rows arrive
in clustering order, so within a partition those values are sorted and adjacent ones share leading
bytes by construction.

It is **not** applied to a text partition key. Partitions are stored in token order, which is not key
order, so consecutive values share nothing systematic and any gain would be luck — exactly the shape
of the rules that lost. And the dictionary still takes precedence wherever values repeat, because it
stores each distinct value once where this stores every occurrence; the writer's repeat-ratio check
runs first. This encoding is for what a dictionary handles badly: a key with many distinct values that
happen to be ordered.

**It uncovered a pre-existing decoder bug.** The round trip failed with `bad delta header`, and the
fault was not in the new code. The spec's DELTA_BINARY_PACKED header has four fields — block size,
miniblocks per block, total count, **first value** — but `decode_delta_binary_packed()`'s `total == 0`
early return bailed out before consuming the first value. That was harmless for as long as a delta
block was always an entire page body: the stray varint was simply never read. `DELTA_BYTE_ARRAY` is the
first encoding here that concatenates two delta blocks with no length prefix between them, so the
second block began parsing at the leftover byte. A new encoding finding a latent bug in an old one is
the argument for implementing the format's encodings rather than only the ones currently needed.

**And the interop suite could not have caught any of this.** All eight of its shapes used `ck int`, so
nothing in it had a text clustering key and the whole path was invisible to external readers — the same
blind spot that let the MAP annotation bug through, where seven flat fixtures meant "pyarrow reads our
files" was true and irrelevant. Added `text_clustering_key` and `blob_clustering_key` shapes, and made
the suite **report the encodings each file actually uses**, because a fixture that passes without
exercising the new path is worse than no fixture: it looks like coverage.

**Interop: 18/18, and the new encoding is demonstrably reached.**

| shape | pyarrow | duckdb | encodings in the file |
|---|---|---|---|
| `text_clustering_key` | 120 rows | 120 rows | `DELTA_BYTE_ARRAY`, PLAIN, RLE_DICTIONARY |
| `blob_clustering_key` | 120 rows | 120 rows | `DELTA_BYTE_ARRAY`, PLAIN, RLE_DICTIONARY |
| the other 16 shapes | 120 rows each | 120 rows each | PLAIN, RLE_DICTIONARY |

Two independent implementations read it — pyarrow wraps parquet-cpp, DuckDB has its own reader, so a
claim of "readable by other tools" backed by one library is really a claim about one library. And
`DELTA_BYTE_ARRAY` appears in exactly the two new shapes and nowhere else, which is what makes this
evidence rather than a green tick: the encoding column shows the path was taken, not merely that a file
parsed.

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
| 2026-08-16 | `storage_format` is `std::optional` in the schema: "never set" is distinct from "set to sstable" | The earlier unconditional cell made revert work but gave *every* table a new schema cell, changing the digest on upgrade. `tablet_options` already solves this with `has_tablet_options()`; mirroring it means a table that never mentioned the property writes no cell, while an explicit revert to `'sstable'` still writes one and takes effect |
| 2026-08-16 | Setting a non-default `storage_format` requires the `PARQUET_SSTABLE_FORMAT` cluster feature | Until every node understands the property, a node that does not would keep writing the native format while others did not. Validated in `cf_prop_defs`, so the error arrives at DDL time with a clear message |
| 2026-08-16 | Verifying the dictionary baseline is a required step of the measurement protocol, not an assumption | Setting `ZstdWithDictsCompressor` does not make files dictionary-compressed; two separate measurements were ~2× and ~7× wrong before this was noticed (Trap 4). Every comparison must retrain, rewrite, confirm the baseline moved, use `ondisk_data_size()`, and cross-check against an existing §10 number |
| 2026-08-16 | Regular columns default to PLAIN; type-based encoding rules rejected | Measured, not assumed: byte-stream-split on doubles cost +54.9 % and delta on bigints +0.3 % on real data (§10.3f). Both destroy the exact-value repetition zstd was exploiting. Delta stays only where monotonicity is structural — key columns and `__ts` |
| 2026-08-16 | V2 page decode honours the page's own `is_compressed` flag, not just the chunk codec | Found by first pointing our reader at parquet-cpp files: it clears the flag when compression does not pay, and decoding those pages with the chunk codec fails outright. Our own files never exercised it |
| 2026-08-16 | Snappy added to the read path | Extremely common in third-party files, and Scylla already links snappy — the cost was a dozen lines |
| 2026-08-16 | `sstable_version_types::pq` is defined but deliberately left out of `all_sstable_versions` and `writable_sstable_versions` | Those arrays mean "versions the node can read / write". Adding `pq` before the writer exists made `sstable_test` look for a `pq` fixture and fail — a useful signal that the arrays are a support claim, not a list of enum values. It goes in with the Data-component writer and `pq::make_reader` |
| 2026-08-16 | The tiering policy is a pure function; a separate `tiering_context` fills its inputs | Keeps every criterion unit-testable without a compaction manager or a schema, and forces each criterion to be expressible as a number the caller supplies — a criterion that cannot be does not belong in the policy |
| 2026-08-16 | `decide_output_format` checks the table's `storage_format` before any criterion | Opting in is permission to convert, not an instruction: a table set to `parquet` still has to clear the size and gain gates, so a 4 KiB flush is never converted |
| 2026-08-16 | The Parquet re-encoder ships as a `scylla sstable parquet-export` operation rather than a hook in the write path | It needs no access to sstable internals, produces the §10.3d measurements from our own writer, and is the natural home for the C6 estimator and the eventual export path. A temporary dual-write hook would have needed friend declarations for something that gets deleted later |
| 2026-08-16 | **Format is a per-table schema property, not per-tablet.** Reverses the earlier per-tablet design | A tablet is an internal distribution unit, not something users reason about, and an SSTable's encoding has nothing to do with which tablet its data is in. Doing it at table level removes the `tablet_info` field, the `system.tablets` column and all the group0 plumbing — schema properties already replicate, so the mechanism exists. Mirrors how `compression` already works (§6.1) |
| 2026-08-16 | Fixed-width scalars are decoded straight from cell bytes (big-endian) rather than via `deserialize()` | Avoids a `data_value` round-trip per cell on the write path; unmapped types keep their serialised form as opaque `BYTE_ARRAY`, which is lossless but forgoes type-specific encoding |
| 2026-08-16 | The Parquet image is handed to a sink, not written to the Data component yet | Keeps the whole fragment→Parquet path drivable from a unit test without constructing an sstable; component plumbing is a separate, smaller step |
| 2026-08-16 | The writer emits V2 data pages only | Keeps definition levels outside the compressed body, so a reader can skip nulls and locate rows without invoking a codec — the property the level-decode test exercises |

---

### 10.16 The corpus re-measured under the deterministic dictionary — 2026-08-20

The open question was whether §10.1f-prod's eight-dataset table survives the dictionary being made
deterministic (§10.11). It was flagged because two rows sit close to parity -- Wikipedia pageviews
at 89.6 % and Backblaze at 95.8 % -- where a few percent decides whether Parquet wins at all.

Same harness, same row counts, autotrainer disabled, one explicit retrain per table.

| Dataset | Input identical? | Native Δ | Parquet Δ | Ratio |
|---|---|---:|---:|---|
| ClickBench | yes | +0.04 % | 0.00 % | 59.8 → 59.8 |
| NYC TLC | yes | +0.02 % | 0.00 % | 56.8 → 56.8 |
| GitHub Archive | yes | −0.52 % | −0.54 % | 68.3 → 68.3 |
| HackerNews | yes | +0.03 % | 0.00 % | 82.3 → 82.3 |
| Wikipedia pageviews | yes | −0.20 % | 0.00 % | 89.6 → 89.8 |
| Backblaze | yes | −0.32 % | −0.41 % | 260.1 → 259.8 |
| NOAA ISD-Lite | **no** | −12.40 % | −15.11 % | 51.0 → 49.4 |
| NOAA ISD-Lite, 3 cols | **no** | −11.05 % | −15.25 % | 33.9 → 32.3 |

**The answer is no material change.** Where the input is identical, nothing moved: six datasets
within ±0.54 % on both formats, and every ratio stable to 0.2 points. The two near-parity rows are
the point of the exercise and they are the two that moved least -- pageviews' Parquet output is
byte-identical.

**The two ISD rows moved because their input changed, not the dictionary**, and the mechanism is
worth recording because it is a trap in any file-backed corpus. `_load_isd()` reads
`sorted(glob("*.gz"))` and stops at `nrows`, so the sample is the earliest-sorting stations. The ISD
scale test (§10.6) downloaded 2 459 station-years, and **38 of the 40 earliest-sorting files
post-date the previous corpus run** -- so "the first 300 000 rows" is a different set of stations
than it was. `raw_bytes` moving (14 801 542 → 14 085 218) is what gives it away; a dictionary change
cannot alter the uncompressed input. Their *ratios* moved 1.6 points, which is a mild independent
result: the ISD shape holds up under a different station sample.

So the deterministic baseline costs nothing in published figures, and §10.1f-prod stands as written.
**This closes the corpus re-measurement gap.** What it does not close is Backblaze's absolute
numbers: both batch runs land in the anomalous regime (§10.17), so that row's 95.8 % rests on the
standalone runs, not on these.

### 10.18 The Backblaze 4x anomaly, localised — 2026-08-20

Not solved, but no longer "cause unknown". Two claims in the previous write-up were wrong and the
mechanism is now identified.

**It is not batch-only.** §10.1f said the anomaly "has appeared only inside full-corpus batches and
never in isolation". `out/bbv_A.tsv` is a standalone run with native 32 472 881 and pq 85 683 469 --
so it happens in isolation too. Measured rate under one binary with the autotrainer disabled: **1 in
7 standalone passes**, against **2 of 2 batch runs**. Batch context makes it much more likely
without being necessary.

**The two regimes are each internally deterministic**, which is what makes the split so odd:

| | native | parquet | ratio | row groups | rows/group | synthetic `__*` leaves |
|---|---:|---:|---:|---:|---:|---:|
| normal (6 of 7 passes) | ~21.8 M | **20 803 872** (byte-identical every time) | 95 % | 60 | 5 000 | 2 695 885 (14.9 %) |
| anomalous (1 of 7) | ~32.3 M | 84 196 292 | 261 % | 163 | 1 840 | **60 102 010 (78.4 %)** |

**The mechanism is the per-cell metadata channel, not row-group overhead.** That was the obvious
hypothesis -- 604 leaves at ~225 B of metadata per leaf per row group would explain 84 MB at ~600
row groups -- and it is wrong. The file has 163 row groups, and the growth is in the data: all **407
synthetic leaves** grow together, from ~6.6 kB each to ~148 kB each, 22x. `__ts` itself barely moves
(2.01 M → 1.89 M). The row groups are cut *earlier* because the rows are bigger, so that is a
consequence rather than the cause.

Those 407 leaves are the deletion and timestamp-exception channels for 195 mostly-null columns. The
dataset is bound with every column present and NULL for a missing reading, which writes a cell
tombstone per null -- roughly 58 M of them across 300 000 rows. At about a byte each that is
~58 MB, which is the 60 MB observed almost exactly. So the anomalous file is the one that **stores
the tombstone metadata**, and the normal one is the one where it has collapsed. Native moving with
it (21.8 → 32.3 M) fits: the row format stores the same tombstones.

**The channel is now named, from the anomalous file itself** (caught on the 6th of 7 passes and
kept: `out/bb_anomalous_pass6.parquet`). It is not the timestamp-exception channel, as the
size arithmetic suggested — it is `__ldt_<column>`, **local deletion time**:

| leaf | bytes | values | nulls |
|---|---:|---:|---:|
| `__ts` | 1 893 064 | 300 000 | 0 |
| `__ldt_smart_27_raw` | 394 255 | 300 000 | 4 818 |
| `__ldt_smart_189_raw` | 393 854 | 300 000 | 23 872 |
| ~390 more `__ldt_*` | ~390 kB each | 300 000 | varies |

`__ts` barely moves between the two regimes; the `__ldt_*` leaves are the entire 60 MB. Each holds a
deletion time for ~295 000 of 300 000 rows, and the null counts differ per column in step with how
often that column actually has a reading. So this is exactly what binding every column with NULL for
a missing value produces: **a cell tombstone per null**, ~58 M of them, and the anomalous file is the
one that stores them.

**And the obvious explanation is wrong.** The natural guess was purge eligibility: the mutation
timestamps are back-dated to 2023 and `gc_grace_seconds` is the default 10 days, so the tombstones
would be droppable and a compaction might or might not drop them. Read out of the file directly, the
write timestamps *are* 2023 — but `local_deletion_time` is **wall-clock at write time**, minutes old:

```
__ts   1700346220936144  ->  2023-11-18T22:23:40Z   (the USING TIMESTAMP)
__ldt  1787184561        ->  2026-08-20T00:09:21Z   (wall clock, ~7 minutes before the read)
```

At 0 days against a 10-day grace these tombstones are **not purgeable in either regime**, so
compaction dropping them cannot be what distinguishes the runs. That eliminates the first hypothesis
rather than confirming it, which is worth more than it sounds: it moves the question from the read
side to the write side. If the normal runs have no tombstones *at all* — and their `__ldt_*` leaves
totalling ~0.7 MB across ~400 leaves is what all-null looks like — then the difference is whether
those NULLs were bound as deletions in the first place, not whether they were later removed.

**The shadowing hypothesis is eliminated too.** A duplicate row inserted twice under different
`USING TIMESTAMP` values would let a later live cell shadow an earlier tombstone, and the harness
does log collapse counts for other datasets (pageviews: 176 082 rows to 163 845 distinct keys). The
Backblaze slice has **no duplicates at all**: 300 000 rows, 300 000 distinct `(serial_number, date)`
pairs.

**The arithmetic checks out exactly, which is worth stating because it closes the size question.**
The slice holds **42 448 175 nulls** across 195 data columns — 141.5 per row. Bound as NULLs that is
42.4 M cell tombstones, and at ~1.4 B each after zstd that is ~60 MB: the anomalous file's 60.1 MB.
So there is no missing mechanism on the size side. Every run writes those tombstones, and the
question is entirely what removes them in the other six passes out of seven.

**The absent-versus-cheap question is now settled, on the files rather than on an aggregate**
(`bb_compare_ldt.py`, comparing a kept normal file against the kept anomalous one):

| | `__ldt_*` leaves | values | non-null | bytes |
|---|---:|---:|---:|---:|
| normal | 195 | 58 500 000 | **0 (0.00 %)** | 327 600 (1 680 B per leaf) |
| anomalous | 195 | 58 500 000 | **42 448 175 (72.56 %)** | 57 327 020 |

1 680 bytes per leaf is what an all-null column costs: definition levels and nothing else. And
42 448 175 is *exactly* the null count of the source slice — so the anomalous file carries precisely
one tombstone per source null and the normal file carries none. The data columns are identical in
both, same per-column null counts and sizes to within 0.1 %, so the `__ldt_*` channel is the only
difference between a 20.8 MB file and an 84.3 MB one.

**So the tombstones are genuinely absent in six runs out of seven, and the cause is upstream of the
Parquet writer.** Two things follow. The Parquet side is exonerated: it faithfully records what it is
given, and `pq_bytes` is byte-identical (20 803 872) across all six normal passes, which is what
identical input looks like. And the native sstable moves with it — 21.9 MB normal against 32.3 MB
anomalous — so the divergence already exists *before* the format conversion.

**What remains genuinely puzzling, stated as such.** Every explanation checked is eliminated: not
gc_grace (the deletion times are wall-clock and minutes old against a 10-day grace), not shadowing
(no duplicate keys), not the load (identical input, identical statements, identical `USING TIMESTAMP`
per row). Yet a bind-NULL insert must write a dead cell, and in most runs no dead cell survives to
the first measurable sstable.

**The write path is exonerated too: it emits the tombstones every time.** Measured as tightly as it
can be — table created with `storage_format='parquet'` and **auto-compaction disabled**, so the first
memtable flush is itself a pq file with nothing between the write path and the bytes read back
(`bb_writepath.py`):

| pass | pq files | `__ldt` values | non-null | populated |
|---|---:|---:|---:|---:|
| 1 | 34 | 58 500 000 | 42 448 175 | 72.56 % |
| 2 | 33 | 58 500 000 | 42 448 175 | 72.56 % |
| 3 | 33 | 58 500 000 | 42 448 175 | 72.56 % |

Identical to the digit across three passes, and identical to the anomalous corpus file. So the load
is not the variable, and neither is the Parquet writer. **Something downstream removes ~42.4 M cell
tombstones in six corpus runs out of seven** — one of the harness's five codec-sweep rewrites, or the
conversion compaction.

Disabling auto-compaction was not a convenience: ICS began compacting the instant the flush landed,
which both raced the read and would have made the measured file something other than what the write
path produced. The first attempt at this experiment died on a half-written footer, which is the only
reason the race was noticed at all.

**Whether that removal is legitimate is deliberately left open here.** Their `local_deletion_time` is
wall-clock and minutes old against a 10-day `gc_grace`, so the timeout rule should not make them
droppable — but a cell tombstone that shadows nothing may be droppable under rules this project has
not verified, and asserting a Scylla-wide correctness claim on the strength of a size measurement
would be exactly the overreach the rest of this section avoids.

**The next step is a bisect, not another hypothesis, and the instrument exists.** Native sstable size
cannot be the signal — the codec sweep moves it from 242 MB to 32 MB, which buries a ~10 MB tombstone
effect. But the sstable Statistics carry `estimated_tombstone_drop_time`
(`sstables/types.hh`), and the in-tree tool reads it without needing a running node or a schema file,
autodetecting the schema from the sstable's own path:

```bash
scylla sstable dump-statistics <path>/me-...-Data.db   # -> stats.estimated_tombstone_drop_time
```

Empty means no tombstones; populated means they are there. Verified against a table known to have
none, which returns `{}`. So the bisect is six invocations — after the flush and after each of the
five codec rewrites — and it names the compaction that drops the tombstones without any further
guesswork. From there the question becomes a specific one about that compaction's purge decision.

If it turns out they are droppable by design, the anomaly is fully explained and Backblaze's corpus
figure should be taken from a run with `tombstone_gc` pinned so it stops varying between runs.

**The bisect ran, and it eliminated more than it identified — including my own reimplementation.**
`bb_bisect.py` walks flush → none → lz4 → zstd → lz4_dict → zstd_dict → parquet, measuring
`estimated_tombstone_drop_time` after each step. The calibration gate passed exactly: the first
flush reports **42 448 175 histogram points**, the precise tombstone count, so the probe provably
counts dead atomic cells and every later reading means something.

Then all **five** runs kept every tombstone at every stage:

| stage | bytes | tombstones |
|---|---:|---:|
| flush | 66 682 357 | 42 448 175 |
| none | 242 357 702 | 42 448 175 |
| lz4 | 67 781 169 | 42 448 175 |
| zstd | 49 003 336 | 42 448 175 |
| lz4_dict | 52 507 015 | 42 448 175 |
| zstd_dict | 32 234 454 | 42 448 175 |
| parquet | 85 986 241 | 42 448 175 (72.56 % of `__ldt`) |

Five anomalous runs in a row, against the harness's rate of roughly one in seven, is about a
1-in-17 000 coincidence — so this is systematic, not luck. **The removal is therefore not in the
load, not in the codec rewrites, and not in the conversion**, because this pipeline performs all of
those and never loses a tombstone.

**What it is, is a difference between the harness and my reimplementation of it**, and that is the
lesson worth keeping. The harness does considerably more compaction than the bisect copied:
`flush_and_compact()` before each codec, `rewrite(); flush_and_compact()` **twice** for the two
dictionary codecs (the second pass exists so the freshly trained dictionary is actually used), and a
full-table read afterwards — roughly twelve major compactions per run against the bisect's five.

So the next step is not another hypothesis, and not a better reimplementation: **instrument the
harness itself**, probing tombstone counts between its real stages. Reimplementing a pipeline in
order to bisect it introduced the very divergence that made the bisect inconclusive, which is a
cheaper lesson here than it usually is.

**No published figure depends on this.** Backblaze's row in §10.1f-prod comes from the standalone
runs that land in the normal regime, and §10.16 excluded its absolute numbers from the corpus
re-measurement for exactly this reason.

### 10.19 Mixed-format bucketing at scale — measured 2026-08-20, and it is only half an answer

The fix (§10.3i) makes a size-tiered candidate set holding both formats bucket on
`ondisk_data_size()` instead of `data_size()`, because the latter means two different quantities:
uncompressed length for a compressed native sstable, file size for a `pq` one. It is unit-tested.
What the unit test cannot show is the consequence the fix exists to prevent -- a hybrid table
settling into repeatedly rewriting the format that is most expensive to rewrite -- so this measures
write amplification per format on a live ICS table under `'hybrid'`.

4 000 000 rows, latte, ICS, Zstd-with-dicts, no major compaction issued (the point is what the
strategy chooses). The table directory was polled throughout; a generation that appears and later
disappears was rewritten.

| format | appeared | rewritten | bytes seen | bytes rewritten |
|---|---:|---:|---:|---:|
| `me` | 18 | 6 | 51 952 131 | 27 040 009 |
| `pq` | 3 | 0 | 3 465 766 | 0 |

Live at the end: 15 sstables, 12 `me` and 3 `pq`. All 4 000 000 rows read back.

**What this establishes.** Mixed-format candidate sets really do arise under ICS + `'hybrid'` at
scale -- 12 native and 3 Parquet sstables coexisting in one table -- compaction keeps making
progress on them, and nothing is lost: the row count is exact.

**What it does not establish, and the distinction is the whole point of writing it down.** `pq` took
0 % of the rewrites, which is the *shape* the fix is supposed to produce -- but it is not evidence
of it. Parquet is only 6.3 % of the bytes here (it is the converted bottom tier, so it appears late),
and the observation window after the load was about two minutes. "0 of 3 files rewritten" is equally
consistent with correct bucketing and with there having been no opportunity. A green result that a
broken build would also produce is not a measurement.

**What would settle it** is a differential rather than a longer run: the same load with the fix
disabled, so the two churn figures can be compared. That needs a way to force `use_ondisk = false`
at runtime, which today would mean test-only code in the compaction path. The alternative -- hours
of sustained writes until Parquet holds the majority of the bytes and the bottom tier is rewritten
several times over -- measures the same thing far more slowly. Either is a real experiment; this run
is not, and the readiness table says so.

### 10.20 Object-storage downgrade — observed, 2026-08-20

§10.12 left this as the one open interaction, and was explicit about why: on object storage the key
is `sstables/<uuid>/Data.db` with **no version in the object name**, where locally the `pq-` filename
prefix is what makes an older node refuse to start. The code path was traced -- the version lives in
`system.sstables_registry` and is parsed by the same `version_from_string()` that throws -- but it was
read, not observed, because the registry is not reachable through CQL and there was nowhere to plant
a bogus version. The doc named what would settle it: *"a build with `pq` removed from
`version_string` reading a bucket written by this one."* That is what this is.

`~/pq-lab/objstore_downgrade.py`, 7/7. Isolation is the part that took thought: a node whose *local*
data directory also held `pq` sstables would abort on those and prove nothing about object storage, so
the experiment uses a dedicated data directory where everything local is `me` and the only `pq`
sstables are in the bucket.

| Stage | Result |
|---|---|
| Current binary, endpoint configured, `pq` table on an S3 keyspace | 10 objects in the bucket, 3 000 rows readable |
| `pq` removed from `version_string`, relinked, same data directory | **refuses to start, exit 1** |
| Source reverted, relinked, same data directory | starts, 3 000 rows still readable |

The message is the one the reasoning predicted, and it names the bucket:

```
ERROR database - Exception while populating keyspace 'dgks' with column family 't'
      from 's3://pqdgbucket': std::out_of_range (Unknown sstable version: pq)
ERROR init - Startup failed: std::runtime_error (Exception while populating keyspace 'dgks' ...)
```

So the registry scan for an object-storage table throws on an unknown version and startup fails,
exactly as the local filename path does. **§10.9's downgrade procedure now holds for object storage
as well as local disk**, and the readiness table's last "reasoned, not observed" row is closed.

**The third stage is not a formality.** Without it the experiment cannot distinguish "the older node
refused because of the version" from "the patch broke the binary in some other way" -- and a
self-reverting source patch that is never verified as reverted is how a tree quietly acquires a
one-line regression. The script reverts in a `finally` and then proves the reverted build works on
the same data.

### 10.21 Point reads, measured properly — 2026-08-20, and it invalidates §10.4's method

Re-measured with `BYPASS CACHE`, production cache settings, interleaved arms, a constant-cost canary
and min-of-400-probes as the estimator (`pointread_v2.py`). 8 M rows, `rows_per_row_group` swept to vary
row groups per file at a fixed row count. Note the direction, because the shorthand `rg=` used in
earlier versions of these tables made it easy to misread: `rows_per_row_group` is *rows per group*, so
**lowering** it **raises** the number of row groups. 8 M rows at 1 000 rows per group is ~8 000
groups; at 20 000 it is ~400. The setting and the count move in opposite directions.

This misreading is why the property was renamed from `row_group_rows` on 2026-08-20 (§8.2): the
old name could be parsed as "the number of row-group rows" *or* as "row groups, in rows", and the
first reading inverts the tuning direction. A comment warning about it -- which is what this
paragraph originally was -- only helps the reader who finds the comment; the name is what everyone
else sees. The old name still parses, permanently, because it is persisted in schema.

| arm | files | row groups | warm min | bypass min | cold min |
|---|---:|---:|---:|---:|---:|
| native | 1 | — | 254 | 455 | 462 |
| `rows_per_row_group` 20 000 | 4 | 402 | 256 | 2 494 | 2 500 |
| `rows_per_row_group` 5 000 | 4 | 1 601 | 249 | 3 942 | 3 958 |
| `rows_per_row_group` 2 000 | 4 | 4 002 | 254 | 5 947 | 6 198 |
| `rows_per_row_group` 1 000 | 4 | 8 001 | 256 | 10 155 | 10 905 |

Canary min across every block: 224–244 µs, a 9 % spread, so the machine was quiet enough for the
numbers to mean something — which matters on a shared 32-core box whose load average moved between
5.9 and 29.2 earlier the same day.

**The warm column is the finding that invalidates the old method.** Every arm lands at ~250 µs
regardless of format or row-group count, and the canary floor is 224–244 µs. Warm point reads were
measuring the **client round trip**, not the storage engine. That is why §10.4's earlier figure came
out at `pq/native = 1.05×`: both arms were pinned to the floor, and the comparison could not have
shown a difference whatever the engine did. **Any conclusion drawn from warm point-read ratios in
this project should be discarded**, including the reassuring ones.

**Cold, the effect is large and scales with the *number* of row groups.** Against native's 462 µs:
5.4× at 402 row groups, 8.6× at 1 601, 23.6× at 8 001.

**The slope is 4.42 µs per row group *per file*, and §10.4j's 4.32 µs estimate was right.** An earlier
version of this section said the estimate was "4× too pessimistic" on the strength of a 1.11 µs
slope; that figure was the slope against *total* row groups across the table's **four** files, while a
point read touches one file's footer. Dividing by files gives 4.42 µs and the estimate lands almost
exactly. The mistake was mine, not the estimate's, and it is recorded because dividing by the wrong
denominator produced a confident correction of something that was already correct.

**And the direction is the opposite of the obvious intuition, which is worth explaining.** A bigger
row group sounds like more work per point read — more rows to wade through for one answer. It is not,
because the reader never scans a row group: the index entry carries a *row ordinal* (§5.4 option A)
and the OffsetIndex turns that into a single page, so decode cost is bounded by `page_rows` (8 192 by
default) no matter how large the row group is. The only term that moves with row-group configuration
is the **footer**, and that scales with the row-group *count*. Bigger row groups mean fewer of them,
a smaller footer, and a faster cold point read.

The footer is also where the time goes rather than being a parse-only cost. Fitting cold latency
against footer size gives ≈ 2.1 ms fixed + 3.1 ms per MB — an implied ~320 MB/s, which is
read-and-parse throughput, not CPU alone. The fit predicts 6.5 ms at 1.42 MB against 6.2 measured and
3.9 ms at 0.57 MB against 3.96 measured.

**One consequence for the cache in §10.4l**: if the dominant term is fetching and parsing a
multi-megabyte footer, caching it removes most of this curve — and then the intuition above may
finally become visible, because page-level decode would be all that is left. So the ordering could
*invert* once the cache lands. That is a prediction to measure, not to assume.

**The trade is now quantified, and it points the opposite way from the size tuning.** At the shipping
default of `rows_per_row_group = 5 000` an 8 M-row table gives 1 601 groups and an 8.6× cold point read.
Fewer, larger row groups improve point reads — 402 groups is 5.4× — while scan-side projection and
size want the opposite (§10.4a). Whoever tunes this is choosing between them explicitly rather than
finding a setting that is good for both.

**Superseded for the cached case by §10.24.** Everything above is the *uncached* cost, and it is
still what the first read of an sstable pays. Once the footer cache is in play the slope inverts and
smaller row groups become the faster point read; the trade described here applies to a cold start,
not to a steady-state node.

### 10.23 Why the whole footer is read for one row group — and how to stop

Raised as an objection and it is a fair one: a point read needs one row group's entry, about
1 420 bytes, and instead reads the entire footer — up to 2.84 MB at 2 000 row groups
(`reader.cc:525`, `data_read(len - 8 - flen, flen)`).

**Two reasons, and only the first is forced.**

`FileMetaData` is a single Thrift compact struct. The `row_groups` list is delta-encoded and every
field is variable-length, so there is no way to seek to entry *N* without walking entries 0…N−1.
Parquet offers no offset index for the footer itself — telling, since it *does* provide exactly that
for page-level data, where `ColumnIndex`/`OffsetIndex` live outside the footer and are pointed at by
offsets. Row-group metadata stayed inside.

The reader already takes the only shortcut the format allows. `metadata_mode::lazy` skips *decoding*
each group's column list and records its byte extent (`columns_offset` / `columns_length`), so
`materialise_row_group()` later decodes one group. Decode is already O(1 group). The **read** and the
**walk** are still O(all groups), and that is what the 3.1 ms/MB fit in §10.21 is measuring.

**The fix is not a cache — it is a persisted side index.** Those extents are immutable for the life
of the file, and Scylla owns the sstable's other components. Writing `row group → (footer offset,
length)` into a component beside `Data.db` turns a cold point read into: read the 8-byte tail, read
the side index, read one 1 420-byte slab. At roughly 8 bytes per row group the index is ~16 kB for
2 000 groups — one read, no recursion. An O(row groups) cold path becomes O(1).

**And it keeps the file a Parquet file**, which is the constraint that rules the design of everything
here: the index lives *outside* `Data.db`, so an external reader is unaffected and still parses a
standard footer. This is the same reasoning that put encryption inside the format (§10.17) and the
row ordinal in the index rather than a byte offset (§5.4) — Scylla-private acceleration belongs in
Scylla's components, never in the interchange file.

**This also separates two things §10.4l had conflated.** An in-memory metadata cache helps the
*second* read of a file; the side index helps the *first*. Productization wants both, and the cache
alone leaves every cold start paying the full footer — which on a large node after a restart is every
read.

Cheaper mitigations, both already quantified and neither a substitute: fewer, larger row groups
(§10.21) and trimming what per-chunk metadata is written at all.

### 10.24 The footer cache, measured — 2026-08-20, and the predicted inversion happened

Same harness, arms and estimator as §10.21 (8 M rows, `BYPASS CACHE`, production caching,
interleaved arms, min of 400 probes × 3 rounds, one restart before the cold pass), so the tables are
directly comparable. The repeat used `pointread_probe.py`, which is §10.21's measurement half
without the ninety-minute rebuild.

**The control is the important column.** "Squeezed" is the *same binary and the same data* with
`components_memory_reclaim_threshold` set to 3 × 10⁻⁵ — about 250 kB on an 8 G shard, below one
sstable's footer — so the reclaimer takes every entry away as soon as a read publishes it. It
reproduces §10.21 to within 10 % arm for arm (+2.5 %, +9.2 %, 0.0 %, −4.1 %), which is what makes the
improvement attributable to the cache rather than to anything else that moved in the tree today.

| arm | row groups | §10.21 cold | squeezed (control) | cached | cached vs control |
|---|---:|---:|---:|---:|---:|
| native | — | 462 | 460 | 470 | — |
| `rows_per_row_group` 20 000 | 402 | 2 500 | 2 563 | **1 684** | 1.5× |
| `rows_per_row_group` 5 000 | 1 601 | 3 958 | 4 321 | **1 361** | 3.2× |
| `rows_per_row_group` 2 000 | 4 002 | 6 198 | 6 196 | **1 030** | 6.0× |
| `rows_per_row_group` 1 000 | 8 001 | 10 905 | 10 458 | **832** | 12.6× |

All figures µs, cold minimum. Canary min 225–261 µs (16 % spread) on the cached run and 225–253 µs
(12 %) on the control, against 224–244 µs in §10.21 — so all three are comparable and none of this
is the machine, which matters on a box whose load average passed 30 earlier in the same hour. The
cached run was measured twice, on two node restarts, agreeing to within 14 % (946/1 042/1 441/1 672
against 832/1 030/1 361/1 684) — the widest gap being the 8 001-group arm, which is also the fastest
and so the most exposed to the 250 µs floor underneath all of this.

**Against the row format, cold: 1.8–3.6× rather than 5.4–23.6×.** The worst case improves most,
which is the shape to expect from removing a term that scales.

**The slope inverted, exactly as §10.21 predicted it might.** It was +4.42 µs per row group per
file; with the footer cached it is **−0.449 µs per row group per file** (−0.112 per row group across
the four files), and the control run reproduces +4.155. So the intuition §10.21 had to argue against
— that *smaller* row groups make point reads faster — is now the visible effect, because the footer
term it was competing with is gone and what remains is the per-chunk dictionary and page extents a
read fetches inside its group. §10.21 said "the ordering could invert once the cache lands; that is
a prediction to measure, not to assume". It inverted.

**This changes the tuning advice back.** With the cache, `rows_per_row_group` no longer trades point-read
latency against size in the same direction: smaller groups are both faster to point-read *and* worse
for size (§10.2), which is the trade §10.4c originally described at test scale. The
larger-row-groups-for-latency argument in §10.21 applies to the *uncached* first read, which is what
§10.23's side index is for.

**And the inversion turned out not to be about row groups at all — see §10.25.** Every arm in this
table changed its *page* size along with its row group, because the writer emits
`min(page_rows, rows in the group)` values per page and 8 192 cannot bind at 5 000. Re-measured with
the row group held fixed, cold latency tracks the page at ~130 ns/row and the row-group count retains
only a 10–20 %-per-4× residual: the −0.449 µs per row group above is mostly the page effect wearing
the row group's label. The slope is real; its attribution was not.

**Warm reads are unchanged at ~256 µs across every arm**, because that is the client round trip
(§10.21) and no storage change can move it.

**Eviction, on a node rather than in a unit test** (`footer_cache_evict.py`, same data, two node
starts):

| threshold | misses | hits | evictions | bytes retained after |
|---|---:|---:|---:|---:|
| 0.2 (default) | 4 | 196 | 0 | 12 181 471 |
| 3 × 10⁻⁵ | 200 | 0 | 200 | 0 |

200 single-partition reads either way, and **every column of every row identical across the two
phases**. At the default an 8 G shard has 1.6 GB of budget, four files are parsed once and 196 reads
hit; squeezed, every read publishes an entry and the reclaimer removes it again, so the miss and
eviction counters move in lockstep and the gauge sits at zero. The 1 600-probe control run above
recorded 1 600 misses and 1 600 evictions with no wrong answers, which is the same evidence at
eight times the volume.

**The retained size, and the "too much to pin" argument checked against reality.** 12 181 471 bytes
for one table's four files at 8 001 row groups is **1 522 bytes per row group**, against 1 425 on
disk (§10.22) — footer blob plus 96 bytes of lazy row-group record and cumulative-row entry. The
five arms of this experiment together held 21.4 MB, which is 1.3 % of one 8 G shard's reclaimable
budget for 32 M rows; a node with a thousand 1 601-group sstables would want ~2.4 GB, which is the
number that made this evictable rather than pinned in the first place.

**What the cache does not do.** The *first* read of an sstable after a restart still fetches and
walks the whole footer — the squeezed column above is what that costs — and on a node that has just
restarted, every read is a first read. That is §10.23's side index, and it is not built. Compaction
also publishes an entry per input sstable that it will never read twice, converting footer bytes into
resident memory for the duration; the reclaimer bounds it, and "populate only for bounded reads" is
the refinement if it ever shows up as pressure.

### 10.25 The two knobs, separated — 2026-08-20, and `page_rows` is a ceiling rather than a size

§10.24 left a fork. Caching the footer inverted the `rows_per_row_group` slope, from +4.42 µs per row
group per file to −0.449, and two readings fit that equally well while recommending opposite
defaults: either the row-group count still drives point-read cost with a different sign, or row
groups only ever mattered *through* the footer and what remains is page decode. Another
`rows_per_row_group` sweep could not settle it, because of a structural confound present in every
sweep in this section.

**The writer slices pages at `min(page_rows, rows in the row group)`** (`parquet_writer.cc`), and
`parquet_reader.cc:375` steps over any page whose row range excludes the target without
decompressing it, then decodes the containing page in full. At the shipping defaults — 5 000 rows per
group, an 8 192-row page — the page bound cannot bind: there is one data page per column chunk, so
the **effective page size *is* the row-group size**. §10.21 and §10.24 therefore moved three terms at
once: footer bytes, the per-chunk dictionary page, and the data page. That is the same defect §10.4f
found one level down, reappearing: the value was reachable, but only if the other knob left it room.

So both are swept separately, and the decisive figure is a prediction at a point the fit did not see.

#### Method

`~/pq-lab/pagesweep.py`. 8 M rows, the same 28-leaf time-series schema, and §10.21's estimator and
gates unchanged so the tables read directly against §10.21 and §10.24: `BYPASS CACHE`, production
cache settings, interleaved arms, a constant-cost canary, min of 400 probes × 3 rounds, one node
restart before the cold pass. `rows_per_row_group` is held at 20 000 — large enough that the footer
term is small and, more importantly, *constant*: every arm in the sweep has 401 row groups and
~620 kB of footer, which the table shows rather than asserts. Footer bytes come from each file's own
tail (the `uint32` before the `PAR1` magic), because the encoded length is what a cold read fetches
and walks, not pyarrow's parsed view of it.

Two failures worth recording, because both were avoidable and neither was anticipated. A restarted
node schedules compaction for every table it finds, and with this many 8 M-row tables on one shard
that starved the cold pass enough to time a read out — which killed the first attempt *after* all
three phases had been collected, because the table and the JSON are both written at the end. The
harness now quiesces after the restart and drops a failed probe rather than the run. And a
**whole-table aggregate is not measurable this way at all**: an 8 M-row scan on one shard exceeds
`range_request_timeout_in_ms`, and all 24 scan probes, native included, returned read timeouts.
Scans are therefore measured over a fixed 2 % token slice — identical across arms, so the comparison
holds and only the absolute figure is a fiftieth of a table.

#### Cold point reads and file size against the effective page

8 M rows, 2 sstables. Two independent replicates; the second adds the control arm and the scans.
Canary 227–257 µs (13 % spread) and 228–266 µs (17 %), against 224–244 µs in §10.21, so all three
runs are comparable.

| arm | row groups | footer share | effective page | cold µs (run 1) | cold µs (run 2) | MB | scan ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| native | — | — | — | 470 | 454 | 223.18 | 326 |
| **5 000 / 8 192 (shipping)** | 1 601 | 9.8 % | **5 000** | 1 930 | 1 366 | 22.32 | 921 |
| 20 000 / 256 | 401 | 0.7 % | 256 | 1 345 | 1 008 | 86.22 | 683 |
| 20 000 / 512 | 401 | 1.0 % | 512 | 1 503 | 996 | 58.52 | 660 |
| 20 000 / 1 024 | 401 | 1.5 % | 1 024 | 1 228 | 937 | 39.87 | 810 |
| 20 000 / 2 048 | 401 | 2.1 % | 2 048 | 1 288 | 1 193 | 28.05 | 946 |
| 20 000 / 8 192 | 401 | 3.1 % | 8 192 | 2 295 | 1 240 | 18.95 | 1 126 |
| 20 000 / 20 000 | 401 | 3.6 % | 20 000 | 3 853 | 3 641 | 15.84 | 1 531 |
| 5 000 / 2 048 *(control)* | 1 601 | 6.9 % | 2 048 | — | 1 073 | 32.45 | 742 |

**The hypothesis holds, and the slope is reproducible across measurements that share no code.** A
least-squares fit of cold latency against *effective* page size gives **130.6 ns per row** (run 1)
and **129.4 ns** (run 2). §10.4g measured the page-decode slope directly at **114 ns/value** on a
different schema, a different binary and an in-process harness; §10.24's `rows_per_row_group` slope
re-expressed per *page* row implies **118 ns/row**; and the in-process grid below gives
**116 ns/row**. Five estimates spanning 114–131 ns/row is one mechanism, not a coincidence.

**And the prediction the fit did not see.** The shipping arm has **four times** the row groups of
every arm the fit was built from — 1 601 against 401, and 3.9× the footer bytes — but its effective
page is a point on the line. Predicted from the six 20 000-row arms alone: 1 874 µs. Measured:
1 930 µs, **within 3.0 %**. An arm that is off the curve by 4× in row-group count is on it to 3 % in
page size. With the footer cached, the row-group count is very nearly not a term.

**"Very nearly" is measured rather than hedged.** The control arm puts a number on the residual: same
2 048-row page, four times the row groups, **120 µs faster** at 1 073 against 1 193 µs — 10 %, in the
direction §10.24's negative slope predicted, and about a twentieth of the 2 400 µs the page spans.
The in-process grid agrees: 19 % for the same 4× at page 2 048, 8 % at page 512. So **the page is the
dial and the row group retains a residual worth roughly 10–20 % per 4×**. The candidates for that
residual are named by the format: Parquet defines the dictionary page as per column *chunk*, so its
decode cost tracks the row group whatever the page is, and a read landing in the last page of a chunk
parses the header of every page before it.

**Below about 2 048 rows per page the curve flattens and is not resolvably monotone** — 1 008, 996,
937, 1 193 µs across 256…2 048 in run 2, against a 228 µs canary floor and a fixed cost the fit puts
at 800–1 229 µs. This is §10.4g's "71 % is a fixed floor" surviving the footer cache: roughly 1 ms
per read that no page setting touches. Spending size below 2 048 rows buys nothing.

**Scans move the same way as point reads, not the opposite way.** This was the risk the sweep existed
to check — a page chosen on point reads quietly hurting the workload Parquet is for — and it is not
there: scan time falls from 1 531 ms at a 20 000-row page to 660 ms at 512, monotone apart from the
256 point. Both read shapes want smaller pages; only *size* wants larger ones.

**Two caveats kept the scan column out of the recommendation's load-bearing structure**, and that was
the right call for a reason better than caution — **§10.26 resolved it on 2026-08-20**. The column
disagreed with §10.4c, which had `pq` at **0.82×** the row format on a full scan where this measures
**2.0–4.7×**, and with §10.4c and §10.4f, which both found scan time *flat* across page and row-group
sweeps. It was the path, as suspected, but not for the suspected reason and not because of the slice:

- `pq_reader::next_window()` forks on whether the partition range carries **bounds**, not on how wide
  the resulting window is. An unbounded range streams whole row groups in 16 384-row windows; a
  bounded one takes the point-read machinery, 512 rows at a time, re-fetching and re-decoding each
  512 rows' containing page for every leaf. §10.4c timed the first path; a token-restricted aggregate
  — and any range scan from a client, because the coordinator splits at tablet boundaries — gets the
  second. Both flat-versus-sloped findings are correct, on their own path.
- **The 2 % slice was not the error.** A whole-table scan, measurable once
  `range_request_timeout_in_ms` was raised, gives **1.54 / 1.90 / 2.29 / 4.30×** across the same page
  settings, reproducing this column's ordering and magnitude; the slice runs 1.2–1.5× pessimistic
  only because a 300 ms probe is mostly noise floor.
- **Column projection is not honoured** — on any path, confirmed at the level of bytes read — but
  that is not why this looked slow: the query above projects all three of the table's regular
  columns, so a working projection would have skipped nothing.

So the numbers in the `scan ms` column stand as measurements of the path CQL uses. What does *not*
follow from them is a page recommendation: their slope is a property of the reader being on the wrong
path, and on the streaming path the page does not affect a scan at all. Choosing `page_rows` on this
column would have optimised for a defect, which is why the recommendation below is still built on
point-read latency, file size and scan memory alone.

#### Scan memory is what `rows_per_row_group` actually costs

Peak reader memory during a scan is in-process and the node harness cannot see it, so it comes from
`perf_pq_scan_memory_scaling`, which already reads both knobs from the environment
(`~/pq-lab/scanmem_page.sh`). 160 000 rows, 7-leaf perf schema:

| `rows_per_row_group` | `page_rows` | data bytes | **peak scan memory** | in-process point µs |
|---:|---:|---:|---:|---:|
| 1 000 | 8 192 (→1 000) | 2 528 084 | **1 168 kB** | 490 |
| 5 000 | 8 192 (→5 000) | 2 049 689 | **5 584 kB** | 1 037 |
| 5 000 | 2 048 | 2 176 053 | **5 576 kB** | 732 |
| 5 000 | 512 | 2 563 315 | **5 588 kB** | 516 |
| 20 000 | 8 192 | 1 996 552 | **19 872 kB** | 1 869 |
| 20 000 | 2 048 | 2 103 746 | **19 860 kB** | 869 |
| 20 000 | 512 | 2 522 977 | **20 188 kB** | 559 |
| 50 000 | 8 192 | 1 984 926 | **20 616 kB** | 1 481 |
| 100 000 | 8 192 | 1 984 926 | **20 628 kB** | 1 604 |

**Peak scan memory is a function of `rows_per_row_group` alone, and is flat across a 16× change in
page size** — 5 576–5 588 kB at every page at 5 000 rows, 19 860–20 188 kB at every page at 20 000.
That one fact separates the knobs, and it is the most decision-relevant number here:

- **`rows_per_row_group` trades size against scan memory**, roughly linearly at ~1.1 kB per row of
  row group, and R-13 is a requirement rather than a preference. 5 000 → 20 000 is **3.6×**. It
  saturates above 20 000 because the 64 MiB shredder budget takes over from the row count
  (§10.1f-rg), which is why 50 000 and 100 000 produce byte-identical files.
- **`page_rows` trades size against latency at zero memory cost.** Nothing in the memory column
  moves when the page changes by 16×.

#### What a smaller page costs in size on real data — and the synthetic schemas are the outliers

§10.4m's lesson was that a size cost measured on one schema transferred badly by 2.5×, so this is
measured per dataset, sweeping *pairs* rather than the page alone (`~/pq-lab/sweep_page.sh`). Each
point round-trips native → `pq` through a real major compaction; the row-group count of the result is
recorded, because on sparse wide shapes the byte budget binds before the row count does and the pair
asked for is not the pair written.

| pair | ISD-Lite, 20 leaves | vs shipping | Backblaze, 199 leaves | vs shipping |
|---|---:|---:|---:|---:|
| **5 000 / 8 192 (shipping)** | 1 530 718 — **48.8 %** | base | 20 803 872 — **95.2 %** | base |
| 20 000 / 8 192 | 1 454 056 — 46.3 % | −5.0 % | 19 908 060 — 91.1 % | −4.3 % |
| 50 000 / 8 192 | 1 430 185 — 45.6 % | −6.6 % | 19 908 060 — 91.1 % | −4.3 % |
| 20 000 / 20 000 | 1 406 834 — 44.8 % | −8.1 % | 19 908 060 — 91.1 % | −4.3 % |
| 20 000 / 2 048 | 1 717 478 — 54.7 % | **+12.2 %** | 23 888 754 — **109.3 %** | **+14.8 %** |

300 000 rows each, against a dictionary-compressed SSTable baseline of 3 139 684 and 21 850 343
bytes. The 5 000-row Backblaze figure is byte-for-byte the 20 803 872 of §10.1f-rg from a separate
run and script, which is the cross-check that makes the rest of the column trustworthy.

**A 4× smaller page costs +18.1 % of the Parquet file on ISD-Lite and +20.0 % on Backblaze** (both
against the 8 192 arm at the same row group). §10.4m measured +16.8 % from a different pairing. The
28-leaf synthetic table in the latency sweep above says **+48 %** for the same step — **the synthetic
schema is the outlier by 2.7×**, in the direction that would have made small pages look unaffordable.
The mechanism is the one §10.4m's correction implies: a page is the unit a codec and a delta baseline
restart on, so the cost of cutting it depends entirely on how much the long-run encodings were
amortising, which is a property of the data.

And the page swept at the *shipping* row group, which is the tuning move an operator would actually
make. Separate load of the same dataset, so the baseline moved slightly (3 246 444 native) and the
column is internally consistent rather than comparable to the one above:

| `rows_per_row_group` | `page_rows` | `pq` bytes | ratio | vs the inert default |
|---:|---:|---:|---:|---:|
| 5 000 | 8 192 (→5 000) | 1 603 302 | 49.4 % | base |
| 5 000 | 4 096 | 1 703 794 | 52.5 % | **+6.3 %** |
| 5 000 | 2 048 | 1 878 547 | 57.9 % | **+17.2 %** |
| 5 000 | 1 024 | 2 209 104 | 68.0 % | +37.8 % |

**Three independent measurements of the same step now agree**: +17.2 % here, +18.1 % at the
20 000-row group, and §10.4m's +16.8 % from a different pairing and run. Likewise 4 096 costs +6.3 %
against §10.4m's +7.5 %. The size cost of halving the page is a stable ~+17 % per 4× on real data —
which is the number that was previously in doubt, and it is *not* the +48 % the synthetic schema
reports.

**Three things in the pair table are worth reading twice.** Backblaze at a 2 048-row page crosses from a
4.8 % win to a **9.3 % loss** — the format stops being worth using on that shape, which is exactly
§10.4m's finding and it survives the footer cache untouched, because the cache changed the latency
side and not the size side. `20 000 / 20 000` and `20 000 / 8 192` are **byte-identical** on
Backblaze, which is the inertness rule visible in real output: the byte budget cuts that table at
~6 400 rows per group, so both page settings exceed the group and produce one page per chunk. And
50 000 buys nothing over 20 000 there for the same reason, while on ISD-Lite, where the budget never
binds, it keeps working.

#### The recommendation: `rows_per_row_group = 5 000`, `page_rows = 8 192` — unchanged, for different reasons

**`rows_per_row_group` stays at 5 000, and the binding constraint is scan memory, not latency.**
Raising it to 20 000 buys −4.3 % to −5.0 % of the Parquet file on real data and costs **3.6× the peak
scan memory**, 5.6 → 19.9 MB per reader. That is ~3.2 MB of reader memory per 1 % of file size saved,
and it would give back most of what the §10.4c default change bought (which was chosen on the same
memory argument, then against a synthetic size figure; the size figure is now real and is smaller
than the synthetic one suggested). Latency is not the reason: with the footer cached the row group is
worth only 10–20 % per 4×, and it points the other way.

**`page_rows` stays at 8 192, and it should be understood as a ceiling rather than a page size.** At
a 5 000-row group it does not bind, and that is correct rather than a defect. Its job is to cap
decode volume the moment the group is larger, and the cap is worth **2.9×**: on the 20 000-row arm,
one page per group is 3 641 µs against 1 240 µs at 8 192. Where it is not needed it costs nothing —
Backblaze's 8 192 and 20 000 outputs are byte-identical. This is the same argument §10.4f made about
dictionary chunks scaling with a knob, and it is why the value should not be lowered to "make it do
something": lowering it to 2 048 costs +18–20 % of the file on both real datasets, pushes Backblaze
into a net loss, and buys 173–293 µs against a ~1 ms floor.

**The trade, in the units the decision is made in.** From the shipping pair, the two available moves
cost:

| move | size, real data | cold point read | scan memory |
|---|---:|---:|---:|
| lower `page_rows` to 4 096 | +6.3 % | ≈ −120 µs (from the 130 ns/row slope) | unchanged |
| lower `page_rows` to 2 048 | **+17 to +20 %** | −293 µs (1 366 → 1 073), 3.0× → 2.4× native | unchanged |
| lower `page_rows` to 1 024 | +37.8 % | nothing further — the floor | unchanged |
| raise `rows_per_row_group` to 20 000 | **−4.3 to −5.0 %** | neutral within replicate spread | **+3.6×** |

So the latency move is ~17 µs of cold point read per 1 % of file size, and the size move is ~3.2 MB
of reader memory per 1 % of file size. Neither is a bargain, which is what "the defaults are already
on the frontier" looks like when it is measured instead of assumed. 4 096 is the one move that is
close to fair — 6.3 % for about 120 µs — and it is left as a per-table option rather than a default
because it is a straight trade of the format's strongest metric for its weakest, and §10.4m already
declined it once on the same grounds with the same numbers.

**What is data and what is judgement.** Data: the 130 ns/row page slope and its five-way replication;
the 3 % prediction across a 4× change in row-group count; the 10–20 % residual row-group term; scan
memory being a function of the row group alone; the real-data size costs (+18/+20 % for the page,
−4.3/−5.0 % for the row group); the 2.9× value of the ceiling; Backblaze crossing into a net loss at
2 048. Judgement: that 3.6× scan memory outweighs 5 % of file size, which is a memory-versus-disk
call this project has made the same way twice before (§10.4c, §10.1f-rg) and which a
memory-rich deployment could reasonably decide differently; and that +18–20 % of the file is too much
for 293 µs on a metric that is still 2.4–3.0× the row format, which is §10.4m's judgement re-examined
against better numbers rather than inherited. The scan column carries no part of this, which §10.26
turned from a caution into a positive reason: its slope comes from the reader taking the point-read
path for a bounded range, so a page chosen on it would be tuned to a defect that is meant to be fixed.

**Should the defaults differ by table shape?** The data says the *optimum* does and the *achievable
gain* does not justify a second default. Wide tables pay the small-row-group tax about four times
harder per group — Backblaze's footer is 10.9 % of its file at 5 000 rows against ISD-Lite's 5.2 %,
because footer bytes scale as groups × leaves (§10.22) — which argues for larger groups on wide
tables. But that is precisely where the knob stops working: the 64 MiB byte budget already cuts
Backblaze at ~6 400 rows, so `rows_per_row_group` above 20 000 is inert there, and the whole
available gain is 4.3 %. A per-shape *row-count* default would therefore be inert exactly where it is
most wanted. That is a measured argument for the reframing already recorded in open question 15: the
per-shape lever worth pursuing is `row_group_buffer_bytes`, not the row count — with the caveat that
it trades against OOM rather than against latency. ClickBench preferring *smaller* row groups
(§10.2) points the same way: no single row count is right, and the useful response is that both are
per-table properties (§8.2) rather than a second global pair.

### 10.26 The two scan numbers were two different readers — resolved 2026-08-20, and §10.4c's headline is wrong

§10.25 left one thing unresolved and flagged it as the thing to settle before any scan figure in this
document is relied on: it measured `pq` at **2.0–4.7×** the row format on a scan, where §10.4c had
measured **0.82×** — Parquet *faster* — and the two also disagreed on whether page size affects a
scan at all. Both measurements are sound. They measured different code.

#### The fork

`pq_reader::next_window()` chooses its unit of work from one bit:

```
const bool bounded = _pr->start() || _pr->end();
const int64_t win = bounded ? point_window_rows : scan_window_rows;   // 512 : 16384
if (bounded) { colsdata = co_await decode_paged(rg, ...); }           // per-window page fetch
else         { co_await load_row_group(rg); ... }                     // sequential row group
```

An **unbounded** range streams: one `data_read` per row group, 16 384 rows decoded per window.
A **bounded** range takes the point-read machinery: 512 rows per window, and `decode_paged` fetches
and decodes, *for every leaf and for every window*, the whole page containing those 512 rows.

That is the right fork for a point read and the wrong one for a scan, because the predicate is the
presence of a bound rather than the width of the resulting window. A bounded range covering two
million rows is read 512 at a time.

**And CQL never produces an unbounded range.** A range scan is split at tablet boundaries by
`query_ranges_to_vnodes_generator` before any replica sees it, and every piece of a split carries a
bound — including the first, which has an open start but a closed end, and `bounded` is an `||`. The
pqps tables have 4 tablets each. So `query::full_partition_range` reaches the reader only from
compaction and streaming (`make_full_scan_reader`) and from in-process tests. **Every scan a client
can issue takes the point-read path.**

This was checked rather than assumed, because internal paging is the other candidate: a CQL aggregate
is paged at `select_internal_page_size` = 10 000 rows, and page 2 onward resumes from a position, so
it too would be bounded. Raising that to 20 000 000 — one internal page for the whole 8 M-row table,
live-updated by SIGHUP — changed **nothing**: 35 291 ms against 34 866, and 497 632 reads against
497 632, byte for byte. The split is the cause and no setting removes it.

#### The signature, which needs no timing

The two paths differ by two orders of magnitude in read count, so `scylla_reactor_aio_reads` names
the path outright. Whole-table `SELECT count(temp), count(status), count(note) ... BYPASS CACHE` over
8 M rows on `pqps`, `range_request_timeout_in_ms` raised to 1 800 s by SIGHUP so the scan that
§10.25 could not run at all becomes measurable (`~/pq-lab/scanpath_node.py`):

| arm | effective page | file MB | full scan ms | vs native | disk reads | MB read | read amplification |
|---|---:|---:|---:|---:|---:|---:|---:|
| native (`me`) | — | 234.0 | **15 200** | 1.00× | 1 792 | 234.0 | **1.0×** |
| `pq`, page 512 | 512 | 61.4 | 23 424 | **1.54×** | 496 402 | 328.2 | 5.3× |
| `pq`, page 2 048 | 2 048 | 34.0 | 28 945 | **1.90×** | 497 632 | 366.6 | 10.8× |
| **`pq`, shipping 5 000 / 8 192** | 5 000 | 23.4 | 34 866 | **2.29×** | 497 632 | 460.0 | **19.7×** |
| `pq`, page 20 000 | 20 000 | 16.6 | 65 420 | **4.30×** | 496 402 | 887.3 | **53.4×** |

Two replicates of every arm, min taken; the two agreed within **0.6 %** on all five (native 15 200
/ 15 342; shipping 35 065 / 34 866; page 20 000 65 420 / 65 803), which is what a 15–65 s probe buys
over the sub-second ones the slice method was reduced to. Native is the canary and moved 0.9 %
across the hour.

**The read count is flat at ~497 000 while the bytes rise 2.7×.** 8.02 M rows ÷ 512 = 15 664 windows
× 28 leaves = 438 592, plus a dictionary extent per chunk per window: ~31.8 reads per window,
independent of the page. What the page changes is how much of each read is wasted. At the shipping
default the reader pulls **460 MB through a 23 MB file** to answer one aggregate.

So §10.25's scan column was measuring something real, and its slice was not the problem: the
full-scan ratios above (1.54 / 1.90 / 2.29 / 4.30) reproduce the ordering and the rough magnitude of
the 2 % slice's (2.0 / 2.3 / 2.8 / 4.7). The slice runs 1.2–1.5× pessimistic because a 300 ms probe
on a shared box is mostly noise floor, not because 2 % of a table scans differently from all of it.

#### The other path, measured over the same rows in the same process

`test/boost/sstable_parquet_perf_test.cc` now times both: the existing unbounded scan, and the same
rows read through a range whose bounds span every partition in the file. Nothing else differs — same
schema, same `full_slice()`, same sstable, same process — so the ratio between the columns is the
cost of the fork and nothing else. 20 000 partitions × 5 rows, min of 5, one pinned core
(`~/pq-lab/scanpath.sh`):

| arm | native scan | native bounded | `pq` scan | `pq` bounded | `pq`/native, streaming | `pq`/native, bounded |
|---|---:|---:|---:|---:|---:|---:|
| **shipping 5 000 / 8 192** | 121 | 121 | 123 | 252 | **1.02×** | 2.08× |
| 20 000 / 256 | 122 | 122 | 134 | 149 | 1.10× | 1.22× |
| 20 000 / 512 | 121 | 123 | 130 | 146 | 1.07× | 1.19× |
| 20 000 / 1 024 | 122 | 122 | 131 | 158 | 1.07× | 1.30× |
| 20 000 / 2 048 | 122 | 123 | 129 | 179 | 1.06× | 1.46× |
| 20 000 / 8 192 | 121 | 123 | 127 | 304 | 1.05× | 2.47× |
| 20 000 / 20 000 | 122 | 123 | 138 | 596 | 1.13× | **4.85×** |
| 5 000 / 2 048 | 122 | 123 | 127 | 176 | 1.04× | 1.43× |
| shipping, +21 leaves | 381 | 381 | 415 | 644 | 1.09× | 1.69× |

**The row format pays 1.00–1.02× for the bound and `pq` pays 1.9–4.3×.** Native's index makes a
bounded range free, which is also the control: its scan column is 121–122 ms across every arm, a
0.8 % spread, so nothing here is load.

**Both of §10.25's disputed claims are now placed.** The bounded column runs 1.19× → 4.85× as the
page goes 512 → 20 000 and reproduces the node's 2.0–4.7× from a different binary, a different
schema and an in-process harness; the streaming column is **flat within 1.02–1.13× across a 78×
change in page size**, which is what §10.4c and §10.4f reported. Page size does not affect a
streaming scan — a page is not a unit of work there — and it dominates a bounded one, because each
512-row window decodes its whole containing page.

#### Column projection is not honoured, on any path

The named prime suspect, and it is confirmed at the level of bytes read. It is also not the
explanation for anything above.

The mechanism is absent rather than lost: `make_reader` carries the comment *"Column projection is
not pushed down yet"* (`reader.cc:1031`), and both readers build their `column_input` spans over
`columns()` — every leaf of the row group — so there is no path on which a projection could arrive.
Measured on `pqps.pqdef` (28 leaves, 5 of them user columns), same table, same aggregate shape:

| query | leaves genuinely needed | disk reads | MB read |
|---|---:|---:|---:|
| `count(temp), count(status), count(note)` | 22 of 28 — 93.7 % of chunk bytes | 497 658 | 461.9 |
| `count(temp)` | 22 of 28 — 93.7 % | 497 632 | 460.0 |
| `count(*)` | 19 of 28 — **22.6 %** | 497 632 | 460.0 |

Identical to 0.005 %. `count(*)`, which needs no value column at all, reads every one of them. The
single-partition path is the same: 300 point reads at 32 extents and 29 696 bytes per read, byte for
byte identical across `SELECT *`, `SELECT temp` and `SELECT station`.

**What projection would be worth here is mostly read *operations*, not bytes.** On this schema
`temp` alone is 70.6 % of the chunk bytes and 23 of the 28 leaves — every `__ttl_*`, `__ldt_*`,
`__rt*`, `__pt_*` — are 44 828 bytes each, 5 % of the file between them, because they are all-null
and RLE away to nothing. They are nearly free to *store* and they cost 23/28 of the ~497 000 reads
and of the per-leaf decode setup. That is the reverse of §10.1c's framing, which counted bytes from
footer metadata and is a lower bound on a benefit the reader cannot yet claim at all.

It also means projection is **not** why §10.25 looked slow: that query projected all three of the
table's regular columns, so a working projection would have skipped nothing.

#### The corrected scan story

**§10.4c's headline is wrong twice over and should not be quoted.** It said `pq` reads a full scan
*faster* than the row format, 0.82×. Two corrections:

1. **It is not the scan anyone runs.** 0.82× was `query::full_partition_range` in-process — the path
   compaction and streaming take. Through CQL the same table at the same defaults is **2.29×**, and
   the sign of the comparison flips.
2. **0.82× does not reproduce even on its own path.** Nine arms measured today put the streaming
   ratio at **1.02–1.13×**. Parity, not an advantage. §10.4c's 0.82 was a single pair of 121–144 ms
   probes with no replication and no canary, taken before the standard §10.21 imposed; §10.4's own
   table reported 0.95× for the same quantity and the two were never reconciled.

**So: on the streaming path `pq` is at parity with the row format, and through CQL it is 2.3× slower
at the shipping defaults.** "Scan parity" (§1.2) is met by the format and missed by the reader.

**The 2.3× is an implementation defect with a named cause and a bounded fix**, not a property of
Parquet. Two changes are indicated, in this order:

- **Choose the window from the width of `[_row_lo, _row_hi)`, not from the presence of a bound.** A
  bounded range spanning millions of rows should stream. This is the whole 2.3×: the in-process
  streaming arm is at parity, and adding the constant per-row coordinator cost the native arm
  exposes (15 200 ms server-side against ~9 700 ms extrapolated in-process for 8 M rows) to `pq`'s
  streaming cost predicts **~15.4 s against native's 15.2 s**. That is an estimate across two
  schemas and an 80× row count, not a measurement, and it should be measured after the change rather
  than believed now.
- **Push the projection down.** Independent of the above, and worth most on wide tables, where the
  ~497 000 reads and the per-leaf decode setup scale with leaf count while the useful bytes do not.

**What this does *not* change.** The §10.25 recommendation stands untouched, and for a better reason
than "the scan column was kept out of it": the direction that column points — smaller pages help
scans — is an artifact of the reader being on the wrong path. On the streaming path the page does not
matter to a scan at all. So `page_rows` should continue to be chosen on point-read latency and file
size, and a future reader that streams bounded ranges will not want it changed. Deciding the page
default on §10.25's scan column would have optimised for a defect.

**Cost of the method, stated.** The whole-table scan needed `range_request_timeout_in_ms` raised from
10 s to 1 800 s (LiveUpdate, SIGHUP, no restart — a restart would recompact the pqps arms). That
raises no bias question: it changes when a read is abandoned, not what it does, and both formats got
the same value in the same session. The 2 % slice of §10.25 is retained as a cross-check rather than
replaced, and it agrees to 1.2–1.5×.

Two pieces of lab state this leaves behind, recorded because a later measurement inherits both. The
node now stands at **300 s** rather than back at 10 s — the slowest arm here ran 66 s, so the default
makes a whole-table scan on one shard unmeasurable rather than merely slow, which is the trap §10.25
fell into; the standing cost is that a genuine range-scan timeout regression would show up on this
node as a slow query instead of a failed one, so anything testing whether a scan fits the production
default must put 10 s back for the duration. And the **`pqps` keyspace is kept** (535 MB, 10 arms at
8 M rows), because these figures overturn the format's headline claim and re-probing them is 14
minutes against 45 to rebuild. `select_internal_page_size` and `read_request_timeout_in_ms` were both
raised for single probes and are back at their defaults — written back explicitly, since a LiveUpdate
option keeps its last live value when the key is merely deleted from the file.

### 10.22 Footer size, measured per file and per row group

Prerequisite for deciding whether a metadata cache is worth building (§10.4l), measured on the same
four files:

| row groups | file bytes | footer bytes | footer share | bytes/row group |
|---:|---:|---:|---:|---:|
| 101 | 4 987 795 | 153 583 | 3.1 % | 1 521 |
| 399 | 5 829 954 | 568 679 | 9.8 % | 1 425 |
| 1 005 | 8 992 781 | 1 431 985 | 15.9 % | 1 425 |
| 1 995 | 13 680 138 | 2 843 066 | **20.8 %** | 1 425 |

**1 420 bytes per row group, plus ~10 kB fixed**, at 28 leaves — so it scales with row groups and
leaves, not with rows. Two consequences.

The footer is **20.8 % of the file** at 1 995 row groups. That is a size argument for larger row
groups quite separate from the latency one, and it is the same direction: the narrow-row-group
setting is expensive twice over.

And it sizes the cache. Holding the parsed footer for one sstable costs on the order of
`1.4 kB × row groups`; a 1 601-group sstable is ~2.3 MB, and a node holding a thousand such sstables
would want ~2.3 GB. That is far too much to pin, which settles the design question in §10.4l:
**a metadata cache has to be evictable under pressure, not merely bounded.** It should register with
`sstables_manager`'s existing reclaim machinery — `_total_reclaimable_memory` against
`memory_reclaim_threshold` (default 0.2), with the intrusive list ordered by
`lesser_reclaimed_memory` — which is how bloom filters are already dropped, rather than inventing a
second policy. Built that way and measured in §10.24, where the retained size comes out at
1 522 bytes per row group — this estimate plus 96 bytes of parsed row-group record.

### 10.17 Encryption at rest, as a Parquet feature — built 2026-08-20

**A correction first, because it shaped the design.** This section previously said Scylla has "no
encryption-at-rest code in this tree at all -- no `ee/` directory, no such symbols". That is **wrong**.
The code is in **`ent/encryption/`** — `key_provider`, `key_provider_factory`, `symmetric_key`, and
providers for local files, replicated keys, **KMIP, AWS KMS, Azure and GCP**. The claim came from
grepping for `ee/`, finding nothing, and concluding absence rather than checking the directory listing.
It propagated into the future-work doc and the deck before being caught.

It matters because those providers are exactly what a **BYOK** deployment uses, and because
`key_provider::key(const key_info&, opt_bytes id)` returns a key *together with an opaque id* to store
with the data and hand back to retrieve the same key later — which is precisely the role Parquet's
`FileCryptoMetaData.key_metadata` plays. The two models line up; there was never a need to invent a
key file.

**Parquet Modular Encryption is still the right layer** for the file itself: encrypting the Data
component from outside would make it opaque to every external reader and forfeit the interoperability
that is the whole argument for Parquet.

**Built against a reference implementation, not against the spec text.** Every constant was read
off files written by parquet-cpp and then pinned by a test that decrypts those files with our code:
module type numbers, the little-endian ordinal order, the `length | nonce | ciphertext | tag`
envelope, which bytes of a V2 page fall inside the body module, and the one that would have been
guessed wrong -- `compressed_page_size` counts the *encrypted* envelope including its length
prefix, while `uncompressed_page_size` stays the plaintext size.

| Layer | What it does |
|---|---|
| `format/encryption.{hh,cc}` | AES-GCM and AES-CTR over OpenSSL EVP, the AAD builder, module envelopes, `FileCryptoMetaData` |
| `format/parquet_writer.cc` | encrypted-footer mode over page headers, page bodies, dictionary pages, the OffsetIndex and the footer |
| `format/parquet_reader.cc` | `parse_encrypted_footer()` plus an optional `read_crypto` threaded through the decode path |
| `encryption_keys.{hh,cc}` | the `key_source` seam, `key_metadata` shapes, DDL-time key validation |
| `ent/encryption/parquet_key_source.cc` | that seam implemented over `key_provider` — local file, replicated, KMIP, AWS KMS, GCP, Azure |
| `cf_prop_defs` | DDL validation, including the mutual exclusion with `scylla_encryption_options` |
| `migration_manager` | the async DDL-time check that the provider can actually produce a key |

**Keys come from `ent/encryption`'s providers, so BYOK works.** A table says

```cql
parquet = {'encryption': 'aes_gcm_v1',
           'key_provider': 'KmipKeyProviderFactory',
           'kmip_host': 'my-kmip', 'secret_key_strength': '256'}
```

and the options are `scylla_encryption_options`' own vocabulary, forwarded verbatim to
`encryption_context::get_provider()`. No key material enters the schema — only the options that
locate it. That is not a detail: schema properties are replicated to every node, kept in system
tables and printed by `DESCRIBE`, so a key placed in one would be copied into the cluster's
metadata and into every schema dump.

The provider returns a key **and an opaque id**; the id goes into `FileCryptoMetaData.key_metadata`
and is handed back to the provider on read, which is the mechanism rotation needs — a file written
before a rotation would get the key it was written with, not the current one. Some providers (the
local-file one) issue no id at all, because their options alone identify the key; an empty id means
exactly that, and is not an error.

Five failure modes are refused at DDL time — six, counting the mutual exclusion with
`scylla_encryption_options` described below — because each would otherwise surface as a compaction
error hours later:

| refused | why |
|---|---|
| an unknown algorithm | |
| `cipher_algorithm` naming any mode but AES-GCM | the format permits nothing else; see below |
| `secret_key_strength` that is not 128/192/256 | not an AES key length |
| provider options with no `encryption` | an inert security setting reads, in `DESCRIBE` and in review, as if the table were encrypted |
| a provider that cannot produce a key | the sync property check cannot do I/O, so this one hangs off the schema-announcement path in `migration_manager::validate()`, alongside where the sstable EaR path validates its own extension |

The last check is **local only** — another node may not reach the same provider and its writes will
fail there. There is no cluster-wide key distribution here, so this is the strongest check available
rather than the one that would be right.

**The AAD prefix is the table's name**, stored in the file. A `Data.db` moved between tables, or
restored from a backup into a different one, then fails authentication instead of decoding -- which
is the property that makes a stolen file useless even to someone holding the key for another table.

**Two consequences callers have to know.** An encrypted file is **not byte-reproducible**: every
module carries a fresh random nonce, because reusing one under a single key breaks AES-GCM outright.
Any test asserting byte-identical output must leave encryption off. And there is no degraded read
path -- without the key the file does not open at all.

**`AES_GCM_CTR_V1` is supported but is not a good default.** Its page bodies are AES-CTR with no
authentication tag, so tampering with one is not detected by the format. It exists because it is
faster and because other writers produce it.

**`key_metadata` is a deployment choice, and the default is now provider-neutral.** The spec says
only "whatever the reader needs to find the key". Two shapes are supported, selected per table by
`encryption_key_metadata`:

| value | what is written | who can read it |
|---|---|---|
| `provider` *(default)* | the key id verbatim | any provider, **including BYOK** via KMIP/KMS/Azure/GCP; readers using explicit keys |
| `parquet_kms` | parquet-java's "key tools" key-material JSON | additionally pyarrow's Python API and Spark, which decrypt **only** through a KMS requiring that shape |

The first version emitted the JSON unconditionally, which was wrong: that shape encodes one specific
key-management model — a `masterKeyID` plus a wrapped DEK — and a BYOK deployment whose keys live in a
KMIP server or a cloud KMS does not necessarily map onto it. Baking it in bought one reader's
convenience at the cost of assuming its key management for everyone. It is opt-in now, because that
trade belongs to the operator. Both shapes are read back correctly; `key_id_from_metadata()` accepts
either.

**Verified end to end on a running node** (`~/pq-lab/encryption_e2e.py`, 24/24), with the key coming
from `LocalFileSystemKeyProviderFactory` — the only provider that can be driven without an external
service, and the same `key_provider::key()` path the other five take: all six DDL rejections; the
provider options round-trip through `DESCRIBE` with no key material in them; every `Data.db` of a
multi-group table begins and ends with `PARE`; no plaintext value appears in any of them; Scylla
reads all 2 000 rows back including a single-partition read; **pyarrow reads the same raw files with
the provider's key** (2 000 rows across 4 files, values exact); the files refuse to open **without a
key and with the wrong key** — the second being the control that makes the first mean something; and
a second table on the defaults (no `cipher_algorithm`, no `encryption_key_metadata`, so the
provider-id shape with an empty id) encrypts and reads back too, since that is the combination a BYOK
deployment actually writes. The format layer is covered from both directions in
`sstables/parquet/run_tests.sh` suites 19 and 20.

**Per-column keys: written, and not yet interoperable.** The writer now takes a key per column,
encrypts that column's pages, dictionary page and OffsetIndex under it, emits
`EncryptionWithColumnKey` in `ColumnCryptoMetaData`, and moves the `ColumnMetaData` out of the
footer into `ColumnChunk.encrypted_column_metadata`. Verified from three directions:

- our reader round-trips it, values exact, and refuses a wrong column key;
- with the footer key alone, our reader opens the file, reads the footer-key column, and sees the
  other column as present-but-encrypted rather than as a malformed footer;
- **pyarrow, given the footer key alone, reads the footer-key column and cannot touch the
  column-key one** -- which is the partial access the feature exists for, working through a stock
  reader.

But pyarrow given *both* keys still cannot decrypt the column-key column: `Failed decryption
finalization`. Our reader can, and our reader also decrypts parquet-cpp's own per-column files
(§ suite 19), so **the divergence is on our write side and is not yet found.** Ruled out so far: the
AAD module type and ordinals (the conformance test decrypts parquet-cpp's `encrypted_column_metadata`
with exactly the AAD our writer builds), which key encrypts the column metadata (the column's, not
the footer's, again from parquet-cpp's file), and the envelope layout (the 4-byte length prefix is
included, same as ours). Next suspects are `RowGroup.ordinal`, which our writer does not emit at all,
and the field order or presence rules around `ColumnChunk` fields 3/8/9.

**So per-column keys are not exposed through CQL.** A property that produced files only Scylla could
read would defeat the reason for encrypting inside the format, and shipping it as an option while
that is true would be the "setting that lies" failure mode this codebase keeps refusing elsewhere.
The interop suite prints the gap on every run rather than asserting it, and says what to change when
it closes.

**The `ent/encryption` alignment — built, and the conflict is what shaped it.**

The seam was already there. `encryption_context::get_provider(const options&)` returns a provider for
an options map, and `get_key_info(options)` parses `cipher_algorithm` / `secret_key_strength` from the
same map, so the whole of the BYOK entry point is:

```cpp
auto provider = ctx.get_provider(opts);                       // local | replicated | KMIP | KMS | Azure | GCP
auto [key, id] = co_await provider->key(get_key_info(opts), existing_id);
```

**The conflict, which is why the key and the selector come from different places.** Scylla's EaR
encrypts the *whole Data component* through an sstable file-io extension, keyed off the
`scylla_encryption_options` schema extension. So a `pq` table carrying that property is **already**
encrypted at the storage layer — and that file is opaque to every external reader, which is the exact
outcome Parquet Modular Encryption exists to avoid. Driving Parquet encryption from the same property
would double-encrypt *and* still hand out an unreadable file. Hence:

- the **selector** stays in `parquet = {'encryption': ...}`, where it means *this table encrypts
  inside the format*;
- the **key** comes from `ent/encryption`, reusing its options vocabulary (and therefore its BYOK
  providers) without reusing the property that triggers the storage-layer path;
- a table asking for **both** is refused at DDL time, in `cf_prop_defs::validate()` — the one place
  the `parquet` map and the schema extensions are both in hand.

**Deleted:** the bespoke `parquet_encryption_key_file`, the `key_registry` that read it, the
`encryption_key` sub-option that named an entry in it, and the startup block that loaded it. It
reimplemented the local-file provider in miniature and supported none of the others. What survived
from that file is the part that was always about the *format* rather than about key management:
`key_metadata_format`, `make_key_metadata()` and `key_id_from_metadata()`.

**Three things went differently from the plan written here beforehand.**

*The global accessor is installed from `register_extensions()`, not from `main.cc`.* The plan said
`main.cc`, but `register_extensions()` is not called from there — it is called from a `configurable`
in `encryption_config.cc`, which also runs for `cql_test_env` and the offline tools. Installing it at
the point the context is created covers every path that can open an sstable, and there is exactly one
such point.

*The seam is an abstract class, not a direct call.* `sstables/parquet` cannot call `encryption::`
directly: `scylla_encryption` is a CMake target that already links against `sstables` (its file-io
extension takes an `sstables::sstable`), so the dependency cannot be mutual. `sstables` therefore
declares `key_source` and `ent/encryption/parquet_key_source.cc` implements it — which also means a
unit test can install a fake.

*`cipher_algorithm` is defaulted rather than required.* Parquet permits only AES-GCM (and AES-GCM-CTR,
whose metadata modules are still GCM), while `get_key_info()`'s default is `AES/CBC/PKCS5Padding` —
unusable. An **absent** `cipher_algorithm` is therefore defaulted to `AES/GCM/NoPadding`, because
overriding a default is not overriding a request; an **explicit** one naming another mode is refused
outright. The default is re-applied on every parse rather than persisted into the schema, which echoes
what the operator wrote; normally an unwritten default is a hazard, but AES-GCM is the only thing the
format permits, so there is no other value for it to become.

**One known limitation, and it is a real difference from the storage-layer path.** The provider
*options* are read from the **schema**, not from the file — only the key id lives in the file. The
whole-component path stores the options in the sstable's own extension attributes, so it can still
open a file after the table's provider is reconfigured; this cannot. `key_metadata` is a single opaque
string, and packing the option map into it would break the "the id, verbatim" contract that makes the
provider-neutral shape work with providers we have never seen. Changing a table's key provider while
it has encrypted sstables is therefore a one-way door.

**Also not covered:** node-global `user_info_encryption`, which applies whole-component encryption to
every user table without appearing in the table's schema extensions, so the mutual-exclusion check
cannot see it. A `pq` table with format-level encryption on such a node would be encrypted twice. The
check would need `encryption_context` to expose the global extension, which is currently
implementation-private, so it is named here rather than half-built.

**Key rotation is supported by construction but not verified.** Handing the stored id back to the
provider is exactly the mechanism, and it is the same one the whole-component path uses -- but no
rotation has been exercised end to end here, and the one provider this lab can drive
(`LocalFileSystemKeyProviderFactory`) issues no ids, so it cannot demonstrate it. Claiming it works
would be claiming a green run that never happened.

**And it *is* testable here, contrary to the note above.** The obstacle was stated as "the only
provider this lab can drive issues no ids", which is true of `LocalFileSystemKeyProviderFactory` and
not of the others: `replicated_key_provider` returns `encode_id(uuid)` and keeps its keys in a system
table, so it needs no external service and runs on one node (it does need a system key, via
`system_key_directory`). That makes the rotation round trip drivable: write a table under the
replicated provider, force a new key, and assert that sstables written before the rotation still
read — their `key_metadata` carries the old id — while new ones carry the new one. Until that is run,
rotation remains **claimed by construction and unverified**, which is the weakest statement in this
section.

**Also absent**: plaintext-footer mode.

### 10.14 Read-shape telemetry — built 2026-08-19, for a criterion that was then dropped

Two references above point here, so it gets its own section: `single_partition_reads` and
`range_scan_reads`, per table, exposed on the `column_family` metrics group.

They exist because C7 -- a read-pattern gate that would refuse conversion for a table taking mostly
point reads -- could not be evaluated at all: Scylla had no per-table read-shape counter, only
cluster-wide sstable-level ones that cannot attribute a read to a table. Building the counters
removed that objection. **C7 was dropped anyway** (§6.3): having the input is not evidence that it
decides better than C5's column ceiling, and a load-bearing criterion should not rest on an untested
hypothesis about which reads matter.

The counters stay, for two reasons that outlive the criterion. They are what an operator setting
`storage_format` by hand actually needs -- the scope limit in §10.4 is "wide Parquet tables must not
be on a point-read path", and this is the only way to see whether a table is on one. And they close a
gap the doc had asserted was unclosable, which is worth having on record.

Verified across five query shapes: a single-partition read, an `IN` over several partitions, a full
range scan, a token-range scan, and a clustering slice within one partition. The classifier is
structural rather than heuristic -- every partition range in the request is singular, or it is a scan
-- so an `IN` counts as a point read and a one-partition slice does too, which matches what the
metric is for.

**One trap, worth stating because it cost a run.** The counters were first registered in the
per-keyspace `column_family` metrics block, which is gated on
`enable_keyspace_column_family_metrics` and off by default. The increment ran, the metric did not
exist, and the diagnostic that separated the two was noticing that `read_latency_count` moved while
`memtable_switch` -- registered in the same block -- was absent entirely. They are registered in both
blocks now.

### 10.15 `metadata_folding = 'uniform'` stops applying once an sstable cuts a row group

Found while auditing for more instances of the two-write-paths gap in §8.2b, and it is a smaller
finding than the one it was looking for -- but it is operator-visible, so it is written down.

L2 (uniform folding) keeps a single write timestamp in the file footer instead of a per-row column.
The reader requires that key: `mapped_schema_from_footer()` throws *"L2 file without
scylla.uniform_timestamp"* without it. `write_rows()`, the path taken when an sstable fits one row
group, emits it. `cut_row_group()` did not.

That reads like "an L2 table large enough to cut a row group is written successfully and cannot be
read back", which would be serious. **It is not reachable**, and the reason is a second difference
between the same two paths. The cutting path has to fix its leaf set before it has seen all the
rows, so it uses `leaf_set::conservative`, which sets `all_same_ts = false` and turns on every
optional metadata leaf. That breaks L2's precondition, `build_mapped_schema()` falls the level back
to L1, and no uniform timestamp is ever needed. The footer key was missing for a state that cannot
occur.

What is left is a real behaviour, just a milder one:

| sstable | level actually used |
|---|---|
| fits one row group | **L2**, as asked |
| cuts a row group | **L1**, silently |

So the same table is uniform-folded while it is small and row-folded once it is large, with nothing
logged either way. It is not data loss -- L1 is lossless, and both cases round-trip exactly -- but a
setting that quietly stops applying at scale is the same species of problem as the encoding override
that only worked on large tables, and it was found by looking for that species rather than by
anything failing.

Two tests pin it, and they only mean something as a pair: the same schema and the same uniform
timestamp reach L2 without a cut and L1 with one. The footer-key guard is kept in
`cut_row_group()` regardless, because the invariant "an L2 footer carries its timestamp" belongs
next to the code that writes the footer, not next to the leaf-set logic three files away that makes
it moot today. If a later change lets the cutting path reach L2, the guard is already there and the
fallback test is what fails.

**The cost of the fallback is unmeasured**, which is the honest state: §10.3 puts L2 at 13 % smaller
than L1 on Backblaze's 197 columns and within 0.03 % on NYC TLC's 20, so the loss ranges from
irrelevant to material depending on width -- and every table big enough to matter is a table big
enough to cut. Either the conservative leaf set should be able to express L2, or the property should
refuse `uniform` for tables that will cut. That is a design decision, not a bug fix, so it is
recorded here rather than guessed at.

## 11. Open questions

> Deferred work is tracked in **[parquet-future-work.md](parquet-future-work.md)** as of
> 2026-08-18 — the backlog, why each item is not done, and the trap that would catch a first
> attempt. Read-path optimisation is explicitly paused there. The questions below are the design
> questions; that file is the work list.


1. **Row-group boundaries vs. partition boundaries.** Should a partition be forbidden
   from spanning row groups? It would simplify point lookup and split, but a single
   large partition would then force an oversized row group. Leaning: allow spanning,
   record the partition's row-group span in the index.
2. ~~**Promoted index equivalent.**~~ **Answered 2026-08-16: yes.** Because rows are
   written sorted by `(token, pk, ck)`, the Parquet ColumnIndex's per-page min/max over
   the clustering columns provides exactly what the promoted index provides —
   intra-partition seeking — natively. Scylla needs to supply the *partition* index
   (job 1 below); the intra-partition half comes free with the page index.
3. **`sstable_run` semantics.** Does an ICS run mix formats mid-run during convergence,
   or is the run the atomic conversion unit? The latter is cleaner; cost unmeasured.
4. **File streaming.** Is streaming a Parquet SSTable to a peer cheaper than re-encoding
   it? Probably yes; needs the receiver-side feature gate. (This is where tablet
   migration touches the format — but only as one caller of file streaming, not as a
   design input.)
5. ~~**Counters.** Excluded in v1 — is there demand?~~ **Resolved 2026-08-17: implemented**
   rather than excluded, because the conformance suite's shared corpus mandates them and a
   storage format that cannot hold counters is not a general table format. The open part is
   narrower: they currently reuse the collection representation, so a counter column reads
   as `map<blob, blob>` to an external reader. Splitting the shard's value and logical clock
   into named leaves is a schema change worth making before the format leaves experimental.
6. **Does folding Level 2 survive real data?** It requires row-group-uniform timestamps;
   real write patterns may break it often enough to make it useless.
7. ~~**`zstd_with_dicts` inside Parquet**~~ — **Answered 2026-08-17: no.** A trained
   dictionary is worth −1.4 % to +7.1 % on Parquet pages and is negative on held-out data,
   because pages are large and single-column so zstd's window has already captured what a
   dictionary would supply. Not worth losing external readability for. See §10.1e.
9. **Per-column encoding selection.** §10.3f shows type-based rules lose. The encoding
   has to be chosen from the data — trial-encode a sample of each column with PLAIN,
   dictionary, delta and byte-stream-split and keep the smallest. The estimator already
   samples and trial-encodes, so this is an extension of it rather than new machinery.
   Until then, regular columns are PLAIN and only construction-monotonic columns use
   delta.
8. ~~**How should timestamp exceptions be encoded?**~~ **Resolved 2026-08-16.** Replaced
   the per-column leaves with a two-leaf sparse side-channel (`__tsx_mask` bitmap +
   `__tsx_vals` zigzag-varint deltas). Leaf count is now width-independent and the
   worst-case penalty fell from 2.68× to 2.05× — see §10.3c. §5.3 amended.

10. ~~**Streaming reader.**~~ **Done 2026-08-17.** The reader loads the footer alone,
    seeks by the index entry's row ordinal and decodes one row group at a time in 16 384-row
    windows, stepping over pages via the V2 header's `num_rows`. R-13 holds: 8× the rows
    costs 1.13× the peak scan memory (§10.4). What remains is narrower — a point read still
    reads a whole row group's *bytes* from disk to serve a few rows, which is why point
    reads are 120× native rather than the ≤1.2× §4 targeted. The fix is per-column-chunk
    reads driven by the OffsetIndex; see §10.4.
11. **Fragment kinds.** Partially closed 2026-08-17. **Row markers, row tombstones and
    partition tombstones now round-trip** through the real sstable path
    (`test/boost/sstable_parquet_test.cc`), each as an optional leaf group that is
    materialised only when some row uses it — so a table that never deletes pays nothing.
    The marker is stored as a delta against the row's own timestamp, which is zero for an
    ordinary INSERT and costs nothing after zstd.

    **Static rows** landed the same day. They ride as ordinary value columns appended
    after the regular ones, which gets them the whole cell machinery — timestamps, TTLs,
    the divergence channel — for free, and costs almost nothing on disk because a static
    value is constant within its partition and compresses away. The awkward case is a
    partition whose only content is a static row: there is no clustering row to attach it
    to, so the writer emits one placeholder row marked with a `__no_ck` leaf, which is
    cheaper than making every clustering-key column nullable for every table.

    **Range tombstones** landed the same day. They are fragments *between* rows rather
    than attributes of one, so they are carried as marked rows that keep their place in the
    clustering order: the clustering columns hold the bound's prefix, `__rtc_len` says how
    much of that prefix is real, and `__rtc_w` / `__rtc_reg` restore the bound weight and
    partition region. Presence of `__rtc_w` is what marks the row, because a weight of zero
    is legitimate. `__rtc_ts` absent means the change closes a range.

    **Multi-cell collections landed 2026-08-17**, as Dremel MAP groups per §5.2 rather
    than opaque blobs:

    ```
    optional group <col> (MAP) {
      repeated group key_value {
        required binary key;  optional binary value;
        required int64  __ts; optional int32 __ttl; optional int32 __ldt;
      }
    }
    ```

    Keys and values stay serialised, which is what lets one code path serve sets, lists,
    maps and non-frozen UDTs alike. Per-element timestamps, TTLs and dead elements live
    inside the group; the collection-wide tombstone is a row-level pair, because it belongs
    to the row. Five states are distinguished and all are tested: absent,
    present-but-empty, populated, populated with a dead element, and deleted-and-empty.
    Absent versus present-but-empty differs only by a definition level, and conflating them
    resurrects a collection the user cleared.

    Getting there needed the format library to support Dremel at all — it emitted definition
    levels only. That work is validated in both directions against parquet-cpp: leaf levels
    against pyarrow's own `max_definition_level`/`max_repetition_level`, reading a pyarrow
    `list<string>`, and writing one for pyarrow to read back (suites 15–17).

    **Counters — implemented 2026-08-17, which retires the last exclusion.** They are
    atomic cells whose value is a set of per-replica shards, and merging two counter cells
    means merging shards by id rather than taking the newer value. Stored as an opaque blob
    they would still read back byte-identical from a single sstable while being wrong the
    moment anything merged them — the failure mode that only appears after compaction.

    They reuse the collection representation rather than getting their own: one element per
    shard, keyed by the shard id, with the shard's value and logical clock packed into the
    element value as two big-endian `int64`s. The cell's timestamp is repeated on each
    element — identical across them, so it compresses away — and a dead counter cell becomes
    an element-less collection carrying the deletion in the collection-tombstone slot, which
    is what distinguishes *absent* from *deleted*. On the way back the shards go through
    `counter_cell_builder::add_maybe_unsorted_shard` and are re-sorted: `counter_cell_view`
    requires them ordered by id, and although our writer emits them already ordered, relying
    on that would make the reader depend on an invariant it does not enforce.

    **Resolved 2026-08-19: the leaves are typed.** A counter's group now carries `value` and
    `clock` as two `INT64` leaves instead of one packed 16-byte `BYTE_ARRAY`, so any Parquet reader
    sees a count and a logical clock rather than an opaque blob.

    **Why this turned out to be cheap.** The plan had been a group nested inside the MAP value — a
    third level of Dremel nesting — and that was the reason for deferring it. It was unnecessary:
    this writer's `key_value` group already carries **five** children (`key`, `value`, `__ts`,
    `__ttl`, `__ldt`) rather than the two a strict MAP has, so the shape is already non-standard in
    exactly the way that makes a sixth sibling free. Adding `clock` alongside is no further
    departure. Two hours of deferral rested on not having read the group's arity.

    The sixth leaf is **appended after `__ldt`** rather than placed next to `value`, so every
    existing `vcol + N` offset in the shred and reassemble paths keeps its meaning. Schema order is
    cosmetic here — a reader sees the siblings of one group either way — and renumbering those
    offsets is the change most likely to go wrong silently.

    Above the storage layer nothing changed: `read_counter_cell()` still produces a packed pair and
    the reassembler repacks one, so the counter rebuild path is untouched. Covered by the existing
    `test_pq_counters_round_trip`, which drives real counter cells through the writer and reader,
    and by `test_pq_declares_the_counter_convention`, which asserts both leaves are `INT64` in the
    written footer.

    **Also still declared in the metadata.** `cql_column` gained a
    `counter` flag — until then nothing downstream could distinguish a counter from a collection,
    since both are `multi_cell` — and the footer carries `scylla.counter_columns` naming them plus
    `scylla.counter_encoding` describing the packing: 16-byte shard id as two big-endian int64s
    (UUID msb, lsb), 16-byte value as two big-endian int64s (value, logical_clock). Emitted on both
    write paths, and pinned by `sstable_parquet_test/test_pq_declares_the_counter_convention`.

    **This is a lesser fix than typed leaves and does not replace them.** What it buys is that
    someone opening the file with `parquet-tools` can discover what those sixteen bytes are instead
    of having to already know. What it does not buy is a reader being able to *interpret* them
    without special-casing Scylla.

    The honest cost of reusing the collection shape: **a counter column is not
    self-describing to an external reader.** It appears as `map<blob, blob>` where the value
    blob is two packed integers, rather than as a group with named `value` and `clock`
    leaves. Splitting it into proper leaves is a schema change, not a data change, and is
    the right thing to do before the format is anything but experimental. What it buys today
    is that counters ride an already-tested pipeline — the same shred, page, and reassemble
    path as every collection — rather than a second nesting implementation written from
    scratch.

    Counter **updates** — the pre-shard-transformation form — remain unrepresentable and
    throw. Those never reach storage; a cell still in that form is an upstream bug, not a
    representation gap.

    **`pq` is in `all_sstable_versions` and `writable_sstable_versions` as of 2026-08-17,**
    and clears all **34** sub-tests of `sstable_conforms_to_mutation_source_test`
    (`test_sstable_conforms_to_mutation_source_pq_small`). The two arrays are coupled:
    `check_sstable_versions` requires every version at or after
    `oldest_writable_sstable_format` to appear in `writable_sstable_versions`, and `pq` sorts
    after `mc`, so it cannot be readable-but-not-writable. The
    `static_assert(writable_sstable_versions.size() == N)` in the conformance test — whose
    whole job is to make someone notice — went from 5 to 6.

    ### What the enrolment experiment found that unit tests had not

    Enrolling `pq` and running the suite was worth more than reasoning about what was
    missing. Besides the clustering-slice bug and the forwarding bug recorded above, it
    surfaced two defects that every targeted test had passed straight over. Both are now
    covered by `test_pq_statics_survive_a_leading_range_tombstone`, and both fixes were
    mutation-checked — broken deliberately, confirmed failing, restored.

    1. **An absent clustering prefix is not an empty one.** The bounds that cover a whole
       partition — `before_all_clustered_rows` and `after_all_clustered_rows` — are an
       *empty but present* clustering prefix carrying bound weight −1 or +1
       (`bound_view::bottom()`/`top()`). The reader rebuilt a range-tombstone bound with
       `prefix_len == 0` as an **absent** prefix, which is not a valid clustered position.
       Comparing one against those bounds does not fail — it silently yields nonsense: the
       position compared as less than *neither* sentinel, which sent the
       `clustering_ranges_walker` past every range, and from there the filter answered
       `ignore` for every row. The symptom was a partition returning its static row and no
       clustering rows at all.

       The diagnostic lesson is the one worth keeping: the position *printed* as
       `{position: clustered, null, -1}`, which reads exactly like `before_all_clustered_rows`
       and is why reading the code repeatedly failed to find it. What settled it was printing
       the comparator's answers — `less(pos, after_all)` and `less(pos, before_all)` were
       *both* false, which is impossible for any valid position, since the sentinels are
       ordered with respect to each other.

    2. **Static collections were dropped whenever a partition's first row was not a
       clustering row.** The writer replays static content onto every row, and the reader
       rebuilds the static row from whichever row it sees first — so the first row's identity
       is load-bearing. Two shapes make it something other than a clustering row: a range
       tombstone change opening before all rows, and the placeholder row emitted for a
       partition with no rows. Both replayed `_static_cells` and not `_static_collections`,
       so every static collection vanished in exactly those shapes. The atomic static cells
       came back, which is what made it look like a collection-encoding bug rather than a
       replay bug. The three replay sites are now one `replay_statics()` helper, because
       three copies of a two-line loop is how the omission happened.

    The placeholder case was the worse of the two: a partition whose only content was a
    static collection produced a placeholder row carrying nothing at all, even though the
    guard deciding whether to emit that placeholder explicitly tested
    `_static_collections`.

    **Consequence for tiering.** C5's schema-eligibility gate (§6.3) previously declined
    counter tables and tables with non-frozen collections. Both are now representable, and
    every remaining type falls back to an opaque blob column that round-trips because the
    bytes are what Scylla stores anyway — so no schema is currently ineligible and
    `schema_is_parquet_eligible` returns a constant. The gate is kept rather than deleted:
    it is where a future encoding gap belongs, and refusing a schema is how the policy avoids
    silently mangling one.

    ### The real remaining blocker: statistics and metadata parity

    With the mutation model complete, `pq` was enrolled in both version arrays and the
    **generic** sstable suites run — not just the conformance one. That is a different and
    weaker gate than conformance, and it is where `pq` still fails. `sstable_datafile_test`
    reports roughly a dozen failures, and they share one cause: **the pq writer populates
    almost none of the Statistics metadata.** It feeds its `metadata_collector` only
    `add_key()`. mx additionally feeds:

    - `update_min_max_components(position)` — at the partition tombstone (both sentinel
      bounds), every clustering row, and every row marker. This is what produces the min/max
      clustering key range.
    - a per-partition `column_stats` via `_collector.update(...)`: the timestamp tracker, the
      *min live* timestamp and *min live row marker* timestamp trackers, the local-deletion-time
      and TTL trackers, the tombstone drop-time histogram, and the row / cell / range-tombstone
      / dead-row counts.
    - `add_compression_ratio(...)`, and `get_ext_timestamp_stats()` at seal time.

    The failing assertions name exactly those fields: `min_max_clustering_key_test`,
    `sstable_tombstone_metadata_check` and its two composite variants,
    `sstable_tombstone_histogram_test`, `sstable_timestamp_metadata_correcness_with_negative`,
    `test_sstable_max_local_deletion_time`, `test_may_have_partition_tombstones`,
    `sstable_partition_estimation_sanity_test`, `test_sstable_bytes_on_disk_correctness`,
    `sstable_run_clustering_disjoint_invariant_test`, `sstable_reader_with_timeout`, and
    `find_first_position_in_partition_from_sstable_test` — the last of which needs a pq path
    in `sstable::find_first_position_in_partition` rather than a collector call.

    **This is correctness, not bookkeeping.** The min/max timestamp and local-deletion-time
    trackers and the tombstone drop-time histogram are what tombstone garbage collection and
    compaction decisions read. An sstable that under-reports them can have a tombstone
    dropped while data it shadows is still live. That is precisely the class of silent
    resurrection bug this project has twice been bitten by, so the parity work should be done
    with the same "watch it fail first" discipline, not inferred from the tests going green.

    Two more things enrolment turned up that are worth writing down, because they are traps
    rather than gaps:

    - Several `sstable_datafile_test` cases loop `all_sstable_versions` to open **checked-in
      reference sstables** under `test/resource/sstables`. Those fixtures exist for the kl and
      m families only, so enrolling `pq` makes them look for a `pq-1-big-TOC.txt` that was
      never generated. Those loops need an explicit "has a reference fixture" guard; the right
      answer is not to generate pq fixtures, since that would only test our writer against our
      own reader, which the pq suites already do directly.
    - `sstable_datafile_test` has three failures at baseline in this environment
      (`datafile_generation_16_gs`, `test_sstable_bytes_on_{gs,s3}_correctness` — they pull a
      docker image) plus `test_small_sstable_has_reasonable_memory_usage`, which measures
      allocator growth and passes alone but not after the rest of the suite has run. Verified
      by stashing the whole change and re-running: the baseline fails the same four. Worth
      knowing before attributing them to the format.

    **Meanwhile the conformance guarantee does not depend on membership.**
    `test_sstable_conforms_to_mutation_source_pq_small` calls
    `test_sstable_conforms_to_mutation_source(sstable_version_types::pq, ...)` directly and
    passes all 34 sub-tests with `pq` absent from both arrays. So that test stays enrolled and
    keeps the mutation-model guarantee locked in, while the arrays wait for metadata parity.

12. ~~**Statistics and metadata parity (the blocker for `all_sstable_versions`).**~~
    **Done 2026-08-17, and `pq` is now in both version arrays.** The writer maintains a
    per-partition `column_stats` with mx's exact semantics (`collect_atomic_cell` /
    `collect_cell` / `collect_marker` in `writer_impl.cc`, mirroring mx's `write_cell()` and
    `write_liveness_info()`) and calls `update_min_max_components()` at the partition
    tombstone's two sentinel bounds, every clustering row and every range-tombstone change.
    `sstable_datafile_test` went from roughly a dozen failures to its pre-existing baseline
    for this environment, and the whole generic battery plus all 34 conformance sub-tests
    pass with `pq` enrolled.

    Enrolling it turned up two things that mattered more than the statistics:

    **a) A dead cell was silently dropped in L1 and L2 — the default folding levels.**
    L0 carries a per-column `__live_` flag; L1 and L2 do not, so deadness has to be read off
    the `__ldt_` leaf. The reassembler bailed first:

    ```cpp
    if (!present) { continue; }     // no value -> assumed absent
    ```

    so every deleted cell in an L1 file came back as a cell that had *never been written*.
    That is the worst shape this format can fail in: the file is valid, the read succeeds,
    and the deletion stops shadowing what it was hiding, so the old value reappears on the
    next merge. The discriminator is now three-way — value present means live, no value with
    an `__ldt_` means **dead**, neither means absent — and `any_deletion` is set by `!c.live`,
    so the leaf was always on disk. The information was there all along and simply was not
    being read.

    **The losslessness suite had been asserting the bug as correct behaviour.** Its check read

    ```cpp
    const bool keeps = (lvl == folding_level::verbatim) || (c.live && c.v);
    ```

    and reported a *preserved* dead cell as `"dead cell resurrected"` — the concept exactly
    backwards, since dropping a dead cell is what resurrects data. The expectation had been
    written to match what the reassembler did rather than what R-6 requires, which is how a
    real data-loss bug looked correct across 540 generated cases. The check now requires every
    cell to survive every lossless level and compares liveness and deletion time as well;
    reverting the fix now reports `"cell lost"`, which is the honest diagnosis. §10.3a's
    "losslessness proven over 540 cases" was therefore overstated for deletions until now.

    **b) Enrolling `pq` made Parquet the default format for the whole node.**
    `get_highest_sstable_version()` returned `all_sstable_versions.back()`, and `pq` sorts
    last, so every one of its 31 callers — including every test that creates an sstable
    without naming a version — silently started writing Parquet. It now skips `pq`, which is
    the same principle `implies_mx_generation()` already encodes: `pq` is a different format,
    not a newer generation of the native one, and it stays opt-in per table.

    Still missing, none of it blocking: `partition_size` / `start_offset` stay 0 (byte offsets
    do not exist per partition — the Parquet image is encoded once at end of stream, so a
    partition has no on-disk length while it is being consumed; this only feeds the
    estimated-partition-size histogram), `add_compression_ratio()` is not called (pq carries
    the CRC component set, not CompressionInfo), and
    `sstable::find_first_position_in_partition` has no pq path.

13. **`__ts` cannot use DELTA_BINARY_PACKED yet.** The dropped-encoding-hints regression is
    fixed (§10.1g) and key columns are delta-encoded again, but asking for it on the folded row
    timestamp still makes `test_pq_corpus_shaped_schema` fail: write timestamps near `int64`'s
    minimum come back with the top bit relocated, `-2**63 + 74` reading back as `2**57 + 74`.
    The codec is not the cause — it round-trips those values exactly and UBSan-clean — and leaf
    order is asserted, so the interaction is still unaccounted for. Currently left on PLAIN,
    which costs nothing (2 456 bytes against 3 772 on delta). Also still open: dictionary
    encoding is attempted only for `byte_array`, so numeric columns get no dictionary, which is
    pyarrow's whole advantage on low-cardinality numerics.

    Superseded (fixed 2026-08-17): ~~**Preferred column encodings are dropped by the writer.**~~ `schema_mapping.cc`
    sets `column_spec::preferred = delta_binary_packed` for `bigint`/`timestamp` key columns
    and for `__ts`, and `parquet_writer.cc` honours it — but `write_rows()` builds the writer
    from `ms.tree`, not `ms.columns`, so the hints never arrive. Every column comes out
    `PLAIN`. Measured cost on a time-series table: our writer produces 2 612 496 B where
    pyarrow produces 1 961 695 B, and delta-encoding the clustering key alone would give
    ~1 469 167 B (§10.1g). Fix: thread per-leaf hints alongside the tree. Also worth doing at
    the same time: dictionary encoding is attempted only for `byte_array`, so numeric columns
    get no dictionary either.

14. **Deriving `pq_writer_config` from the table.** `parquet::make_writer` uses defaults
    (L1, sparse exceptions). §6 specifies table-level control of folding level and row
    group sizing; wiring the schema properties through to the writer is not done.
15. **Should `row_group_buffer_bytes` vary by shape?** (Reframed 2026-08-18 — the original
   form, "`rows_per_row_group` should scale inversely with leaf count", is answered in §10.1f-rg: it
   buys ~4 points on a sparse wide table and nothing on a dense one, because there the byte budget
   binds first.) The effective row-group size is `min(rows_per_row_group, what 64 MiB of shredder
   memory allows)`, and the second term varies enormously across the corpus because it depends on
   row density. The hard part is that this budget exists to stop a shard OOMing (R-13), so it
   cannot simply be raised for size. Superseded rationale follows.
   **`rows_per_row_group` should scale inversely with leaf count.** A single default cannot suit
   both ends of the corpus. Every row group writes a column-chunk header plus statistics per
   leaf, so at the 5 000-row default a 300 000-row table has 60 row groups and pays that fixed
   cost 11 940 times on a 199-leaf table against 420 times on a 7-leaf one. Measured
   consequence (§10.1f-prod): moving from one row group per file to 5 000 rows costs 0.7 points
   on 22-leaf NYC TLC and **18.6 points on 199-leaf Backblaze**, which is the difference between
   a 23 % win and a 4 % one. A rule of the shape `rows = clamp(target_chunk_bytes * leaves⁻¹)`
   would equalise the metadata overhead across shapes; the target has to be chosen against
   point-read latency, which is what the row count buys. Until then, wide tables should be given
   a larger `rows_per_row_group` explicitly via the `parquet` property (§8.2).
16. **C2 should gate on rows, not bytes.** Its purpose is "enough row groups to amortise the
   per-row-group metadata", which is a row count. Expressed in bytes it is shape-dependent: four
   row groups is 126 kB at ISD-Lite's 6.3 B/row and 1.4 MB at Backblaze's 69 B/row, so any single
   byte threshold admits under one row group on a wide table while correctly excluding it on a
   narrow one. The fix is `rows >= 4 x rows_per_row_group` in `tiering_inputs`, fed from sstable
   stats. The 256 KiB byte floor set in §10.1f-c2 is the interim form.

## 10.9 Downgrade: what happens, and the procedure

This was a "not started" GA gap. It is now specified, and the failure behaviour it rests on is
observed rather than inferred.

**What an older node does when it meets a `pq` sstable: it refuses to start.** `parse_path()` returns
an error for a version string it does not know, `sstable_directory` turns that into
`throw_malformed_sstable_exception`, and the node aborts during the directory scan. Verified on
2026-08-19 by planting a file with an unrecognised version prefix in a live table directory and
restarting the node:

```
ERROR sstable - malformed sstable error (aborting): invalid version for file
zz-...-big-Data.db. Name doesn't match any known version.
```

The node exited; it did not skip the file. That is the behaviour worth having — the alternative,
silently ignoring an sstable it cannot parse, would present as data loss. (The check was made with an
unknown version prefix rather than an actual older binary, which exercises the same code path: to a
binary that predates `pq`, `pq` *is* an unknown version.)

**So a downgrade is safe but not automatic.** Every `pq` sstable must be gone before the old binary
starts. The cluster feature `PARQUET_SSTABLE_FORMAT` prevents the property being *set* in a
mixed-version cluster (`cf_prop_defs.cc`), which stops the problem being created during a rolling
upgrade — it does nothing about files that already exist.

### Procedure

1. **Find the tables.** `SELECT keyspace_name, table_name, storage_format FROM system_schema.tables
   WHERE storage_format != 'sstable'` — plus any table on TWCS with `'hybrid'`, which is Parquet
   whether or not the property says `'parquet'` (§6.3).
2. **Set them back.** `ALTER TABLE ks.t WITH storage_format = 'sstable'` on each. New flushes and new
   compaction outputs are native from that moment.
3. **Rewrite what exists.** `nodetool upgradesstables -a ks t` on **every node**. Not optional and
   not automatic: conversion happens on rewrite, and a closed TWCS window holding one sstable is
   never rewritten by compaction on its own (§10.7). This is the step that takes the time — it is a
   full rewrite of the table.
4. **Verify per node, not per cluster.** `GET /column_family/storage_format/{ks:table}` reports
   `native_sstables`, `parquet_sstables`, `parquet_fraction` and `converged` for the node it is asked.
   Every node must report `converged: native`. A cluster-wide count is not enough — convergence is a
   per-node property, which is exactly what §10.8 measured.
5. **Then downgrade**, one node at a time as usual.

### Notes

- **Snapshots and backups taken while the table was Parquet still contain `pq` files.** Restoring one
  onto a downgraded node fails the same way, at startup. A downgrade plan has to cover the backup
  retention window, not just the live data.
- **Reverting is tested**, not assumed: `cql_ddl_test/test_storage_format_converts_on_compaction`
  drives native → Parquet → native and reads every key back after each switch. It exists because
  un-conversion is the direction nobody checks.
- **There is no partial state to worry about.** Native and `pq` sstables coexist correctly by design
  — that is what hybrid mode is — so an interrupted step 3 leaves a mixed table that the next run
  finishes. No data is at risk at any point during the procedure; the risk is entirely in starting the
  old binary before step 4 says it is safe.

## 11.1 GA readiness — executed, and remaining

A single place to answer "is this shippable, and what is missing". The deck carries the same content
from `GA_DONE` / `GA_GAPS` in `~/pq-lab/deck_data.py`; this section is the prose form.

### Executed

| Area | State |
|---|---|
| Format integration | `pq` enrolled in the writable version set, TOC/index/filter components kept, `data_size()` semantics settled. Round-trips static rows, range tombstones, multi-cell collections, counters. |
| Format control | `storage_format` per-table property: validated, persisted, in `DESCRIBE`, acted on by compaction, with a `parquet = {...}` parameter map. Converting *back* is tested, not only forward. Per-column encodings are settable as CQL enums (§8.2b), rejected at DDL time when they cannot apply, and cancellable with `auto`. |
| Hybrid tiering | Policy wired into `compaction_manager`. C1 bottom tier, C5 column ceiling, C6 measured gain over real data. Every decision logged with the deciding criterion. C6 fails closed. |
| TWCS is all Parquet | Under TWCS, `'hybrid'` ≡ `'parquet'`; verified live against an ICS control (§6.3). |
| Automatic convergence | Flushes write Parquet where the table writes Parquet unconditionally (§10.7). |
| Multi-node | 3 nodes, RF=3: independent per-replica convergence, `CL=ALL` and per-replica `CL=ONE` reads, repair across Parquet replicas, bootstrap streaming onto a new node. |
| Bounded memory | Writer streams row groups: 98 % → 19 % buffered, byte-identical output. Scan memory asserted flat (1.01× for 8× rows). |
| Size accounting | Mixed candidate sets bucket on `ondisk_data_size()`; all-native sets untouched (§10.3i). |
| Interoperability | 16 shapes read by pyarrow and DuckDB — which is what caught the MAP annotation bug. |
| Scale | GB-scale three-way measurement on real NOAA ISD-Lite under TWCS (§10.6). |

### Remaining

| Area | Severity | What is missing |
|---|---|---|
| Point-read latency | known | **1.8–3.6× the row format cold**, at 8 M rows with the footer cache in place (§10.24), down from 5.4–23.6×. Warm reads are at the client round-trip floor for every format. What remains uncovered is the *first* read of an sstable after a restart, which still walks the whole footer: §10.23's side index is specified, not built. Parquet is still for scanned data. |
| **Scan path** | **known, and it is the format's justification** | **2.3× the row format through CQL** at 8 M rows and the shipping defaults, where the format itself scans at parity (§10.26). The reader takes the point-read path — 512-row windows, per-window page fetch of every leaf — for any *bounded* partition range, and the coordinator splits every range scan at tablet boundaries, so no client scan reaches the streaming path. 460 MB read through a 23 MB file for one aggregate. Column projection is also not pushed down on any path. Two bounded fixes, both specified in §10.26, neither built. |
| Production-scale re-measurement | **done for point reads and scans** | §10.21 re-measured point reads at 8 M rows with a canary and `BYPASS CACHE`, and retired every warm ratio in §10.4 as a measurement of the client round trip. §10.24 then measured the footer cache against a squeezed-cache control on the same data. §10.26 did the same for scans, and it moved a conclusion rather than scaling one. Write figures are still from 200–300 k-row sstables. |
| C6 skipped under TWCS | accepted | A schema Parquet stores worse converts anyway; the 197-column sparse shape is 208 % of its SSTable. `storage_format = 'sstable'` is the only guard, and it is manual (§6.3). |
| Corpus re-measurement | **done** | Re-ran all eight datasets under the deterministic dictionary (§10.16): where the input is identical nothing moved, and the two near-parity rows moved least. The two that moved did so because their input changed, not the dictionary. |
| Mixed-format bucketing at scale | **partly measured** | Mixed sets confirmed to arise under ICS + `'hybrid'` at 4 M rows (12 `me` + 3 `pq` live, all rows readable), and Parquet took 0 % of rewrites (§10.19). But Parquet is 6.3 % of the bytes over a two-minute window, so that zero is consistent with correct bucketing *and* with no opportunity. Settling it needs a differential against the fix disabled. |
| Backblaze 4× anomaly | **localised, not solved** | Mechanism found (§10.18): the per-cell tombstone/exception channel is sometimes stored (60 MB across 407 synthetic leaves) and sometimes collapsed (2.7 MB). Not batch-only as previously claimed -- 1 in 7 standalone passes, 2 of 2 batch runs. Why it collapses is open. No published figure depends on it. |
| Encryption at rest | **done, including BYOK** | Built and verified end to end (§10.17): AES_GCM_V1/AES_GCM_CTR_V1, encrypted footer, and **keys from `ent/encryption`'s own providers** — local file, replicated, KMIP, AWS KMS, GCP, Azure — so BYOK works. Mutually exclusive with `scylla_encryption_options`, refused at DDL time, because that path would double-encrypt and hand out a file no external reader can open. pyarrow reads the encrypted `Data.db` with the provider's key. Not covered: key rotation (unexercised), per-column keys (written but not yet interoperable), plaintext-footer mode, node-global `user_info_encryption`. |
| Object storage | **done** | 7/7 on minio (§10.12): keyspace on S3 storage, `pq` table on it, objects in the bucket carrying `PAR1`, all rows readable. The downgrade sub-question is now closed too (§10.20): a binary that does not know `pq` refuses to start on a bucket written by one that does, naming the version. |
| Maintenance tooling | **done** | 10/10 (§10.12): scrub in all four modes, cleanup, and the snapshot → truncate → refresh round trip. |
| Downgrade procedure | **done, both storage types** | Specified in §10.9, and the behaviour it rests on is observed on local disk *and* on object storage (§10.20): an older node aborts at startup on an sstable version it does not know rather than skipping it. The procedure is manual and covers the backup retention window. |


**The honest summary.** The functionality is built and, where the claim is behavioural, verified on
a running node rather than only in a unit test. What is left is genuinely narrow:

- **One conclusion that has already changed, and it is the headline one**: scans. §10.26 measured a
  whole-table scan through CQL and it is **2.3× the row format**, not the 0.82× §10.4c reported and
  the deck quoted. The format scans at parity; the reader takes the point-read path for any bounded
  range and the coordinator never hands it an unbounded one. This is the one remaining item that
  bears on whether the format is worth shipping, because scan performance is its justification — and
  it is an implementation defect with a named cause, not a property of Parquet.
- **One accepted trade**: TWCS converts without a gain check, so a schema Parquet stores worse
  converts anyway. `storage_format = 'sstable'` is the only guard and it is manual.
- **One half-measurement**: mixed-format bucketing at scale showed the right shape but over too
  small a share of the bytes to be evidence (§10.19).
- **One localised-but-unexplained anomaly**: Backblaze's per-cell metadata channel sometimes
  collapses and sometimes does not (§10.18). No published figure depends on it.

Everything else on the old list is closed. Encryption at rest is built as Parquet Modular Encryption
with its keys taken from Scylla's own encryption-at-rest providers, so BYOK works, and verified end to
end including an external reader opening the encrypted file with the provider's key (§10.17) — the
earlier "not applicable to this tree" answer conflated a standard format feature with Scylla's own
EaR, and was wrong about both: the format feature was available, and `ent/encryption` was there all
along. Downgrade safety now holds for **both**
storage types, observed rather than reasoned (§10.20). The corpus survives the deterministic
dictionary unchanged (§10.16). Object storage and maintenance tooling are done and verified.

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
