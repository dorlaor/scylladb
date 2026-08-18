# Parquet storage format — deferred work

Companion to `parquet-storage-format.md`, which is the design and the evidence. This file is
only the backlog: what is deliberately not done, why, and what a person picking it up needs to
know that is not obvious from the code.

Every item names the trap that would catch a reasonable first attempt, because in this project
that has been the expensive part rather than the implementation.

Last reviewed 2026-08-18.

---

## Read-path performance — closed for now

Point-read p50 went **1 915 µs → 581 µs (3.3×)** across four changes (§10.4c, 10.4f, 10.4i, and
dictionary paging), for about 14 % of size. Further optimisation is **explicitly paused** — the
remaining items are recorded, not scheduled.

### 1. Footer metadata cache — specified, not built
**Why deferred:** needs a format-layer refactor, and the obvious implementation is wrong.

Lazy footer parsing (§10.4k) shrank the cacheable object from tens of megabytes to ~128 kB but
only bought 12 %, because TCompactProtocol has no length prefixes so `skip()` is O(content) — a
lazy parse still walks every byte it intends to ignore. Caching is what removes the walk.

**The trap:** per-sstable state lives in `shareable_components`, declared
`foreign_ptr<lw_shared_ptr<shareable_components>>`. Caching parsed metadata there and calling
`materialise_row_group()` on it per reader mutates shard-foreign state. Single-shard testing —
which is all the perf harness does — will not show it.

**Shape that works:** cache the lazily-parsed `file_metadata` plus footer bytes, immutable after
construction (both are pure functions of an immutable file, so the cache dies with the sstable and
needs no invalidation); keep materialised column metadata in per-reader state, one entry for a
point read.

**Bulk of the work:** `format::read_row_range()` indexes `row_groups[rg].columns` itself, so the
column list must be threaded through the format-layer read functions as a parameter. Mechanical.

**Expected, not measured:** on the 8 000-row-group sstable of §10.4j, footer parse per read goes
from ~34 ms to ~0.

### 2. `decode_cpu` — 279 µs, largest remaining phase at test scale
Needs sub-splitting into page-header parse versus value decode before anything is attempted, on
the §10.4h precedent: phase boundaries chosen for convenience hid the real split once already.

### 3. Re-measure the whole of §10.4 at production sstable size
**This is the most important item in this section.** Every latency figure was measured on a
100 000-row, 1.27 MB file — two to three orders of magnitude smaller than a bottom-tier sstable.
§10.4j showed that footer parse alone reorders the whole ranking at realistic sizes. The fixes are
real; the *priorities* derived from them are not trustworthy at scale.

### 4. I/O queue depth from batched reads — watch, do not pre-empt
§10.4i replaced 35 serial reads with one concurrent batch. That multiplies queue depth by leaf
count under concurrent point reads. Nothing here measures a loaded node. If it surfaces as
queueing, cap the batch; do not return to serial.

---

## Format gaps

### 5. `DELTA_BYTE_ARRAY` is unimplemented
The encoding that would help exactly where Parquet does worst: HackerNews at 82.5 % and Wikipedia
pageviews at 90.0 % are both dominated by near-unique text (§10.1f-prod). Prefix-sharing between
adjacent sorted strings is the one mechanism that attacks that, and it is unavailable.

### 6. Counter shard leaves are not self-describing
A counter cell's shards are written as one element per shard, but the leaf carries no marker
saying so, which makes the encoding readable only by something that already knows the convention.

### 7. Statistics metadata is thinly fed
`metadata_collector` gets keys, min/max components and `column_stats`. Compaction and tombstone GC
read more than that. Correctness-adjacent rather than cosmetic, and untested at present.

---

## Hybrid tiering — trimmed to three criteria, further items open

The decision function is **C1 (bottom tier), C5 (width in columns), C6 (measured gain)**. C2, C3,
C4 and C7 were removed. The pattern: three of the four were thresholds nobody had measured, and
the fourth could not be evaluated at all. **Any proposal to add a criterion should come with a
measurement, not a rationale.**

### 8. C2's replacement should gate on rows, not bytes
C2 was removed as subsumed by C6, but if a size floor is ever wanted again it must be a row count:
four row groups is 126 kB at 6.3 B/row and 1.4 MB at 69 B/row, so no byte threshold suits both.
`rows >= 4 × row_group_rows`, fed from sstable stats.

### 9. `row_group_buffer_bytes` per shape
The effective row-group size is `min(row_group_rows, what 64 MiB of shredder memory allows)`, and
which term binds depends on row *density*, not column count (§10.1f-rg). On dense wide tables the
byte budget binds and the row count is inert. Harder than it looks: this budget exists to stop a
shard OOMing (R-13), so it cannot simply be raised for size.

### 10. C7 needs read-path instrumentation
The criterion that should refuse a wide table on a point-read path cannot be evaluated: no counter
separates point reads from scans, and `live_scanned` is an unpopulated Cassandra-compatibility
stub that would have silently reported zero — i.e. "not point-read dominated" — and converted
exactly the tables C7 exists to protect. C5's column ceiling is the stand-in. Fixing it means two
counters where single-partition and range queries enter `replica::table`, exposed through
`compaction_group_view`, plus a `storage_format = 'auto'` enum value with persistence and a
round-trip test. Small in lines, large in blast radius.

---

## Accepted limitations — not to-do items

- **Large partitions overshoot the row-group budget.** Cuts happen only at partition boundaries,
  so one oversized partition stays whole. Splitting would put `(row group, ordinal)` in every
  index entry and make point reads span row groups: complexity on every read to bound a rare case.
  The partition stays in one file, by decision.
- **Wide tables are refused conversion even when only scanned.** C5 bounds columns, which is
  cruder than C7's read-pattern gate; a scan is where Parquet is *fastest* (0.82× native). This
  false negative is the price of not instrumenting the read path.

---

## Housekeeping

### 11. Decks describe seven criteria
`~/pq-lab/deck_data.py` and the generators still present C1–C7. v2.1 was published before the trim
to three. Bump to v2.2 when refreshed; the version lives in `~/pq-lab/deck_version.py` and is
shared by all four generators so they cannot disagree.

### 12. Environment traps worth knowing
- A rebuilt binary does not replace a running node. `~/pq-lab/ensure_fresh_node.sh` is a
  precondition on all measurement scripts for this reason; it cost real debugging time three times
  before it existed.
- The lab keyspace needs `NetworkTopologyStrategy`; `SimpleStrategy` is rejected on this node.
- `sstable_compaction_test` segfaults after ~68 cases in this environment. Verified identical on a
  stashed baseline, so pre-existing.
