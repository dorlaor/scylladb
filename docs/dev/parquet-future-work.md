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

## Size accounting: `pq` sstables report a different unit — **open, and it affects core compaction**

`sstable::data_size()` returns the *uncompressed* data length when a `CompressionInfo` component
exists, and the file size otherwise. A `pq` sstable has no `CompressionInfo` — Parquet compresses
internally — so for it `data_size() == ondisk_data_size()`, while a native compressed sstable
reports several times more than it occupies. `get_compression_ratio()` likewise returns
`NO_COMPRESSION_RATIO` for `pq`.

**Fixed in this project's own code:** C1's bottom-tier rule compared `data_size()` across
candidates, which in a hybrid table — where both formats are present by definition — compared
mixed units. The same data reports several times smaller once converted, so a `pq` sstable would
never be judged "the largest" and C1 would read false for a bucket that is genuinely the bottom
tier. Now uses `ondisk_data_size()`. The gain estimator and `make_tiering_inputs()` already did.

**Not fixed, and not this project's code to change lightly:** `size_tiered_compaction_strategy`
buckets on `data_size()` (`size_tiered_compaction_strategy.cc:133`, `:173`). In a hybrid table a
converted sstable's reported size drops by the compression factor, so it can fall out of the size
bucket it belongs to and stop being compacted with its peers. ICS inherits the same bucketing.

Three ways out, none obviously right:
1. Have `pq` populate an uncompressed length so `data_size()` means the same thing for both. It
   is computable — the shredder knows its buffered volume — but it means synthesising a
   `CompressionInfo`-like value for a file that has no Scylla compression.
2. Change the size-tiered strategies to bucket on `ondisk_data_size()`. Correct in principle and a
   behaviour change for every existing cluster, so not a Parquet decision.
3. Accept it and document that hybrid tables should use a strategy that does not bucket by size.

**Whichever is chosen, it should be decided before hybrid mode is used on anything real**, because
the symptom is silent: compaction simply stops choosing the converted files, and nothing errors.

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

### 11. Decks — done at v2.2 (2026-08-18)
Refreshed to the three-criteria policy, all eight datasets at current defaults, a new
three-column ISD variant, and a slide explaining delta encoding of timestamps. The version now
appears in the **filename** as well as the title slide, so a copy sitting in someone's Downloads
folder can be identified without opening it.

**Regression found while doing it — now resolved, see below.** Original note kept for the record: The `page_values = 2048` default
(§10.4f) was chosen on a +6.3 % size cost measured on the perf schema. On the real corpus it costs
**9–17 %**: ISD-Lite 50.9 % → 59.4 %, NYC TLC 56.8 % → 64.5 %, ClickBench 60.2 % → 65.6 %, and
**Backblaze 95.9 % → 111.7 %, i.e. from a marginal win to a net loss**. The 1.41× point-read win
is real but it was priced on one schema. Either revert `page_values` to a size-optimal value and
give back the latency, or keep it and accept that Parquet loses outright on sparse wide telemetry.
**Resolved 2026-08-18: reverted to 8 192.** Isolating `page_rows` on current code put the real
cost at **+16.7 %** on both a narrow numeric and a wide sparse table — 2.5× what the perf schema
predicted — against a 1.65× point read that leaves the format 20–33× slower than the row format
regardless. Disk is what Parquet is for. §10.4m has the numbers; the corpus figures in the v2.2
deck were measured at 2 048 and are therefore pessimistic by about that much, **Re-measured at 8 192 and
published as v2.3.** The revert reproduced the pre-regression figures exactly — ISD-Lite 50.9 %,
ClickBench 60.0 %, NYC TLC 56.9 %, GitHub 68.1 % — which is the cleanest possible confirmation
that `page_values` was the whole of it.

**New best case: the three-column ISD variant at 33.8 %, i.e. 66.2 % saved.** At 2 048 it read
42.7 %, so the revert is worth nearly nine points on the dataset that shows the format at its
best.

**Backblaze is not reproducible and is the one figure in the deck not from that batch.** Three
runs of the identical command produced lz4 sizes of 59.4, 64.5 and 67.5 MB at identical row and
column counts, and this batch produced a 171.5 % outlier against 95.8 % and 95.9 % from two other
runs at this setting. lz4 is deterministic for fixed input, so the instability is in the loader,
not in the writer. The two agreeing figures are used and the deck says so. **Diagnosed 2026-08-18, and the deck figure is sound.** Three hypotheses were tested and two were
wrong:

1. *The loader is nondeterministic.* **No.** Run twice in one process and fingerprinted, the table
   is byte-identical: same 197 columns, same 300 000 rows, same value hash.
2. *An unsettled compaction is being double-counted.* **No.** Two runs that differed by 13.8 % on
   lz4 each had exactly **one** sstable. (A settle-and-report guard was added to `measure()`
   anyway, and the sstable count is now printed on every size line, because a byte total alone
   hides this.)
3. *`table_dir()` returns the wrong directory.* **Yes.** It was
   `list(DATA_DIR.glob(...))[0]` — arbitrary glob order. A dropped table's directory can linger,
   so the harness could measure a previous run's leftovers. Now sorted by mtime, newest first.

**Verified 2026-08-18, and the conclusion inverted.** Two runs after the `table_dir()` fix put lz4
at 67 531 875 and 67 549 648 — stable to **0.03 %**, so the fix works. But it also showed that the
59 351 983 figure which had "reproduced exactly" across earlier runs was itself the fossil:
reproducible precisely because it was the *same stale directory* every time. Reproducibility was
evidence of the bug, not of correctness.

**Backblaze is now withheld from the deck (v2.4).** The same two guarded runs gave **95.4 %** and
**263.9 %** on the headline ratio, with lz4 identical between them — so the input data is the same
and the variance is downstream of it and still unexplained. Four hypotheses have now been tested
and eliminated: loader nondeterminism (fingerprinted identical in-process), double-counted
sstables (exactly one, asserted), stale directory (fixed, verified), and swallowed insert errors
(retries plus a distinct-key assertion). Whatever remains is in the conversion or measurement of
that one table. Seven datasets are published and the omission is stated on the slide.

**Superseded reasoning below.** The `pq` figures were thought unaffected, which is the confirmation: `measure_native_vs_pq.sh` has
picked the newest directory and asserted a single sstable since the same bug was found there, and
its Backblaze `pq` figure is **20 803 872 in every run** — three independent runs, byte-identical
— while the harness's own logged lines moved. So the deck's 95.8 % stands, and what was unstable
was the reporting path rather than the format.

### 12. Environment traps worth knowing
- A rebuilt binary does not replace a running node. `~/pq-lab/ensure_fresh_node.sh` is a
  precondition on all measurement scripts for this reason; it cost real debugging time three times
  before it existed.
- The lab keyspace needs `NetworkTopologyStrategy`; `SimpleStrategy` is rejected on this node.
- `sstable_compaction_test` segfaults after ~68 cases in this environment. Verified identical on a
  stashed baseline, so pre-existing.
