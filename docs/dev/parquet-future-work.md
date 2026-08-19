# Parquet storage format — deferred work

Companion to `parquet-storage-format.md`, which is the design and the evidence. This file is
only the backlog: what is deliberately not done, why, and what a person picking it up needs to
know that is not obvious from the code.

Every item names the trap that would catch a reasonable first attempt, because in this project
that has been the expensive part rather than the implementation.

Last reviewed 2026-08-18. Corpus figures live in `parquet-storage-format.md` §10.1f-prod; the
decks are generated from `~/pq-lab/deck_data.py` and are at v2.5.

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

## Verified, not open — recorded so nobody re-investigates

The operational surface works on `pq` sstables and was checked against a live node, not inferred:
`dump-data` (full row count), `dump-statistics`, `dump-index`, `dump-summary`, `validate`
(0 errors), `validate-checksums` (digest and CRC verified), `nodetool upgradesstables` (converts
`me` → `pq`), and `nodetool scrub` in validate mode (passes, rows still readable). The design
doc's claim that `upgradesstables` does not force convergence was stale — it does, because the
sstable creator is shared by the rewrite paths.

**Snapshot and restore round-trip works, and finding out exposed a real bug.** Snapshot captures
`pq` sstables; truncate-and-refresh returns every row. But `refresh` in load-and-stream mode
re-streams the partitions rather than adopting the files, and the streaming creators in
`replica/table.cc` did not consult `storage_format` — so repair, bootstrap, decommission and
refresh all wrote **native** sstables into a table declared `'parquet'`. Fixed and re-verified by
the same round trip. `'hybrid'` still streams native by design, since streamed data is not
bottom-tier.

---

## Format gaps

### 5. `DELTA_BYTE_ARRAY` is unimplemented
The encoding that would help exactly where Parquet does worst: HackerNews at 82.5 % and Wikipedia
pageviews at 90.0 % are both dominated by near-unique text (§10.1f-prod). Prefix-sharing between
adjacent sorted strings is the one mechanism that attacks that, and it is unavailable.

### 6. ~~Counter shard leaves are not typed~~ — closed 2026-08-19
`value` and `clock` are now two `INT64` leaves of the counter's group, so an external reader sees a
count and a logical clock rather than a packed blob. The nested-group work this was deferred for was
never needed: the `key_value` group already carries five children rather than a strict MAP's two, so
a sixth typed sibling costs nothing structurally. The metadata declaration added earlier stays as
documentation of the shard-id packing, which is still 16 bytes.

### 7. ~~Statistics metadata is thinly fed~~ — closed 2026-08-19
Investigated and largely a false alarm. `update_live_row_marker_timestamp` *is* fed (an earlier
grep here only matched the `_collector.` prefix, and the writer calls it through `_c_stats`), so
`pq` never hits the `on_internal_error_noexcept` path in `get_max_purgeable_timestamp`. The one
real gap was `add_compression_ratio`, now fixed: the writer records it from the Parquet footer and
`sstable::get_compression_ratio()` falls back to the statistics value when there is no
`CompressionInfo` component. Measured 0.0732 through the REST API, corroborated to within 0.2
points by the independent raw measurement in §10.1f-raw.

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

### 11. Decks — done at v2.6 (2026-08-19)
Three-criteria policy, eight datasets at the shipped defaults, the three-column ISD variant, a
slide deriving delta encoding of timestamps from the encoder, and a slide decomposing the win
against the **uncompressed** baseline — which is the one a reader is most likely to push back on,
since it shows Scylla's own compressor doing the larger share on six of seven datasets. Version
appears in the filename as well as the title slide.

**Backblaze: resolved, and the lesson is about measurement, not the format.** Its ratio appeared
to swing between 95.4 % and 263.9 %, and it was withheld from v2.4 on that basis. The cause was
**directory selection**, through three successive wrong answers:

1. `list(glob(...))[0]` — arbitrary order.
2. Newest by **mtime** — worse, and the one that produced the wild readings. *Removing files from
   a directory updates that directory's mtime*, so a table being dropped gets a fresh timestamp
   and beats the newly-created one. The pipeline measured the dying directory.
3. **Resolved from the table's id** in `system_schema.tables` — exact, since Scylla names the
   directory `<table>-<id without dashes>`. Now in `live_table_dir.py`, shared by every
   measurement script.

Under the id lookup, two consecutive runs give lz4 **byte-identical** at 59 351 983 and pq
**byte-identical** at 20 803 872, ratio 95.8 % / 95.9 %. Backblaze is restored at 95.8 %.

**Leaf-set hypothesis disproven, 2026-08-19.** The suspicion was that a row-group cut flips the
writer from the *derived* leaf set to the *conservative* one — 199 leaves against 394 on this
table — and that this caused the 4x swing. Swept `row_group_rows` on one loaded table under the
id-based lookup:

| `row_group_rows` | `pq` bytes |
|---:|---:|
| no cut (10⁸ rows, 1 GiB buffer) | 18 037 425 |
| 20 000 | 19 908 060 |
| **5 000 — the default** | **20 803 872** |
| 2 000 | 26 862 193 |

Two things follow. The leaf set is worth **+10.4 %**, not 4x — real, and the cost of the
conservative set is modest even on the corpus's widest table. And the default reproduces
**20 803 872 exactly**, the same value every controlled run gives, with size growing smoothly as
row groups shrink. **No row-group setting produces 84.5 MB**, so the mechanism is not this.

**Where that leaves it.** Backblaze at the shipped defaults is 20 803 872 bytes against a
~21.7 MB native, i.e. 95.8 %, established by repeated isolated runs and now by a controlled sweep
that lands on the same number. The 84.5 MB and 32.5 MB readings have only ever occurred inside
**full-corpus batch runs**, where Backblaze runs after five other datasets, and never in an
isolated run. Since `pq` output does not depend on the compression dictionary at all, a
batch-polluted dictionary cannot explain the `pq` figure either.

So: the controlled value is trustworthy and is what the corpus should quote; the batch anomaly is
real, unexplained, and confined to a setting no published figure depends on. Not worth more
iterations until it reproduces in isolation — **if it does, capture the sstable and diff its footer
against a good one**, which is the one diagnostic not yet tried.

**Two conclusions I published and then had to withdraw**, worth recording because both were
confidently argued from a broken selector: that the stable 59 351 983 was a fossil (it was the
correct value), and that the format produced 4× swings on this table (it never did). A figure
that repeats is not thereby trustworthy, and neither is a figure that varies — both need the
selector checked first.

**Unverified as a result:** the row-group-cut leaf-set experiment (+22.7 % for a cut, 69.1 → 84.7
MB) ran through its own mtime-based lookup and should be redone before it is cited.

### 12. Run `interop_shapes.py` after any change to the schema mapping
It builds a table per shape the format emits — flat scalars, all three non-frozen collection kinds,
frozen collections, statics, TTL, counters, and a collection at each folding level — converts each,
and reads every resulting `pq` sstable with **both pyarrow and DuckDB**. 11/11 today.

Two readers on purpose: pyarrow *is* parquet-cpp, and the MAP arity check that rejected every
collection file was parquet-cpp's, so a gate built on pyarrow alone tests one implementation's
opinion of the spec. DuckDB has its own reader. (`pip install --user duckdb` was needed here; it is
not a Scylla dependency, only a test one.)

**Gap closed 2026-08-19, and it turned into a finding.** L2 fell back for three successive fixtures
because its precondition forbids a row marker and **every CQL `INSERT` writes one**. Only
`UPDATE`-written data with a single timestamp, no TTL and no deletions can reach L2. With that
fixture L2 applies and shows 4 leaves against L1's 11, verified by both readers. 16/16 shapes now.

**Follow-on worth checking:** §10.1f's L2 savings figures were produced by the harness, which
populates tables with prepared `INSERT`s. If so they are L1 measurements mislabelled as L2, and the
"L2 folds `__ts` away" numbers understate what L2 actually does.

This exists because a passing interop suite of *flat* fixtures let a broken MAP annotation make
every collection and counter file unreadable by parquet-cpp for as long as collections have
existed (§10.3i). The seven original fixtures are still worth keeping, but they only ever proved
that flat schemas interoperate. **Any change to the tree builder or the leaf layout should re-run
this**, because the failure mode is a file no other tool can open, and nothing inside Scylla
notices.

### 13. Measure through `live_table_dir.py`, never by glob or mtime
Every measurement script resolves the table's data directory from its id in
`system_schema.tables`. Do not reintroduce a glob or an mtime heuristic: deleting files from a
directory updates that directory's mtime, so a dropped table outranks the live one and the
pipeline silently measures a corpse. This cost two published conclusions before it was found
(item 11).

### 14. Environment traps worth knowing
- A rebuilt binary does not replace a running node. `~/pq-lab/ensure_fresh_node.sh` is a
  precondition on all measurement scripts for this reason; it cost real debugging time three times
  before it existed.
- The lab keyspace needs `NetworkTopologyStrategy`; `SimpleStrategy` is rejected on this node.
- `sstable_compaction_test` segfaults after ~68 cases in this environment. Verified identical on a
  stashed baseline, so pre-existing.
