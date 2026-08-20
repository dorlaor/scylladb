# Row-group side index for `pq` sstables — design

Status: **design only, nothing implemented.** Written while another change was in flight in
`parquet-storage-format.md`, hence a separate file; fold it into §10.23 when that settles.

## The problem, restated from measurement

A point read needs one row group's metadata — about 1 420 bytes — and reads the entire footer:
0.14 MB at 100 row groups per file, 2.84 MB at 2 000 (§10.22). Cold, that cost the measured
`2.1 ms + 3.1 ms per MB` (§10.21), and it is the reason the pre-cache slope was 4.42 µs per row
group per file.

It cannot be avoided by reading less of the footer. `FileMetaData` is one Thrift compact struct
whose `row_groups` list is delta-encoded and variable-length, so entry *N* cannot be located
without walking 0…N−1, and Parquet publishes no offset index for the footer itself. The reader
already takes the only shortcut the format allows — `metadata_mode::lazy` records each group's byte
extent and decodes exactly one — so **decode is already O(1) and the read and walk are O(all
groups)**.

The footer cache (§10.24) removes this for the *second* read of a file. It does nothing for the
first, and after a restart every read is a first read. That is the gap this closes.

## What the index has to contain

Enough that a point read touches the footer **once, at a known offset, for one row group**:

| field | width | why |
|---|---:|---|
| `schema_extent` | 8 + 4 | byte range of the footer prefix holding `version` + `schema`; needed to build the mapped schema, and its length is not knowable a priori |
| per row group: `columns_offset` | 8 | byte offset, within the footer, of that group's column list — exactly what `parse_row_group_light()` already computes |
| per row group: `columns_length` | 4 | its length |
| per row group: `num_rows` | 8 | so ordinal → row group needs no footer read at all |

20 bytes per row group plus a small header. At 2 000 groups that is **40 kB**, against a 2.84 MB
footer — a 71× reduction in bytes touched, and fixed-width records make lookup O(1) rather than a
walk.

The cold path becomes: read the side index, binary-search `num_rows` for the ordinal, read one
~1 420-byte slab plus the schema prefix, decode. **O(1) in row groups.**

## Where it lives, and why not a new component

The obvious shape is a new `component_type`. **Rejected**: `component_type` is a fixed enum whose
`Unknown` sentinel sizes arrays, adding a member changes TOC contents, and an older node meeting an
unfamiliar component in a TOC is a downgrade question this project has already had to answer once
for sstable *versions* (§10.9, §10.20). Introducing a second such question to save a few kilobytes
is a poor trade.

**Use the `Scylla` component's existing attribute map instead.** It is already an extensible
key → blob store for Scylla-private per-sstable data, and encryption at rest already uses it exactly
this way — `ent/encryption` stores its serialised options and key id there (`encryption_attribute_ds`,
`key_id_attribute_ds`). So the precedent, the plumbing and the compatibility story all exist:

- no new component, no TOC change, no downgrade hazard;
- an older node ignores an attribute it does not know;
- an sstable written before this change simply lacks the attribute, and the reader falls back to
  the current whole-footer path. **The fallback is the compatibility story** — there is no migration.

## The one thing to measure before building

`Scylla.db` is read when an sstable is opened, not lazily per read. So the index becomes resident
for **every** open sstable, not just ones being read: 40 kB × sstables. A thousand 2 000-group
sstables is ~40 MB — cheap next to the footer cache's 1 522 B/row group (~3 MB for one such
sstable), but it is memory spent whether or not anyone reads the file, which is the opposite of the
footer cache's profile.

That suggests it should be **reclaimable on the same terms as the footer cache** — registered with
`sstables_manager`'s `_total_reclaimable_memory` against `components_memory_reclaim_threshold`, the
machinery bloom filters and the footer cache already share — with re-read from `Scylla.db` on miss.
Whether that is worth the complexity depends on the real number, so **measure resident size across a
realistic sstable count first**. If 40 MB is noise, pin it and keep the code simple.

## Interaction with the footer cache

They compose rather than overlap, and the distinction is worth stating because §10.4l originally
conflated them:

- **side index** — makes the *first* read cheap, by not reading the whole footer;
- **footer cache** — makes *subsequent* reads free, by not re-parsing what was read.

With both, a cold read costs one small index lookup plus one 1 420-byte slab, and warm reads cost
nothing. The cache's entry shape may need to express "schema plus one materialised group" rather
than "whole footer parsed", since with the index the reader never holds the whole footer — worth
checking against `cached_footer` before implementing.

## Open questions

- Does anything else depend on the whole footer being resident? Compaction and `scylla-sstable`
  tooling read footers; both should keep working through the fallback path, but that needs checking
  rather than assuming.
- The index must be written on **both** write paths — `cut_row_group()` and the single-row-group
  `write_rows()` — which is the divergence that produced two separate bugs already (§8.2b, §10.15).
  Whatever writes it should be asserted on both in one test.
- Encrypted files: the extents are offsets into the *plaintext* footer, so for `PARE` files the
  index must describe post-decryption offsets, and the whole encrypted footer must still be fetched
  and decrypted before the slab can be cut out of it. **That removes most of the benefit for
  encrypted tables**, and is the one case where this design does not obviously pay. Needs thought
  before implementation, not after.
