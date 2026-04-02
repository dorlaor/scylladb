# W-TinyLFU Cache Replacement Algorithm

## Overview

The row cache uses a W-TinyLFU (Window Tiny Least Frequently Used) eviction
policy, which combines recency and frequency information to achieve better hit
rates than plain LRU. The implementation lives in `utils/lru.hh` and
`utils/count_min_sketch.hh`, orchestrated by `cache_tracker` in
`db/cache_tracker.hh`.

The cache is split into three segments, each governed by its own replacement
strategy. A frequency-based admission gate sits between the window and the
main cache, filtering out entries that lack sufficient access history.

```
New entry --> [Window 1%, LRU]
                   |
            window overflows
                   |
                   v
       +-- TinyLFU Admission Gate --+
       |  w_freq vs p_freq          |
       |  winner --> probation       |
       |  loser  --> evicted         |
       +----------------------------+
                   |
            hit in probation
                   |
                   v
          [Protected 80%, LRU]
             |           |
          hit: refresh   overflow: demote LRU
          (move to MRU)  back to probation
                            |
                            v
                   [Probation 19%]
                   (compete at gate)
```

## Stage 1: Window (1% of cache) -- Pure LRU

Every new entry enters the window segment at the back (MRU position). On a
cache hit within the window, the entry moves to the back of the window list
(standard LRU refresh).

When `_window_size` exceeds `max_window_size()` (1% of total entries), the
front of the window (LRU victim) is picked as a candidate for the admission
competition.

The window's purpose is to act as a frequency-building buffer. New entries get
a chance to accumulate hits in the Count-Min Sketch before they face the
TinyLFU admission gate. Without a window, a burst of one-shot entries would
immediately evict valuable entries from the main cache.

## Stage 2: TinyLFU Admission Gate

When the window overflows, the window victim (LRU of window) competes against
the probation victim (LRU of probation). Both entries have their frequency
estimated via the Count-Min Sketch:

```
w_freq = sketch.estimate(entry_key(window_victim))
p_freq = sketch.estimate(entry_key(probation_victim))
```

The admission decision:

1. If `w_freq > p_freq`: **admit** the window candidate into probation; **evict**
   the probation victim.
2. Else if `w_freq >= 6` (hash-DoS threshold): admit with approximately 1/128
   probability (randomized jitter to prevent hash-collision attacks from gaming
   the sketch).
3. Otherwise: **reject** the window candidate (evict it); the probation victim
   survives.

The winner stays in probation (at the back of the list). The loser is evicted
from the cache entirely.

If probation is empty, the window victim simply moves into probation without
competition.

## Stage 3: Probation (19% of cache) -- SLRU Entry Point

Probation is the "on trial" segment of a Segmented LRU (SLRU). It is not a
standalone eviction algorithm.

**How entries arrive:** Winners of the TinyLFU admission competition land at
the back of the probation list. Entries demoted from the protected segment also
land here.

**On a cache hit in probation:** The entry is promoted to the protected
segment. It is removed from probation and pushed to the back (MRU) of the
protected list.

**Eviction:** The front (LRU) of probation is the candidate that competes
against incoming window victims in the TinyLFU admission gate. Under memory
pressure beyond window overflow, probation's front is evicted directly.

In essence, probation is a holding area. Entries sit here until they either
prove their worth by getting accessed (promoting to protected) or get evicted
by losing a frequency competition.

## Stage 4: Protected (80% of cache) -- SLRU Main Segment

**Promotion into protected:** Any cache hit on a probation entry triggers
immediate promotion to the back (MRU) of the protected list.

**On a cache hit in protected:** The entry moves to the back of the protected
list (standard LRU refresh).

**Demotion:** When `_protected_size` exceeds `max_protected_size()` (80% of
total), the front (LRU) of the protected list is demoted back to probation.
This `rebalance_protected()` runs during every eviction cycle before the
admission competition.

**Direct eviction from protected:** Only happens as a last resort, when both
window and probation are empty.

## Frequency Estimation: Count-Min Sketch

### Structure

The Count-Min Sketch (`utils/count_min_sketch.hh`) is a probabilistic data
structure that estimates how many times each key has been accessed. It uses:

- **Depth:** 4 rows of counters.
- **Counter width:** 4 bits each, saturating at 15.
- **Layout:** Cache-line optimized. All 4 row counters for a given key are
  co-located in the same 64-byte block (one L1 cache line), turning 4 cache
  misses per operation into 1.

### How increment works

Each access calls `_sketch.increment(entry_key(e))`. For a given key:

1. **`spread(key)`** -- splitmix64-style hash. Selects which 64-byte block all
   4 rows will use.
2. **`rehash(spread)`** -- second hash. Its 4 bytes are split, one byte per
   row, to select the counter position within each row's 2-word (32-counter)
   segment of the block.
3. For each of the 4 rows, the corresponding 4-bit counter is incremented
   (saturating at 15).

One `increment()` call bumps 4 independent counters (one per row), all within
the same 64-byte cache line.

### How estimate works

Same hashing as `increment()`, but instead of incrementing, it reads all 4
counters and returns the **minimum**:

```cpp
uint8_t min_val = 15;
for (row 0..3) {
    min_val = min(min_val, get_counter(...));
}
return min_val;
```

The minimum is the key CMS property: hash collisions can only inflate a
counter (false positives), never deflate it. Taking the minimum across 4 rows
minimizes this over-counting. The returned value is in the range [0, 15].

### Block layout

```
Block (64 bytes = 8 x uint64_t words):
  words 0-1: row 0 (32 four-bit counters)
  words 2-3: row 1 (32 four-bit counters)
  words 4-5: row 2 (32 four-bit counters)
  words 6-7: row 3 (32 four-bit counters)
```

### Sketch key

Each `evictable` carries a `_sketch_key` field. For row cache entries, this is
set to the partition token's raw hash before insertion. All rows within the
same partition share the same sketch key. This ensures that:

- Frequency is tracked at partition granularity, not per-row.
- Evicting and re-inserting the same logical entry does not lose its
  accumulated frequency (the key is content-derived, not pointer-based).

The sketch key is propagated through MVCC row copy and creation paths
(`partition_snapshot_row_cursor`, `cache_mutation_reader`) so that rows
created during version merges or cache population inherit the correct key.

### Aging

Every `10 * total_entries` accesses, `_sketch.reset()` is called, which
halves all counters in a single pass:

```cpp
word = (word >> 1) & 0x7777777777777777ULL;
```

The mask `0x77..77` clears the high bit of each 4-bit nibble after the right
shift, preventing bits from bleeding across counter boundaries.

This aging ensures that entries which were hot in the past but are no longer
accessed will have their frequency estimates decay toward zero, giving fresh
entries a fair chance at admission. An entry accessed 20 times might have
counters like `[15, 12, 14, 13]` (estimate: 12). After one aging cycle:
`[7, 6, 7, 6]` (estimate: 6). After another: `[3, 3, 3, 3]` (estimate: 3).

## Key Insight

The window uses pure LRU for recency. The CMS-based admission gate filters for
frequency. The SLRU (probation + protected) combines both: entries must be both
frequent enough to pass admission AND recently accessed enough to avoid being
the LRU victim in their segment. This layered approach resists both scan
pollution (one-shot sequential reads) and frequency-based attacks better than
either LRU or LFU alone.

## Source Files

| Component | File | Purpose |
|-----------|------|---------|
| Count-Min Sketch | `utils/count_min_sketch.hh` | 4-bit CMS with cache-line optimized layout |
| LRU + W-TinyLFU | `utils/lru.hh` | Three-segment SLRU with TinyLFU admission |
| Cache tracker | `db/cache_tracker.hh` | Orchestrates LRU instance, sketch sizing, configuration |
| Sketch key propagation | `db/cache_mutation_reader.hh`, `db/partition_snapshot_row_cursor.hh` | Tags rows with partition token for stable frequency tracking |
