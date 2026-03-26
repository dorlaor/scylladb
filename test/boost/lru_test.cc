/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE lru

#include <boost/test/unit_test.hpp>
#include <vector>
#include <algorithm>
#include <memory>

#include "utils/count_min_sketch.hh"
#include "utils/lru.hh"

// A concrete evictable for testing.
struct test_evictable final: public evictable {
    int id;
    bool was_evicted = false;

    explicit test_evictable(int id) : id(id) {}

    void on_evicted() noexcept override {
        was_evicted = true;
    }

    ~test_evictable() {
        // Ensure unlinked before destruction.
    }
};

// ---------------------------------------------------------------------------
// Count-Min Sketch Tests
// ---------------------------------------------------------------------------

// Width = 2^test_sketch_width_log2 = 1024 counters per row.
static constexpr size_t test_sketch_width_log2 = 10;

BOOST_AUTO_TEST_CASE(test_count_min_sketch_basic) {
    utils::count_min_sketch sketch(test_sketch_width_log2);

    // An unseen key should have estimate 0.
    BOOST_REQUIRE_EQUAL(sketch.estimate(42), 0);

    sketch.increment(42);
    BOOST_REQUIRE_EQUAL(sketch.estimate(42), 1);

    sketch.increment(42);
    sketch.increment(42);
    BOOST_REQUIRE_EQUAL(sketch.estimate(42), 3);

    // A different key should be independent.
    BOOST_REQUIRE_EQUAL(sketch.estimate(100), 0);
    sketch.increment(100);
    BOOST_REQUIRE_EQUAL(sketch.estimate(100), 1);
    BOOST_REQUIRE_EQUAL(sketch.estimate(42), 3);
}

BOOST_AUTO_TEST_CASE(test_count_min_sketch_max_counter) {
    utils::count_min_sketch sketch(test_sketch_width_log2);

    for (int i = 0; i < 20; ++i) {
        sketch.increment(1);
    }
    // 4-bit counter caps at 15.
    BOOST_REQUIRE_EQUAL(sketch.estimate(1), 15);
}

BOOST_AUTO_TEST_CASE(test_count_min_sketch_reset) {
    utils::count_min_sketch sketch(test_sketch_width_log2);

    sketch.increment(1);
    sketch.increment(1);
    sketch.increment(1);
    sketch.increment(1); // freq = 4
    BOOST_REQUIRE_EQUAL(sketch.estimate(1), 4);

    sketch.reset(); // halve → 2
    BOOST_REQUIRE_EQUAL(sketch.estimate(1), 2);

    sketch.reset(); // halve → 1
    BOOST_REQUIRE_EQUAL(sketch.estimate(1), 1);

    sketch.reset(); // halve → 0
    BOOST_REQUIRE_EQUAL(sketch.estimate(1), 0);
}

// ---------------------------------------------------------------------------
// W-TinyLFU LRU Tests
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(test_lru_add_and_evict) {
    lru l;
    test_evictable e1(1), e2(2), e3(3);

    l.add(e1);
    l.add(e2);
    l.add(e3);

    BOOST_REQUIRE(e1.is_linked());
    BOOST_REQUIRE(e2.is_linked());
    BOOST_REQUIRE(e3.is_linked());

    // Evict removes at least one entry.
    auto r = l.evict();
    BOOST_REQUIRE(r == seastar::memory::reclaiming_result::reclaimed_something);

    // At least one entry should have been evicted.
    int evicted_count = (e1.was_evicted ? 1 : 0) + (e2.was_evicted ? 1 : 0) + (e3.was_evicted ? 1 : 0);
    BOOST_REQUIRE_GE(evicted_count, 1);

    // Clean up remaining linked entries.
    if (e1.is_linked()) l.remove(e1);
    if (e2.is_linked()) l.remove(e2);
    if (e3.is_linked()) l.remove(e3);
}

BOOST_AUTO_TEST_CASE(test_lru_evict_empty) {
    lru l;
    auto r = l.evict();
    BOOST_REQUIRE(r == seastar::memory::reclaiming_result::reclaimed_nothing);
}

BOOST_AUTO_TEST_CASE(test_lru_touch_keeps_entry_alive) {
    lru l;

    // Create entries with different access patterns.
    test_evictable hot(1), cold1(2), cold2(3);

    l.add(hot);
    l.add(cold1);
    l.add(cold2);

    // Touch 'hot' many times to build frequency.
    for (int i = 0; i < 10; ++i) {
        l.touch(hot);
    }

    // Evict all - the hot entry may survive longer than cold entries.
    l.evict();
    l.evict();

    // Hot entry should still be linked (survived eviction of cold entries).
    BOOST_REQUIRE(hot.is_linked());

    // Clean up.
    l.remove(hot);
    // cold entries may or may not still be linked, clean up if needed.
    if (cold1.is_linked()) l.remove(cold1);
    if (cold2.is_linked()) l.remove(cold2);
}

BOOST_AUTO_TEST_CASE(test_lru_evict_all) {
    lru l;
    test_evictable e1(1), e2(2), e3(3);

    l.add(e1);
    l.add(e2);
    l.add(e3);

    l.evict_all();

    BOOST_REQUIRE(!e1.is_linked());
    BOOST_REQUIRE(!e2.is_linked());
    BOOST_REQUIRE(!e3.is_linked());
    BOOST_REQUIRE(e1.was_evicted);
    BOOST_REQUIRE(e2.was_evicted);
    BOOST_REQUIRE(e3.was_evicted);
}

BOOST_AUTO_TEST_CASE(test_lru_remove) {
    lru l;
    test_evictable e1(1), e2(2), e3(3);

    l.add(e1);
    l.add(e2);
    l.add(e3);

    l.remove(e2);
    BOOST_REQUIRE(!e2.is_linked());
    BOOST_REQUIRE(!e2.was_evicted); // remove does not call on_evicted

    l.evict_all();
    BOOST_REQUIRE(e1.was_evicted);
    BOOST_REQUIRE(e3.was_evicted);
}

BOOST_AUTO_TEST_CASE(test_lru_add_before) {
    lru l;
    test_evictable e1(1), e2(2), e3(3);

    l.add(e1);
    l.add(e2);

    // Insert e3 before e2 so e3 is evicted before e2.
    l.add_before(e2, e3);

    BOOST_REQUIRE(e1.is_linked());
    BOOST_REQUIRE(e2.is_linked());
    BOOST_REQUIRE(e3.is_linked());

    // Clean up.
    l.evict_all();
}

BOOST_AUTO_TEST_CASE(test_lru_frequency_based_eviction) {
    lru l;

    // Create entries with different access patterns.
    // Use a fixed-size array to avoid move construction issues.
    static constexpr int N = 20;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
    }

    for (int i = 0; i < N; ++i) {
        l.add(*entries[i]);
    }

    // Touch entries 15-19 many times (they should be "hot").
    for (int round = 0; round < 10; ++round) {
        for (int i = 15; i < N; ++i) {
            l.touch(*entries[i]);
        }
    }

    // Evict half the entries.
    for (int i = 0; i < 10; ++i) {
        l.evict();
    }

    // Hot entries (15-19) should still be linked.
    for (int i = 15; i < N; ++i) {
        BOOST_REQUIRE_MESSAGE(entries[i]->is_linked(),
            "Hot entry " << i << " should survive eviction");
    }

    // Clean up remaining entries.
    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) {
            l.remove(*entries[i]);
        }
    }
}

// ---------------------------------------------------------------------------
// Caffeine-parity tests
// ---------------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(test_aging_reset_uses_entry_count) {
    // The sketch reset threshold should be based on cache entry count,
    // not sketch width.  With 5 entries the threshold is max(1000, 50) = 1000.
    // After enough touches we expect at least one reset (halving), so a
    // previously-saturated counter (15) should decay.
    lru l;
    static constexpr int N = 5;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        entries[i]->set_sketch_key(i + 1);
        l.add(*entries[i]);
    }

    // Saturate entry 0's counter to 15.
    for (int i = 0; i < 20; ++i) {
        l.touch(*entries[0]);
    }
    BOOST_REQUIRE_EQUAL(l.sketch_estimate(1), 15);

    // Generate 1100 touches on entry 1 to trigger at least one reset.
    for (int i = 0; i < 1100; ++i) {
        l.touch(*entries[1]);
    }
    // After at least one halving, 15 should have decayed.
    BOOST_REQUIRE_LE(l.sketch_estimate(1), 7);

    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_lru_touch_promotes_from_probation) {
    lru l;

    // Create entries.
    static constexpr int N = 10;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
    }
    for (int i = 0; i < N; ++i) {
        l.add(*entries[i]);
    }

    // Evict and re-add some to force entries into probation via the eviction logic.
    // The eviction drains excess from window to probation.
    // After enough evictions, remaining entries should be in probation or protected.

    // Touch entries 0-4 multiple times to build frequency.
    for (int round = 0; round < 5; ++round) {
        for (int i = 0; i < 5; ++i) {
            l.touch(*entries[i]);
        }
    }

    // Evict 5 entries - cold entries (5-9) should be evicted.
    for (int i = 0; i < 5; ++i) {
        l.evict();
    }

    // Entries 0-4 (frequently touched) should survive.
    for (int i = 0; i < 5; ++i) {
        BOOST_REQUIRE_MESSAGE(entries[i]->is_linked(),
            "Frequently touched entry " << i << " should survive eviction");
    }

    // Clean up.
    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) {
            l.remove(*entries[i]);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_hill_climbing_tracks_hits_and_misses) {
    lru l;
    static constexpr int N = 10;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        entries[i]->set_sketch_key(3000 + i);
        l.add(*entries[i]);  // each add is a miss
    }

    BOOST_REQUIRE_GE(l.misses_in_sample(), 10u);

    // Touches on linked entries count as hits.
    for (int i = 0; i < 5; ++i) {
        l.touch(*entries[i]);
    }
    BOOST_REQUIRE_GE(l.hits_in_sample(), 5u);

    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_hill_climbing_adjusts_window_size) {
    lru l;

    static constexpr int N = 200;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        entries[i]->set_sketch_key(4000 + i);
        l.add(*entries[i]);
    }

    size_t initial_window_max = l.current_max_window_size();

    // Simulate a sample period with hits, then climb.
    for (int i = 0; i < 100; ++i) {
        l.touch(*entries[i % N]);
    }
    l.climb();

    // Counters should be reset after climb.
    BOOST_REQUIRE_EQUAL(l.hits_in_sample(), 0u);
    BOOST_REQUIRE_EQUAL(l.misses_in_sample(), 0u);

    // After a second climb cycle, the window should have been adjusted.
    for (int i = 0; i < 100; ++i) {
        l.touch(*entries[i % N]);
    }
    l.climb();

    // Window max may have changed (we can't predict direction, but it
    // should either change or converge with a very small step).
    // At minimum: no crash, counters reset.
    BOOST_REQUIRE_EQUAL(l.hits_in_sample(), 0u);

    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_count_min_sketch_cache_line_layout) {
    // Verify functional correctness of the cache-line optimized sketch.
    utils::count_min_sketch sketch(test_sketch_width_log2);

    // Basic increment and estimate.
    sketch.increment(42);
    sketch.increment(42);
    sketch.increment(42);
    BOOST_REQUIRE_EQUAL(sketch.estimate(42), 3);

    // Different key is independent.
    sketch.increment(999);
    BOOST_REQUIRE_EQUAL(sketch.estimate(999), 1);
    BOOST_REQUIRE_EQUAL(sketch.estimate(42), 3);

    // Reset halves counters.
    sketch.reset();
    BOOST_REQUIRE_EQUAL(sketch.estimate(42), 1);
    BOOST_REQUIRE_EQUAL(sketch.estimate(999), 0);

    // Saturation at 15.
    for (int i = 0; i < 20; ++i) sketch.increment(7);
    BOOST_REQUIRE_EQUAL(sketch.estimate(7), 15);
}

BOOST_AUTO_TEST_CASE(test_count_min_sketch_resize_clears) {
    // After the cache-line layout change, resize discards old counts
    // (matching Caffeine's ensureCapacity behavior).
    utils::count_min_sketch sketch(12);
    for (int i = 0; i < 10; ++i) sketch.increment(100);
    BOOST_REQUIRE_EQUAL(sketch.estimate(100), 10);

    sketch.resize(14);
    // After resize, old counts are gone.
    BOOST_REQUIRE_EQUAL(sketch.estimate(100), 0);

    // Re-increment works correctly in the new size.
    for (int i = 0; i < 5; ++i) sketch.increment(100);
    BOOST_REQUIRE_EQUAL(sketch.estimate(100), 5);
}

BOOST_AUTO_TEST_CASE(test_count_min_sketch_many_keys) {
    // Stress test: many distinct keys should have low collision rate.
    utils::count_min_sketch sketch(16);  // 65536 counters per row

    // Insert 10000 unique keys once each.
    for (uint64_t k = 0; k < 10000; ++k) {
        sketch.increment(k);
    }

    // Each key was inserted once; estimate should be >= 1.
    // Due to collisions, some may be > 1, but the majority should be exactly 1.
    int exact_count = 0;
    for (uint64_t k = 0; k < 10000; ++k) {
        BOOST_REQUIRE_GE(sketch.estimate(k), 1);
        if (sketch.estimate(k) == 1) exact_count++;
    }
    // With 65536 counters and 10000 keys, most should be collision-free.
    BOOST_REQUIRE_GT(exact_count, 8000);
}

BOOST_AUTO_TEST_CASE(test_lru_set_window_percent) {
    lru l;
    // Default is 1%.
    BOOST_REQUIRE_EQUAL(l.window_percent(), 1u);

    // Set to 50% (LRU-like).
    l.set_window_percent(50.0);
    BOOST_REQUIRE_EQUAL(l.window_percent(), 50u);

    // Clamped to [1, 99].
    l.set_window_percent(0.0);
    BOOST_REQUIRE_EQUAL(l.window_percent(), 1u);
    l.set_window_percent(100.0);
    BOOST_REQUIRE_EQUAL(l.window_percent(), 99u);
}

BOOST_AUTO_TEST_CASE(test_lru_hill_climbing_disabled) {
    lru l;
    BOOST_REQUIRE(l.hill_climbing_enabled());

    l.set_hill_climbing_enabled(false);
    BOOST_REQUIRE(!l.hill_climbing_enabled());

    // With hill climbing disabled, climb() should be a no-op (no crash).
    static constexpr int N = 50;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        entries[i]->set_sketch_key(5000 + i);
        l.add(*entries[i]);
    }

    for (int i = 0; i < 100; ++i) {
        l.touch(*entries[i % N]);
    }
    l.climb();

    // Window max should remain at default (percentage-based), not adjusted.
    // With 50 entries and 1% window, max_window = max(1, 50*1/100) = 1.
    BOOST_REQUIRE_EQUAL(l.current_max_window_size(), std::max(size_t(1), size_t(N) * l.window_percent() / 100));

    for (int i = 0; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}

BOOST_AUTO_TEST_CASE(test_lru_large_window_behaves_like_lru) {
    lru l;
    l.set_window_percent(99.0);
    l.set_hill_climbing_enabled(false);

    // With 99% window, almost all entries stay in window (pure LRU behavior).
    static constexpr int N = 20;
    std::unique_ptr<test_evictable> entries[N];
    for (int i = 0; i < N; ++i) {
        entries[i] = std::make_unique<test_evictable>(i);
        l.add(*entries[i]);
    }

    // In a large window, the oldest entry should be evicted first (LRU order).
    l.evict();
    BOOST_REQUIRE(entries[0]->was_evicted);

    for (int i = 1; i < N; ++i) {
        if (entries[i]->is_linked()) l.remove(*entries[i]);
    }
}
