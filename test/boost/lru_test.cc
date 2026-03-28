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
