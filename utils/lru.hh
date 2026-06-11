/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/assert.hh"
#include <boost/intrusive/list.hpp>
#include <seastar/core/memory.hh>
#include <seastar/core/preempt.hh>

// Identifies which SLRU segment an evictable belongs to.
enum class lru_segment : uint8_t {
    none = 0,
    probation = 2,
    protected_ = 3,
};

class evictable {
    friend class lru;
    // For bookkeeping, we want the unlinking of evictables to be explicit.
    // E.g. if the cache's internal data structure consists of multiple lists, we would
    // like to know which list is an element being removed from.
    // Therefore, we are using auto_unlink only to be able to call unlink() in the move constructor
    // and we do NOT rely on automatic unlinking in _lru_link's destructor.
    // It's the programmer's responsibility. to call lru::remove on the evictable before its destruction.
    // Failure to do so is a bug, and it will trigger an assertion in the destructor.
protected:
    using link_base = boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
    struct lru_link_type : link_base {
        lru_link_type() noexcept = default;
        lru_link_type(lru_link_type&& o) noexcept {
            swap_nodes(o);
        }
    };
    static_assert(std::is_nothrow_constructible_v<lru_link_type, lru_link_type&&>);
private:
    lru_link_type _lru_link;

    // Packed layout:
    //   bits [1:0]  — lru_segment tag (none=0, probation=2, protected=3)
    //   bit  [2]    — has_sketch_key flag (1 = set_sketch_key() was called)
    //   bits [63:3] — sketch key value (61 bits, used for MVCC key propagation)
    uint64_t _packed = 0;

    static constexpr uint64_t segment_mask  = 0x3;
    static constexpr uint64_t has_key_mask  = uint64_t(1) << 2;
    static constexpr uint64_t key_shift     = 3;

    lru_segment get_segment() const noexcept {
        return static_cast<lru_segment>(_packed & segment_mask);
    }
    void set_segment(lru_segment seg) noexcept {
        _packed = (_packed & ~segment_mask) | static_cast<uint64_t>(seg);
    }
protected:
    ~evictable() {
        SCYLLA_ASSERT(!_lru_link.is_linked());
    }
    evictable() = default;
    evictable(evictable&&) noexcept = default;
public:
    virtual void on_evicted() noexcept = 0;
    virtual void on_evicted_shallow() noexcept { on_evicted(); }

    bool is_linked() const {
        return _lru_link.is_linked();
    }

    void swap(evictable& o) noexcept {
        _lru_link.swap_nodes(o._lru_link);
        std::swap(_packed, o._packed);
    }

    virtual bool is_index() const noexcept {
        return false;
    }

    void set_sketch_key(uint64_t key) noexcept {
        _packed = (key << key_shift) | has_key_mask | (_packed & segment_mask);
    }
    uint64_t sketch_key() const noexcept {
        return _packed >> key_shift;
    }
    bool has_sketch_key() const noexcept {
        return _packed & has_key_mask;
    }
};

// Sstable index cache shares memory with the data cache.
// To prevent index entries from depriving the data cache of memory,
// there is a limit (index_cache_fraction) on the total fraction of cache usable
// by index entries.
//
// To maintain this limit, index entries might have to be evicted outside of the regular LRU order.
// Therefore they are linked both in the common LRU list and in a separate LRU list for index entries.
class index_evictable : public evictable {
    friend class lru;
    evictable::lru_link_type _index_lru_link;
    bool is_index() const noexcept override {
        return true;
    }
};

// Implements Segmented LRU (SLRU) cache replacement for row cache and
// sstable index cache.
//
// SLRU splits the cache into two segments:
//   - Probation (20%): new entries land here. Eviction picks from the front.
//   - Protected (80%): entries promoted on re-access. Safe from scan eviction.
//
// The key property: a scan that touches each entry once fills probation but
// cannot displace entries in the protected segment. Hot entries that are
// re-accessed get promoted to protected and stay there.
//
// This provides scan resistance without the complexity of a frequency
// sketch or admission gate.
class lru {
private:
    using lru_type = boost::intrusive::list<evictable,
        boost::intrusive::member_hook<evictable, evictable::lru_link_type, &evictable::_lru_link>,
        boost::intrusive::constant_time_size<false>>;
    lru_type _probation;
    lru_type _protected;

    using index_lru_type = boost::intrusive::list<index_evictable,
        boost::intrusive::member_hook<index_evictable, index_evictable::lru_link_type, &index_evictable::_index_lru_link>,
        boost::intrusive::constant_time_size<false>>;
    index_lru_type _index_list;

    using reclaiming_result = seastar::memory::reclaiming_result;

    static constexpr size_t default_protected_percent = 80;
    size_t _probation_size = 0;
    size_t _protected_size = 0;

public:
    struct stats {
        uint64_t direct_evictions = 0;
        uint64_t protected_promotions = 0;
        uint64_t protected_demotions = 0;
        uint64_t eviction_calls = 0;
        uint64_t eviction_calls_empty = 0;
    };

private:
    stats _stats{};

    size_t total_size() const noexcept {
        return _probation_size + _protected_size;
    }

    size_t max_protected_size() const noexcept {
        return total_size() * default_protected_percent / 100;
    }

    // Move excess protected entries to probation front.
    void rebalance_protected() noexcept {
        size_t max_prot = max_protected_size();
        while (_protected_size > max_prot && !_protected.empty()) {
            ++_stats.protected_demotions;
            evictable& victim = _protected.front();
            _protected.erase(_protected.iterator_to(victim));
            --_protected_size;
            victim.set_segment(lru_segment::probation);
            _probation.push_front(victim);  // demoted to probation FRONT (oldest)
            ++_probation_size;
            if (seastar::need_preempt()) {
                break;
            }
        }
    }

    // Evicts a single element.
    template <bool Shallow = false>
    reclaiming_result do_evict(bool should_evict_index) noexcept {
        if (should_evict_index && !_index_list.empty()) {
            evictable& e = _index_list.front();
            remove(e);
            if constexpr (!Shallow) {
                e.on_evicted();
            } else {
                e.on_evicted_shallow();
            }
            return reclaiming_result::reclaimed_something;
        }

        if (_probation.empty() && _protected.empty()) {
            return reclaiming_result::reclaimed_nothing;
        }

        rebalance_protected();

        ++_stats.direct_evictions;
        evictable* victim = nullptr;
        if (!_probation.empty()) {
            victim = &_probation.front();
        } else if (!_protected.empty()) {
            victim = &_protected.front();
        } else {
            return reclaiming_result::reclaimed_nothing;
        }
        remove(*victim);
        if constexpr (!Shallow) {
            victim->on_evicted();
        } else {
            victim->on_evicted_shallow();
        }
        return reclaiming_result::reclaimed_something;
    }

public:
    ~lru() {
        auto drain = [this](lru_type& list) {
            while (!list.empty()) {
                evictable& e = list.front();
                remove(e);
                e.on_evicted();
            }
        };
        drain(_probation);
        drain(_protected);
    }

    void remove(evictable& e) noexcept {
        auto seg = e.get_segment();
        if (seg == lru_segment::probation) {
            _probation.erase(_probation.iterator_to(e));
            --_probation_size;
        } else if (seg == lru_segment::protected_) {
            _protected.erase(_protected.iterator_to(e));
            --_protected_size;
        }
        e.set_segment(lru_segment::none);
        if (e.is_index()) {
            _index_list.erase(_index_list.iterator_to(static_cast<index_evictable&>(e)));
        }
    }

    void add(evictable& e) noexcept {
        e.set_segment(lru_segment::probation);
        _probation.push_back(e);
        ++_probation_size;
        if (e.is_index()) {
            _index_list.push_back(static_cast<index_evictable&>(e));
        }
    }

    // Like add(e) but makes sure that e is evicted right before "more_recent" in the absence of later touches.
    void add_before(evictable& more_recent, evictable& e) noexcept {
        auto seg = more_recent.get_segment();
        if (seg == lru_segment::probation) {
            _probation.insert(_probation.iterator_to(more_recent), e);
            ++_probation_size;
        } else if (seg == lru_segment::protected_) {
            _protected.insert(_protected.iterator_to(more_recent), e);
            ++_protected_size;
        }
        e.set_segment(seg);
        if (e.is_index()) {
            auto& ie = static_cast<index_evictable&>(e);
            auto& mr = static_cast<index_evictable&>(more_recent);
            if (mr._index_lru_link.is_linked()) {
                _index_list.insert(_index_list.iterator_to(mr), ie);
            } else {
                _index_list.push_back(ie);
            }
        }
    }

    // Handles access to an entry:
    //  - In probation: promotes to protected.
    //  - In protected: moves to back of protected.
    //  - Not linked: adds to probation.
    void touch(evictable& e) noexcept {
        switch (e.get_segment()) {
            case lru_segment::none:
                e.set_segment(lru_segment::probation);
                _probation.push_back(e);
                ++_probation_size;
                break;
            case lru_segment::probation:
                ++_stats.protected_promotions;
                _probation.erase(_probation.iterator_to(e));
                --_probation_size;
                e.set_segment(lru_segment::protected_);
                _protected.push_back(e);
                ++_protected_size;
                break;
            case lru_segment::protected_:
                _protected.erase(_protected.iterator_to(e));
                _protected.push_back(e);
                break;
        }
        if (e.is_index()) {
            auto& ie = static_cast<index_evictable&>(e);
            _index_list.erase(_index_list.iterator_to(ie));
            _index_list.push_back(ie);
        }
    }

    reclaiming_result evict(bool should_evict_index = false) noexcept {
        ++_stats.eviction_calls;
        auto result = do_evict<false>(should_evict_index);
        if (result == reclaiming_result::reclaimed_nothing) {
            ++_stats.eviction_calls_empty;
        }
        return result;
    }

    reclaiming_result evict_shallow() noexcept {
        ++_stats.eviction_calls;
        auto result = do_evict<true>(false);
        if (result == reclaiming_result::reclaimed_nothing) {
            ++_stats.eviction_calls_empty;
        }
        return result;
    }

    void evict_all() {
        while (evict() == reclaiming_result::reclaimed_something) {}
    }

    size_t current_max_protected_size() const noexcept { return max_protected_size(); }

    stats& get_stats() noexcept { return _stats; }
    const stats& get_stats() const noexcept { return _stats; }

    size_t probation_size() const noexcept { return _probation_size; }
    size_t protected_size() const noexcept { return _protected_size; }
    // Compatibility: window_size is always 0 in SLRU.
    size_t window_size() const noexcept { return 0; }
};
