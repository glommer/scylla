/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <unordered_set>
#include <bits/unique_ptr.h>
#include <seastar/core/scollectd.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include "allocation_strategy.hh"

namespace logalloc {

struct occupancy_stats;
class region;
class region_impl;
class allocating_section;

constexpr int segment_size_shift = 18; // 256K; see #151, #152
constexpr size_t segment_size = 1 << segment_size_shift;

//
// Frees some amount of objects from the region to which it's attached.
//
// This should eventually stop given no new objects are added:
//
//     while (eviction_fn() == memory::reclaiming_result::reclaimed_something) ;
//
using eviction_fn = std::function<memory::reclaiming_result()>;

// Groups regions for the purpose of statistics.  Can be nested.
class region_group {
    region_group* _parent = nullptr;
    region_group* _root = nullptr;
    size_t _total_memory = 0;
    size_t _throttle_threshold = std::numeric_limits<size_t>::max();

    std::vector<region_group*> _subgroups;
    std::vector<region_impl*> _regions;

    // See the comments in release_requests() for an explanation of this, specially on why we
    // shouldn't just try keep a list of region_groups in which requests can be released after
    // update().
    struct subgroup_fairness_comparator;
    friend class subgroup_fairness_comparator;
    struct subgroup_fairness_comparator {
        bool operator()(region_group* rg1, region_group* rg2) {
            if (rg1->_last_released < rg2->_last_released) {
                return true;
            } else if (rg1->_last_released == rg2->_last_released) {
                return rg1 < rg2;
            }
            return false;
        }
    };

    circular_buffer<promise<>> _blocked_requests;
    uint64_t _last_released = 0;

    // Following fields are expected in the root only
    std::set<region_group*, subgroup_fairness_comparator> _blocked_subgroups;
    uint64_t _root_release_stamps = 0;
public:
    // When creating a region_group, one can specify an optional throttle_threshold parameter. This
    // parameter won't affect normal allocations, but an API is provided, through the region_group's
    // method run_when_memory_available(), to make sure that a given function is only executed when
    // the total memory for the region group (and all of its parents) is lower or equal to the
    // region_group's throttle_treshold (and respectively for its parents).
    region_group(size_t throttle_threshold = std::numeric_limits<size_t>::max()) : region_group(nullptr, throttle_threshold) {}
    region_group(region_group* parent, size_t throttle_threshold = std::numeric_limits<size_t>::max()) : _parent(parent), _throttle_threshold(throttle_threshold) {
        if (_parent) {
            _parent->add(this);
            _root = _parent->_root;
        } else {
            _root = this;
        }
    }
    region_group(region_group&& o) noexcept;
    region_group(const region_group&) = delete;
    ~region_group() {
        if (_parent) {
            _parent->del(this);
        }
        _root->_blocked_subgroups.erase(this);
    }
    region_group& operator=(const region_group&) = delete;
    region_group& operator=(region_group&&) = delete;
    size_t memory_used() const {
        return _total_memory;
    }
    void update(ssize_t delta) {
        // During normal update, we'll call release_requests for the root of the hierarchy
        // only. So find it.
        do_for_each_parent(this, [delta] (auto rg) mutable {
            rg->_total_memory += delta;
            return true;
        });
        _root->release_requests();
    }

    //
    // Make sure that the function specified by the parameter func only runs when this region_group,
    // as well as each of its ancestors have a memory_used() amount of memory that is lesser or
    // equal the throttle_threshold, as specified in the region_group's constructor.
    //
    // region_groups that did not specify a throttle_threshold will always allow for execution.
    //
    // In case current memory_used() is over the threshold, a non-ready future is returned and it
    // will be made ready at some point in the future, at which memory usage in the offending
    // region_group (either this or an ancestor) falls below the threshold.
    //
    // Requests that are not allowed for execution are queued and released in FIFO order within the same
    // region_group, but no guarantees are made regarding release ordering across different
    // region_groups, although there will be a best effort attempt not to starve any waiter.
    template <typename Func>
    future<> run_when_memory_available(Func&& func) {
        // We disallow future-returning functions here, because otherwise memory may be available
        // when we start executing it, but no longer available in the middle of the execution.
        static_assert(!is_future<std::result_of_t<Func()>>::value, "future-returning functions are not permitted.");
        bool exec_ok = do_for_each_parent(this, [] (auto rg) {
            return rg->_blocked_requests.empty() && rg->execution_permitted();
        });

        if (exec_ok) {
            return futurize<void>::apply(func);
        }

        _blocked_requests.emplace_back();
        _root->_blocked_subgroups.insert(this);
        return _blocked_requests.back().get_future().then([this, func] {
            return futurize<void>::apply(func);
        }).finally([this] {
            // re-evaluate root in case it changed, but likely it didn't.
            _root->release_requests();
        });
    }
private:
    // Executes the function func for each region_group upwards in the hierarchy, starting
    // with the parameter node. Stops when func returns false or when the root of the hierarchy
    // is reached.
    template <typename Func>
    bool do_for_each_parent(region_group *node, Func&& func) {
        auto rg = node;
        while (rg) {
            if (!func(rg)) {
                return false;
            }
            rg = rg->_parent;
        }
        return true;
    }
    inline bool execution_permitted() const {
        return _total_memory <= _throttle_threshold;
    }

    void release_requests();
    void add(region_group* child);
    void del(region_group* child);
    void add(region_impl* child);
    void del(region_impl* child);
    friend class region_impl;
};

// Controller for all LSA regions. There's one per shard.
class tracker {
public:
    class impl;
private:
    std::unique_ptr<impl> _impl;
    memory::reclaimer _reclaimer;
    friend class region;
    friend class region_impl;
public:
    tracker();
    ~tracker();

    //
    // Tries to reclaim given amount of bytes in total using all compactible
    // and evictable regions. Returns the number of bytes actually reclaimed.
    // That value may be smaller than requested when evictable pools are empty
    // and compactible pools can't compact any more.
    //
    // Invalidates references to objects in all compactible and evictable regions.
    //
    size_t reclaim(size_t bytes);

    // Compacts as much as possible. Very expensive, mainly for testing.
    // Invalidates references to objects in all compactible and evictable regions.
    void full_compaction();

    void reclaim_all_free_segments();

    // Returns aggregate statistics for all pools.
    occupancy_stats region_occupancy();

    // Returns statistics for all segments allocated by LSA on this shard.
    occupancy_stats occupancy();

    impl& get_impl() { return *_impl; }
};

tracker& shard_tracker();

// Monoid representing pool occupancy statistics.
// Naturally ordered so that sparser pools come fist.
// All sizes in bytes.
class occupancy_stats {
    size_t _free_space;
    size_t _total_space;
public:
    occupancy_stats() : _free_space(0), _total_space(0) {}

    occupancy_stats(size_t free_space, size_t total_space)
        : _free_space(free_space), _total_space(total_space) { }

    bool operator<(const occupancy_stats& other) const {
        return used_fraction() < other.used_fraction();
    }

    friend occupancy_stats operator+(const occupancy_stats& s1, const occupancy_stats& s2) {
        occupancy_stats result(s1);
        result += s2;
        return result;
    }

    friend occupancy_stats operator-(const occupancy_stats& s1, const occupancy_stats& s2) {
        occupancy_stats result(s1);
        result -= s2;
        return result;
    }

    occupancy_stats& operator+=(const occupancy_stats& other) {
        _total_space += other._total_space;
        _free_space += other._free_space;
        return *this;
    }

    occupancy_stats& operator-=(const occupancy_stats& other) {
        _total_space -= other._total_space;
        _free_space -= other._free_space;
        return *this;
    }

    size_t used_space() const {
        return _total_space - _free_space;
    }

    size_t free_space() const {
        return _free_space;
    }

    size_t total_space() const {
        return _total_space;
    }

    float used_fraction() const {
        return _total_space ? float(used_space()) / total_space() : 0;
    }

    friend std::ostream& operator<<(std::ostream&, const occupancy_stats&);
};

//
// Log-structured allocator region.
//
// Objects allocated using this region are said to be owned by this region.
// Objects must be freed only using the region which owns them. Ownership can
// be transferred across regions using the merge() method. Region must be live
// as long as it owns any objects.
//
// Each region has separate memory accounting and can be compacted
// independently from other regions. To reclaim memory from all regions use
// shard_tracker().
//
// Region is automatically added to the set of
// compactible regions when constructed.
//
class region {
public:
    using impl = region_impl;
private:
    shared_ptr<impl> _impl;
public:
    region();
    explicit region(region_group& group);
    ~region();
    region(region&& other);
    region& operator=(region&& other);
    region(const region& other) = delete;

    occupancy_stats occupancy() const;

    allocation_strategy& allocator();

    // Merges another region into this region. The other region is left empty.
    // Doesn't invalidate references to allocated objects.
    void merge(region& other);

    // Compacts everything. Mainly for testing.
    // Invalidates references to allocated objects.
    void full_compaction();

    // Changes the reclaimability state of this region. When region is not
    // reclaimable, it won't be considered by tracker::reclaim(). By default region is
    // reclaimable after construction.
    void set_reclaiming_enabled(bool);

    // Returns the reclaimability state of this region.
    bool reclaiming_enabled() const;

    // Returns a value which is increased when this region is either compacted or
    // evicted from, which invalidates references into the region.
    // When the value returned by this method doesn't change, references remain valid.
    uint64_t reclaim_counter() const;

    // Makes this region an evictable region. Supplied function will be called
    // when data from this region needs to be evicted in order to reclaim space.
    // The function should free some space from this region.
    void make_evictable(eviction_fn);

    friend class region_group;
    friend class allocating_section;
};

// Forces references into the region to remain valid as long as this guard is
// live by disabling compaction and eviction.
// Can be nested.
struct reclaim_lock {
    region& _region;
    bool _prev;
    reclaim_lock(region& r)
        : _region(r)
        , _prev(r.reclaiming_enabled())
    {
        _region.set_reclaiming_enabled(false);
    }
    ~reclaim_lock() {
        _region.set_reclaiming_enabled(_prev);
    }
};

// Utility for running critical sections which need to lock some region and
// also allocate LSA memory. The object learns from failures how much it
// should reserve up front in order to not cause allocation failures.
class allocating_section {
    size_t _lsa_reserve = 10; // in segments
    size_t _std_reserve = 1024; // in bytes
private:
    struct guard {
        size_t _prev;
        guard();
        ~guard();
        void enter(allocating_section&);
    };
    void on_alloc_failure();
public:
    //
    // Invokes func with reclaim_lock on region r. If LSA allocation fails
    // inside func it is retried after increasing LSA segment reserve. The
    // memory reserves are increased with region lock off allowing for memory
    // reclamation to take place in the region.
    //
    // Throws std::bad_alloc when reserves can't be increased to a sufficient level.
    //
    template<typename Func>
    decltype(auto) operator()(logalloc::region& r, Func&& func) {
        auto prev_lsa_reserve = _lsa_reserve;
        auto prev_std_reserve = _std_reserve;
        try {
            while (true) {
                assert(r.reclaiming_enabled());
                guard g;
                g.enter(*this);
                try {
                    logalloc::reclaim_lock _(r);
                    return func();
                } catch (const std::bad_alloc&) {
                    on_alloc_failure();
                }
            }
        } catch (const std::bad_alloc&) {
            // roll-back limits to protect against pathological requests
            // preventing future requests from succeeding.
            _lsa_reserve = prev_lsa_reserve;
            _std_reserve = prev_std_reserve;
            throw;
        }
    }
};

}
