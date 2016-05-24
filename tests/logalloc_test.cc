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

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <algorithm>
#include <chrono>

#include <seastar/core/thread.hh>
#include <seastar/core/sleep.hh>
#include <seastar/tests/test-utils.hh>
#include <deque>

#include "utils/logalloc.hh"
#include "utils/managed_ref.hh"
#include "utils/managed_bytes.hh"
#include "log.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

[[gnu::unused]]
static auto x = [] {
    logging::logger_registry().set_all_loggers_level(logging::log_level::debug);
    return 0;
}();

using namespace logalloc;

SEASTAR_TEST_CASE(test_compaction) {
    return seastar::async([] {
        region reg;

        with_allocator(reg.allocator(), [&reg] {
            std::vector<managed_ref<int>> _allocated;

            // Allocate several segments

            auto reclaim_counter_1 = reg.reclaim_counter();

            for (int i = 0; i < 32 * 1024 * 4; i++) {
                _allocated.push_back(make_managed<int>());
            }

            // Allocation should not invalidate references
            BOOST_REQUIRE_EQUAL(reg.reclaim_counter(), reclaim_counter_1);

            shard_tracker().reclaim_all_free_segments();

            // Free 1/3 randomly

            std::random_shuffle(_allocated.begin(), _allocated.end());

            auto it = _allocated.begin();
            size_t nr_freed = _allocated.size() / 3;
            for (size_t i = 0; i < nr_freed; ++i) {
                *it++ = {};
            }

            // Freeing should not invalidate references
            BOOST_REQUIRE_EQUAL(reg.reclaim_counter(), reclaim_counter_1);

            // Try to reclaim

            size_t target = sizeof(managed<int>) * nr_freed;
            BOOST_REQUIRE(shard_tracker().reclaim(target) >= target);

            // There must have been some compaction during such reclaim
            BOOST_REQUIRE(reg.reclaim_counter() != reclaim_counter_1);
        });
    });
}


SEASTAR_TEST_CASE(test_compaction_with_multiple_regions) {
    return seastar::async([] {
        region reg1;
        region reg2;

        std::vector<managed_ref<int>> allocated1;
        std::vector<managed_ref<int>> allocated2;

        int count = 32 * 1024 * 4;
        
        with_allocator(reg1.allocator(), [&] {
            for (int i = 0; i < count; i++) {
                allocated1.push_back(make_managed<int>());
            }
        });

        with_allocator(reg2.allocator(), [&] {
            for (int i = 0; i < count; i++) {
                allocated2.push_back(make_managed<int>());
            }
        });

        size_t quarter = shard_tracker().region_occupancy().total_space() / 4;

        shard_tracker().reclaim_all_free_segments();

        // Can't reclaim anything yet
        BOOST_REQUIRE(shard_tracker().reclaim(quarter) == 0);
        
        // Free 60% from the second pool

        // Shuffle, so that we don't free whole segments back to the pool
        // and there's nothing to reclaim.
        std::random_shuffle(allocated2.begin(), allocated2.end());

        with_allocator(reg2.allocator(), [&] {
            auto it = allocated2.begin();
            for (size_t i = 0; i < (count * 0.6); ++i) {
                *it++ = {};
            }
        });

        BOOST_REQUIRE(shard_tracker().reclaim(quarter) >= quarter);
        BOOST_REQUIRE(shard_tracker().reclaim(quarter) < quarter);

        // Free 60% from the first pool

        std::random_shuffle(allocated1.begin(), allocated1.end());

        with_allocator(reg1.allocator(), [&] {
            auto it = allocated1.begin();
            for (size_t i = 0; i < (count * 0.6); ++i) {
                *it++ = {};
            }
        });

        BOOST_REQUIRE(shard_tracker().reclaim(quarter) >= quarter);
        BOOST_REQUIRE(shard_tracker().reclaim(quarter) < quarter);

        with_allocator(reg2.allocator(), [&] () mutable {
            allocated2.clear();
        });

        with_allocator(reg1.allocator(), [&] () mutable {
            allocated1.clear();
        });
    });
}

SEASTAR_TEST_CASE(test_mixed_type_compaction) {
    return seastar::async([] {
        static bool a_moved = false;
        static bool b_moved = false;
        static bool c_moved = false;

        static bool a_destroyed = false;
        static bool b_destroyed = false;
        static bool c_destroyed = false;

        struct A {
            uint8_t v = 0xca;
            A() = default;
            A(A&&) noexcept {
                a_moved = true;
            }
            ~A() {
                BOOST_REQUIRE(v == 0xca);
                a_destroyed = true;
            }
        };
        struct B {
            uint16_t v = 0xcafe;
            B() = default;
            B(B&&) noexcept {
                b_moved = true;
            }
            ~B() {
                BOOST_REQUIRE(v == 0xcafe);
                b_destroyed = true;
            }
        };
        struct C {
            uint64_t v = 0xcafebabe;
            C() = default;
            C(C&&) noexcept {
                c_moved = true;
            }
            ~C() {
                BOOST_REQUIRE(v == 0xcafebabe);
                c_destroyed = true;
            }
        };

        region reg;
        with_allocator(reg.allocator(), [&] {
            {
                std::vector<int*> objs;

                auto p1 = make_managed<A>();

                int junk_count = 10;

                for (int i = 0; i < junk_count; i++) {
                    objs.push_back(reg.allocator().construct<int>(i));
                }

                auto p2 = make_managed<B>();

                for (int i = 0; i < junk_count; i++) {
                    objs.push_back(reg.allocator().construct<int>(i));
                }

                auto p3 = make_managed<C>();

                for (auto&& p : objs) {
                    reg.allocator().destroy(p);
                }

                reg.full_compaction();

                BOOST_REQUIRE(a_moved);
                BOOST_REQUIRE(b_moved);
                BOOST_REQUIRE(c_moved);

                BOOST_REQUIRE(a_destroyed);
                BOOST_REQUIRE(b_destroyed);
                BOOST_REQUIRE(c_destroyed);

                a_destroyed = false;
                b_destroyed = false;
                c_destroyed = false;
            }

            BOOST_REQUIRE(a_destroyed);
            BOOST_REQUIRE(b_destroyed);
            BOOST_REQUIRE(c_destroyed);
        });
    });
}

SEASTAR_TEST_CASE(test_blob) {
    return seastar::async([] {
        region reg;
        with_allocator(reg.allocator(), [&] {
            auto src = bytes("123456");
            managed_bytes b(src);

            BOOST_REQUIRE(bytes_view(b) == src);

            reg.full_compaction();

            BOOST_REQUIRE(bytes_view(b) == src);
        });
    });
}

SEASTAR_TEST_CASE(test_merging) {
    return seastar::async([] {
        region reg1;
        region reg2;

        reg1.merge(reg2);

        managed_ref<int> r1;

        with_allocator(reg1.allocator(), [&] {
            r1 = make_managed<int>();
        });

        reg2.merge(reg1);

        with_allocator(reg2.allocator(), [&] {
            r1 = {};
        });

        std::vector<managed_ref<int>> refs;

        with_allocator(reg1.allocator(), [&] {
            for (int i = 0; i < 10000; ++i) {
                refs.emplace_back(make_managed<int>());
            }
        });

        reg2.merge(reg1);

        with_allocator(reg2.allocator(), [&] {
            refs.clear();
        });
    });
}

#ifndef DEFAULT_ALLOCATOR
SEASTAR_TEST_CASE(test_region_lock) {
    return seastar::async([] {
        region reg;
        with_allocator(reg.allocator(), [&] {
            std::deque<managed_bytes> refs;

            for (int i = 0; i < 1024 * 10; ++i) {
                refs.push_back(managed_bytes(managed_bytes::initialized_later(), 1024));
            }

            // Evict 30% so that region is compactible, but do it randomly so that
            // segments are not released into the standard allocator without compaction.
            std::random_shuffle(refs.begin(), refs.end());
            for (size_t i = 0; i < refs.size() * 0.3; ++i) {
                refs.pop_back();
            }

            reg.make_evictable([&refs] {
                if (refs.empty()) {
                    return memory::reclaiming_result::reclaimed_nothing;
                }
                refs.pop_back();
                return memory::reclaiming_result::reclaimed_something;
            });

            std::deque<bytes> objects;

            auto counter = reg.reclaim_counter();

            // Verify that with compaction lock we rather run out of memory
            // than compact it
            {
                BOOST_REQUIRE(reg.reclaiming_enabled());

                logalloc::reclaim_lock _(reg);

                BOOST_REQUIRE(!reg.reclaiming_enabled());
                auto used_before = reg.occupancy().used_space();

                try {
                    while (true) {
                        objects.push_back(bytes(bytes::initialized_later(), 1024*1024));
                    }
                } catch (const std::bad_alloc&) {
                    // expected
                }

                BOOST_REQUIRE(reg.reclaim_counter() == counter);
                BOOST_REQUIRE(reg.occupancy().used_space() == used_before); // eviction is also disabled
            }

            BOOST_REQUIRE(reg.reclaiming_enabled());
        });
    });
}

SEASTAR_TEST_CASE(test_large_allocation) {
    return seastar::async([] {
        logalloc::region r_evictable;
        logalloc::region r_non_evictable;

        static constexpr unsigned element_size = 16 * 1024;

        std::deque<managed_bytes> evictable;
        std::deque<managed_bytes> non_evictable;
        try {
            while (true) {
                with_allocator(r_evictable.allocator(), [&] {
                    evictable.push_back(bytes(bytes::initialized_later(),element_size));
                });
                with_allocator(r_non_evictable.allocator(), [&] {
                    non_evictable.push_back(bytes(bytes::initialized_later(),element_size));
                });
            }
        } catch (const std::bad_alloc&) {
            // expected
        }

        std::random_shuffle(evictable.begin(), evictable.end());
        r_evictable.make_evictable([&] {
            return with_allocator(r_evictable.allocator(), [&] {
                if (evictable.empty()) {
                    return memory::reclaiming_result::reclaimed_nothing;
                }
                evictable.pop_front();
                return memory::reclaiming_result::reclaimed_something;
            });
        });

        auto clear_all = [&] {
            with_allocator(r_non_evictable.allocator(), [&] {
                non_evictable.clear();
            });
            with_allocator(r_evictable.allocator(), [&] {
                evictable.clear();
            });
        };

        try {
            auto ptr = std::make_unique<char[]>(evictable.size() * element_size / 4 * 3);
        } catch (const std::bad_alloc&) {
            // This shouldn't have happened, but clear remaining lsa data
            // properly so that humans see bad_alloc instead of some confusing
            // assertion failure caused by destroying evictable and
            // non_evictable without with_allocator().
            clear_all();
            throw;
        }

        clear_all();
    });
}
#endif

SEASTAR_TEST_CASE(test_region_groups) {
    return seastar::async([] {
        logalloc::region_group just_four;
        logalloc::region_group all;
        logalloc::region_group one_and_two(&all);

        auto one = std::make_unique<logalloc::region>(one_and_two);
        auto two = std::make_unique<logalloc::region>(one_and_two);
        auto three = std::make_unique<logalloc::region>(all);
        auto four = std::make_unique<logalloc::region>(just_four);
        auto five = std::make_unique<logalloc::region>();

        constexpr size_t one_count = 1024 * 1024;
        std::vector<managed_ref<int>> one_objs;
        with_allocator(one->allocator(), [&] {
            for (size_t i = 0; i < one_count; i++) {
                one_objs.emplace_back(make_managed<int>());
            }
        });
        BOOST_REQUIRE_GE(ssize_t(one->occupancy().used_space()), ssize_t(one_count * sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(one->occupancy().total_space()), ssize_t(one->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(one_and_two.memory_used(), one->occupancy().total_space());
        BOOST_REQUIRE_EQUAL(all.memory_used(), one->occupancy().total_space());

        constexpr size_t two_count = 512 * 1024;
        std::vector<managed_ref<int>> two_objs;
        with_allocator(two->allocator(), [&] {
            for (size_t i = 0; i < two_count; i++) {
                two_objs.emplace_back(make_managed<int>());
            }
        });
        BOOST_REQUIRE_GE(ssize_t(two->occupancy().used_space()), ssize_t(two_count * sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(two->occupancy().total_space()), ssize_t(two->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(one_and_two.memory_used(), one->occupancy().total_space() + two->occupancy().total_space());
        BOOST_REQUIRE_EQUAL(all.memory_used(), one_and_two.memory_used());

        constexpr size_t three_count = 2048 * 1024;
        std::vector<managed_ref<int>> three_objs;
        with_allocator(three->allocator(), [&] {
            for (size_t i = 0; i < three_count; i++) {
                three_objs.emplace_back(make_managed<int>());
            }
        });
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().used_space()), ssize_t(three_count * sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().total_space()), ssize_t(three->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(all.memory_used(), one_and_two.memory_used() + three->occupancy().total_space());

        constexpr size_t four_count = 256 * 1024;
        std::vector<managed_ref<int>> four_objs;
        with_allocator(four->allocator(), [&] {
            for (size_t i = 0; i < four_count; i++) {
                four_objs.emplace_back(make_managed<int>());
            }
        });
        BOOST_REQUIRE_GE(ssize_t(four->occupancy().used_space()), ssize_t(four_count * sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(four->occupancy().total_space()), ssize_t(four->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(just_four.memory_used(), four->occupancy().total_space());

        with_allocator(five->allocator(), [] {
            std::vector<managed_ref<int>> five_objs;
            for (size_t i = 0; i < 16 * 1024; i++) {
                five_objs.emplace_back(make_managed<int>());
            }
        });

        three->merge(*four);
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().used_space()), ssize_t((three_count  + four_count)* sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().total_space()), ssize_t(three->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(all.memory_used(), one_and_two.memory_used() + three->occupancy().total_space());
        BOOST_REQUIRE_EQUAL(just_four.memory_used(), 0);

        three->merge(*five);
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().used_space()), ssize_t((three_count  + four_count)* sizeof(int)));
        BOOST_REQUIRE_GE(ssize_t(three->occupancy().total_space()), ssize_t(three->occupancy().used_space()));
        BOOST_REQUIRE_EQUAL(all.memory_used(), one_and_two.memory_used() + three->occupancy().total_space());

        with_allocator(two->allocator(), [&] {
            two_objs.clear();
        });
        two.reset();
        BOOST_REQUIRE_EQUAL(one_and_two.memory_used(), one->occupancy().total_space());
        BOOST_REQUIRE_EQUAL(all.memory_used(), one_and_two.memory_used() + three->occupancy().total_space());

        with_allocator(one->allocator(), [&] {
            one_objs.clear();
        });
        one.reset();
        BOOST_REQUIRE_EQUAL(one_and_two.memory_used(), 0);
        BOOST_REQUIRE_EQUAL(all.memory_used(), three->occupancy().total_space());

        with_allocator(three->allocator(), [&] {
            three_objs.clear();
            four_objs.clear();
        });
        three.reset();
        four.reset();
        five.reset();
        BOOST_REQUIRE_EQUAL(all.memory_used(), 0);
    });
}

using namespace std::chrono_literals;
void quiesce() {
    // Unfortunately seastar::thread::yield is not enough here, because the process of releasing
    // a request may be broken into many continuations. While we could just yield many times, the
    // exact amount needed to guarantee execution would be dependent on the internals of the
    // implementation, we want to avoid that.
    sleep(1ms).get();
}

SEASTAR_TEST_CASE(test_region_groups_basic_throttling) {
    return seastar::async([] {
        // singleton hierarchy, only one segment allowed
        logalloc::region_group simple(logalloc::segment_size);
        auto simple_region = std::make_unique<logalloc::region>(simple);
        std::vector<managed_ref<uint64_t>> simple_objs;

        auto alloc_one_obj = [&simple_region, &simple_objs] {
            with_allocator(simple_region->allocator(), [&simple_objs] {
                simple_objs.emplace_back(make_managed<uint64_t>());
            });
        };

        // Expectation: after first allocation region will have one segment,
        // memory_used() == throttle_threshold and we are good to go, future
        // is ready immediately.
        //
        // The allocation of the first element won't change the memory usage inside
        // the group and we'll be okay to do that a second time.
        auto fut = simple.run_when_memory_available(alloc_one_obj);
        BOOST_REQUIRE_EQUAL(simple.memory_used(), logalloc::segment_size);
        BOOST_REQUIRE_EQUAL(fut.available(), true);

        fut = simple.run_when_memory_available(alloc_one_obj);
        BOOST_REQUIRE_EQUAL(simple.memory_used(), logalloc::segment_size);
        BOOST_REQUIRE_EQUAL(fut.available(), true);

        auto big_region = std::make_unique<logalloc::region>(simple);
        // Allocate a big chunk, that will certainly get us over the threshold
        std::vector<managed_bytes> big_alloc;
        with_allocator(big_region->allocator(), [&] {
            big_alloc.push_back(bytes(bytes::initialized_later(), logalloc::segment_size));
        });

        // We should not be permitted to go forward with a new allocation now...
        fut = simple.run_when_memory_available(alloc_one_obj);
        BOOST_REQUIRE_GT(simple.memory_used(), logalloc::segment_size);
        BOOST_REQUIRE_EQUAL(fut.available(), false);

        // But when we remove the big bytes allocator from the region, then we should.
        // Internally, we can't guarantee that just freeing the object will give the segment back,
        // that's up to the internal policies. So to make sure we need to remove the whole region.
        with_allocator(big_region->allocator(), [&] {
            big_alloc.clear();
        });
        big_region.reset();
        quiesce();

        BOOST_REQUIRE_EQUAL(fut.available(), true);
        BOOST_REQUIRE_EQUAL(simple_objs.size(), size_t(3));

        with_allocator(simple_region->allocator(), [&simple_objs] {
            simple_objs.clear();
        });
    });
}

SEASTAR_TEST_CASE(test_region_groups_linear_hierarchy_throttling_child_alloc) {
    return seastar::async([] {
        logalloc::region_group parent(2 * logalloc::segment_size);
        logalloc::region_group child(&parent, logalloc::segment_size);

        auto child_region = std::make_unique<logalloc::region>(child);
        auto parent_region = std::make_unique<logalloc::region>(parent);

        // fill the child. Try allocating at parent level. Should be allowed.
        std::vector<managed_bytes> big_alloc;
        std::vector<managed_ref<uint64_t>> small_alloc;

        with_allocator(child_region->allocator(), [&big_alloc] {
            big_alloc.push_back(bytes(bytes::initialized_later(), logalloc::segment_size));
        });
        BOOST_REQUIRE_GE(parent.memory_used(), logalloc::segment_size);

        auto fut = parent.run_when_memory_available([&parent_region, &small_alloc] {
            with_allocator(parent_region->allocator(), [&small_alloc] {
                small_alloc.emplace_back(make_managed<uint64_t>());
            });
        });
        BOOST_REQUIRE_EQUAL(fut.available(), true);
        BOOST_REQUIRE_GE(parent.memory_used(), 2 * logalloc::segment_size);

        // This time child will use all parent's memory. Note that because the child's memory limit
        // is lower than the parent's, for that to happen we need to allocate directly.
        with_allocator(child_region->allocator(), [&big_alloc] {
            big_alloc.push_back(bytes(bytes::initialized_later(), logalloc::segment_size));
        });
        BOOST_REQUIRE_GE(child.memory_used(), 2 * logalloc::segment_size);

        fut = parent.run_when_memory_available([&parent_region, &small_alloc] {
            with_allocator(parent_region->allocator(), [&small_alloc] {
                small_alloc.emplace_back(make_managed<uint64_t>());
            });
        });
        BOOST_REQUIRE_EQUAL(fut.available(), false);
        BOOST_REQUIRE_GE(parent.memory_used(), 2 * logalloc::segment_size);

        with_allocator(child_region->allocator(), [&big_alloc] {
            big_alloc.clear();
        });
        child_region.reset();
        quiesce();

        BOOST_REQUIRE_EQUAL(fut.available(), true);
        with_allocator(parent_region->allocator(), [&small_alloc] {
            small_alloc.clear();
        });
        parent_region.reset();
    });
}

SEASTAR_TEST_CASE(test_region_groups_linear_hierarchy_throttling_parent_alloc) {
    return seastar::async([] {
        logalloc::region_group parent(logalloc::segment_size);
        logalloc::region_group child(&parent, logalloc::segment_size);

        auto child_region = std::make_unique<logalloc::region>(child);
        auto parent_region = std::make_unique<logalloc::region>(parent);

        // fill the parent. Try allocating at child level. Should not be allowed.
        std::vector<managed_bytes> big_alloc;
        std::vector<managed_ref<uint64_t>> small_alloc;

        with_allocator(parent_region->allocator(), [&big_alloc] {
            big_alloc.push_back(bytes(bytes::initialized_later(), logalloc::segment_size));
        });
        BOOST_REQUIRE_GE(parent.memory_used(), logalloc::segment_size);

        auto fut = child.run_when_memory_available([&child_region, &small_alloc] {
            with_allocator(child_region->allocator(), [&small_alloc] {
                small_alloc.emplace_back(make_managed<uint64_t>());
            });
        });
        BOOST_REQUIRE_EQUAL(fut.available(), false);

        with_allocator(parent_region->allocator(), [&big_alloc] {
            big_alloc.clear();
        });
        parent_region.reset();
        quiesce();

        BOOST_REQUIRE_EQUAL(fut.available(), true);

        with_allocator(child_region->allocator(), [&small_alloc] {
            small_alloc.clear();
        });
        child_region.reset();
    });
}

SEASTAR_TEST_CASE(test_region_groups_fifo_order) {
    // tests that requests that are queued for later execution execute in FIFO order
    return seastar::async([] {
        logalloc::region_group rg(logalloc::segment_size);

        auto region = std::make_unique<logalloc::region>(rg);

        // fill the parent. Try allocating at child level. Should not be allowed.
        std::vector<managed_bytes> big_alloc;
        with_allocator(region->allocator(), [&big_alloc] {
            big_alloc.push_back(bytes(bytes::initialized_later(), logalloc::segment_size));
        });
        BOOST_REQUIRE_GE(rg.memory_used(), logalloc::segment_size);

        auto exec_cnt = make_lw_shared<int>(0);
        std::vector<future<>> executions;

        for (auto index = 0; index < 100; ++index) {
            auto fut = rg.run_when_memory_available([exec_cnt, index] {
                BOOST_REQUIRE_EQUAL(index, (*exec_cnt)++);
            });
            BOOST_REQUIRE_EQUAL(fut.available(), false);
            executions.push_back(std::move(fut));
        }

        with_allocator(region->allocator(), [&big_alloc] {
            big_alloc.clear();
        });
        region.reset();
        quiesce();

        when_all(executions.begin(), executions.end()).get();
    });
}



SEASTAR_TEST_CASE(test_region_groups_linear_hierarchy_throttling_moving_restriction) {
    // Hierarchy here is A -> B -> C.
    // We will fill B causing an execution in C to fail. We then fill A and free B.
    //
    // C should still be blocked.
    return seastar::async([] {
        logalloc::region_group root(logalloc::segment_size);
        logalloc::region_group inner(&root, logalloc::segment_size);
        logalloc::region_group child(&inner, logalloc::segment_size);

        auto child_region = std::make_unique<logalloc::region>(child);
        auto inner_region = std::make_unique<logalloc::region>(inner);
        auto root_region = std::make_unique<logalloc::region>(root);

        // fill the inner node. Try allocating at child level. Should not be allowed.
        circular_buffer<managed_bytes> big_alloc;
        std::vector<managed_ref<uint64_t>> small_alloc;

        with_allocator(inner_region->allocator(), [&big_alloc] {
            big_alloc.push_back(bytes(bytes::initialized_later(), logalloc::segment_size));
        });
        BOOST_REQUIRE_GE(inner.memory_used(), logalloc::segment_size);

        auto fut = child.run_when_memory_available([&child_region, &small_alloc] {
            with_allocator(child_region->allocator(), [&small_alloc] {
                small_alloc.emplace_back(make_managed<uint64_t>());
            });
        });
        BOOST_REQUIRE_EQUAL(fut.available(), false);

        // Now fill the root...
        with_allocator(root_region->allocator(), [&big_alloc] {
            big_alloc.push_back(bytes(bytes::initialized_later(), logalloc::segment_size));
        });
        BOOST_REQUIRE_GE(root.memory_used(), logalloc::segment_size);

        // And free the inner node. We will verify that
        // 1) the notifications that the inner node sent the child when it was freed won't
        //    erroneously cause it to execute
        // 2) the child is still able to receive notifications from the root
        with_allocator(inner_region->allocator(), [&big_alloc] {
            big_alloc.pop_front();
        });
        inner_region.reset();
        quiesce();

        // Verifying (1)
        BOOST_REQUIRE_EQUAL(fut.available(), false);

        // Verifying (2)
        with_allocator(root_region->allocator(), [&big_alloc] {
            big_alloc.pop_front();
        });
        root_region.reset();
        quiesce();
        BOOST_REQUIRE_EQUAL(fut.available(), true);

        with_allocator(child_region->allocator(), [&small_alloc] {
            small_alloc.clear();
        });
        child_region.reset();
    });
}

SEASTAR_TEST_CASE(test_region_groups_tree_hierarchy_throttling_leaf_alloc) {
    return seastar::async([] {
        class leaf {
            logalloc::region_group rg;
            std::unique_ptr<logalloc::region> region;
            std::vector<managed_bytes> allocs;
        public:
            leaf(logalloc::region_group& parent)
                : rg(&parent, logalloc::segment_size)
                , region(std::make_unique<logalloc::region>(rg))
                {}

            ~leaf() {
                with_allocator(region->allocator(), [this] {
                    allocs.clear();
                });
                region.reset();
            }

            void alloc(size_t size) {
                with_allocator(region->allocator(), [this, size] {
                    allocs.push_back(bytes(bytes::initialized_later(), size));
                });
            }
            future<> try_alloc(size_t size) {
                return rg.run_when_memory_available([this, size] {
                    alloc(size);
                });
            }
            void reset() {
                with_allocator(region->allocator(), [this] {
                    allocs.clear();
                });
                region.reset(new logalloc::region(rg));
            }
        };

        logalloc::region_group parent(logalloc::segment_size);
        leaf first_leaf(parent);
        leaf second_leaf(parent);
        leaf third_leaf(parent);

        first_leaf.alloc(logalloc::segment_size);
        second_leaf.alloc(logalloc::segment_size);
        third_leaf.alloc(logalloc::segment_size);

        auto fut_1 = first_leaf.try_alloc(sizeof(uint64_t));
        auto fut_2 = second_leaf.try_alloc(sizeof(uint64_t));
        auto fut_3 = third_leaf.try_alloc(sizeof(uint64_t));

        BOOST_REQUIRE_EQUAL(fut_1.available() || fut_2.available() || fut_3.available(), false);

        // Total memory is still 2 * segment_size, can't proceed
        first_leaf.reset();
        quiesce();

        BOOST_REQUIRE_EQUAL(fut_1.available() || fut_2.available() || fut_3.available(), false);

        // Now all futures should resolve.
        first_leaf.reset();
        second_leaf.reset();
        third_leaf.reset();
        quiesce();

        BOOST_REQUIRE_EQUAL(fut_1.available() && fut_2.available() && fut_3.available(), true);
    });
}
