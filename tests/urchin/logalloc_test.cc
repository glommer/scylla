/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <algorithm>

#include <seastar/core/thread.hh>
#include <seastar/tests/test-utils.hh>

#include "utils/logalloc.hh"
#include "utils/managed_ref.hh"
#include "utils/blob.hh"
#include "log.hh"

static auto x = [] {
    logging::logger_registry().set_all_loggers_level(logging::log_level::debug);
    return 0;
}();

using namespace logalloc;

SEASTAR_TEST_CASE(test_compaction) {
    return seastar::async([] {
        using region_type = region<typeset<managed<int>>>;
        using allocator = region_allocator<region_type>;
        region_type reg;

        with_region(reg, [] {
            std::vector<managed_ref<int, allocator>> _allocated;

            // Allocate several segments

            for (int i = 0; i < 32 * 1024 * 4; i++) {
                _allocated.push_back(make_managed<int, allocator>());
            }

            // Free 1/3 randomly

            std::random_shuffle(_allocated.begin(), _allocated.end());

            auto it = _allocated.begin();
            size_t nr_freed = _allocated.size() / 3;
            for (size_t i = 0; i < nr_freed; ++i) {
                *it++ = {};
            }

            // Try to reclaim

            reclaim(sizeof(managed<int>) * nr_freed);
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

        using region_type = region<typeset<managed<A>, managed<B>, managed<C>, int>>;
        using allocator = region_allocator<region_type>;

        region_type reg;
        with_region(reg, [&] {
            {
                std::vector<int*> objs;

                auto p1 = make_managed<A, allocator>();

                int junk_count = 10;

                for (int i = 0; i < junk_count; i++) {
                    objs.push_back(reg.construct<int>(i));
                }

                auto p2 = make_managed<B, allocator>();

                for (int i = 0; i < junk_count; i++) {
                    objs.push_back(reg.construct<int>(i));
                }

                auto p3 = make_managed<C, allocator>();

                for (auto&& p : objs) {
                    reg.destroy(p);
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
        using region_type = region<typeset<blob_storage>>;
        region_type reg;

        with_region(reg, [] {
            auto src = bytes("123456");
            blob<region_allocator<region_type>> b(src);
            BOOST_REQUIRE(bytes_view(b) == src);
            {
                auto b2 = std::move(b);
                BOOST_REQUIRE(bytes_view(b2) == src);
            }
        });
    });
}
