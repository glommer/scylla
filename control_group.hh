/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once


/*

A control_group measures the amount of resources (memory, disk bandwidth,
network banwidth, storage) used by a group of consumers, and provides
throttling interfaces to limit resource consumption according to some
quality-of-service policy.

At preent the only policy is throttling all input until memory consumption
drops below some threshold.

*/

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <vector>
#include <type_traits>
#include <cstdint>

class control_group_impl final {
    uint64_t _dirty_memory = 0;
    std::vector<promise<>> _waiters;
private:
    control_group_impl(const control_group_impl&) = delete;
    void operator=(const control_group_impl&) = delete;
public:
    control_group_impl() = default;
    // measurement interfaces
    void add_dirty_memory(uint64_t delta) {
        _dirty_memory += delta;
    }
    void remove_dirty_memory(uint64_t delta);
public:
    // throttling interfaces

    // Allows a new request into the control group when conditions allos
    future<> enter();
    // Marks a request as having left the control group
    void leave() {}
};

class control_group final {
    lw_shared_ptr<control_group_impl> _impl;
private:
    control_group(lw_shared_ptr<control_group_impl> impl) : _impl(std::move(impl)) {}
public:
    control_group() = default;
public:
    // measurement interfaces
    void add_dirty_memory(uint64_t delta) {
        _impl->add_dirty_memory(delta);
    }
    void remove_dirty_memory(uint64_t delta) {
        _impl->remove_dirty_memory(delta);
    }
public:
    // throttling interfaces

    // Allows a new request into the control group when conditions allos
    future<> enter() { return _impl->enter(); }
    // Marks a request as having left the control group
    void leave() { return _impl->leave(); }
    friend control_group make_control_group();
};

inline
control_group make_control_group() {
    return control_group(make_lw_shared<control_group_impl>());
}

template <typename Func>
inline
futurize_t<std::result_of_t<Func ()>>
with_control_group(control_group& cg, Func&& func) {
    return cg.enter().then([func = std::forward<Func>(func)] {
        return func();
    }).finally([&cg] {
        cg.leave();
    });
}
