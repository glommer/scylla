/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "control_group.hh"

// FIXME: should be bytes, but we are not accounting
//        memory in bytes yet (in partitions, instead)
static constexpr uint64_t threshold = 500000;

void
control_group_impl::remove_dirty_memory(uint64_t delta) {
    _dirty_memory -= delta;
    if (_dirty_memory < threshold) {
        for (auto&& p : _waiters) {
            p.set_value();
        }
        _waiters.clear();
    }
}

future<>
control_group_impl::enter() {
    if (_dirty_memory < threshold) {
        return make_ready_future<>();
    }
    _waiters.emplace_back();
    return _waiters.back().get_future();
}

