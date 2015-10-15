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
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <seastar/core/reactor.hh>
#include <seastar/core/future-util.hh>
#include <boost/range/irange.hpp>

// We will keep a list of all the snapshots currently tracked by the system.
// This will allow us to easily figure out whether a snapshot exists or not in
// a certain keyspace. Note that the only thing we track about the snapshots is
// their names.
//
// In order to do that, we will keep a mapping between keyspaces and snapshots.
// All shards should see (potentially) all keyspaces, but instead of duplicating
// the existence of the snapshots into all shards, we will shard the distribution
// of the snapshot names.
//
// Instead of local tests, testing a snapshot existence may involve a trip to another
// CPU. But on the other hand, we guarantee that the memory usage diminishes and we
// don't have sync issues.
using snapshot_set = std::unordered_set<sstring>;
static inline auto& local_snapshots() {
    static thread_local std::unordered_map<sstring, snapshot_set> s;
    return s;
}

template <typename Func>
inline auto sharded_snapshot(sstring name, Func&& func) {
    auto shard = std::hash<sstring>()(name) % smp::count;
    return smp::submit_to(shard, [func = std::forward<Func>(func), name = std::move(name)] {
        return func(name);
    });
}

inline future<>
add_snapshot(sstring ks, sstring name) {
    return sharded_snapshot(std::move(name), [ks = std::move(ks)] (sstring name) {
        local_snapshots()[ks].insert(std::move(name));
        return make_ready_future<>();
    });
}

inline future<>
remove_snapshot(sstring ks, sstring name) {
    return sharded_snapshot(name, [ks = std::move(ks)] (sstring name) {
        local_snapshots()[ks].erase(std::move(name));
        return make_ready_future<>();
    });
}

inline future<>
remove_all_snapshots(sstring ks) {
    auto idx = boost::irange(0, int(smp::count));
    return parallel_for_each(idx.begin(), idx.end(), [ks = std::move(ks)] (auto shard) {
        return smp::submit_to(shard, [ks = std::move(ks)] {
            snapshot_set empty;
            local_snapshots()[ks].swap(empty);
        });
    });
}

inline future<bool>
snapshot_exists(sstring ks, sstring name) {
    return sharded_snapshot(std::move(name), [ks = std::move(ks)] (sstring name) {
        return make_ready_future<bool>(local_snapshots()[ks].count(name));
    });
}
