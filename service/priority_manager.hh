/*
 * Copyright 2016 ScyllaDB
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

#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/enum.hh>
#include <seastar/core/timer.hh>
#include <chrono>

namespace service {
class priority_manager {
public:
    enum class priority_class_id { commitlog, memtable_flush, mutation_streaming, sstable_query_read, compaction };
private:
    struct priority_class {
        ::io_priority_class pclass;
        uint32_t current_shares;
        std::function<int()> pressure;
        priority_class(sstring name, uint32_t shares)
            : pclass(engine().register_one_priority_class(name, shares))
            , current_shares(shares)
            , pressure([] { return 50; })
        {}
    };

    std::unordered_map<priority_class_id, priority_class, enum_hash<priority_class_id>> _classes;
    timer<> _adjustment_timer;
    void adjust_priorities();
public:

    void update_pressure_function(priority_class_id id, std::function<int()> func) {
        _classes.at(id).pressure = std::move(func);
    }

    const ::io_priority_class&
    commitlog_priority() {
        return _classes.at(priority_class_id::commitlog).pclass;
    }

    const ::io_priority_class&
    memtable_flush_priority() {
        return _classes.at(priority_class_id::memtable_flush).pclass;
    }

    const ::io_priority_class&
    mutation_stream_priority() {
        return _classes.at(priority_class_id::mutation_streaming).pclass;
    }

    const ::io_priority_class&
    sstable_query_read_priority() {
        return _classes.at(priority_class_id::sstable_query_read).pclass;
    }

    const ::io_priority_class&
    compaction_priority() {
        return _classes.at(priority_class_id::compaction).pclass;
    }

    priority_manager()
        : _classes({
            { priority_class_id::commitlog, priority_class{"commitlog", 100}},
            { priority_class_id::memtable_flush, priority_class{"memtable_flush", 100}},
            { priority_class_id::mutation_streaming, priority_class{"mutation_streaming", 100}},
            { priority_class_id::sstable_query_read, priority_class{"sstable_query_read", 100}},
            { priority_class_id::compaction, priority_class{"compaction", 100}},
        })
        , _adjustment_timer([this] { adjust_priorities(); })
    {
        _adjustment_timer.arm_periodic(std::chrono::seconds(1));
    }
};

priority_manager& get_local_priority_manager();
const inline ::io_priority_class&
get_local_commitlog_priority() {
    return get_local_priority_manager().commitlog_priority();
}

const inline ::io_priority_class&
get_local_memtable_flush_priority() {
    return get_local_priority_manager().memtable_flush_priority();
}

const inline ::io_priority_class&
get_local_mutation_stream_priority() {
    return get_local_priority_manager().mutation_stream_priority();
}

const inline ::io_priority_class&
get_local_sstable_query_read_priority() {
    return get_local_priority_manager().sstable_query_read_priority();
}

const inline ::io_priority_class&
get_local_compaction_priority() {
    return get_local_priority_manager().compaction_priority();
}
}
