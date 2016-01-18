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

namespace service {
class priority_manager {
    ::io_priority_class _commitlog_priority;
    ::io_priority_class _mt_flush_priority;
    ::io_priority_class _mut_stream_priority;
    ::io_priority_class _sstable_query_read;
    ::io_priority_class _compaction_priority;
public:
    const ::io_priority_class&
    commitlog_priority() {
        return _commitlog_priority;
    }

    const ::io_priority_class&
    memtable_flush_priority() {
        return _mt_flush_priority;
    }

    const ::io_priority_class&
    mutation_stream_priority() {
        return _mut_stream_priority;
    }

    const ::io_priority_class&
    sstable_query_read_priority() {
        return _sstable_query_read;
    }

    const ::io_priority_class&
    compaction_priority() {
        return _compaction_priority;
    }

    priority_manager()
        : _commitlog_priority(engine().register_one_priority_class(100))
        , _mt_flush_priority(engine().register_one_priority_class(100))
        , _mut_stream_priority(engine().register_one_priority_class(100))
        , _sstable_query_read(engine().register_one_priority_class(100))
        , _compaction_priority(engine().register_one_priority_class(10))

    {}
};

priority_manager& get_local_priority_manager();
}
