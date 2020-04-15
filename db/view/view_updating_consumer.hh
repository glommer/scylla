/*
 * Copyright (C) 2018 ScyllaDB
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

#include "dht/i_partitioner.hh"
#include "schema_fwd.hh"
#include "mutation_fragment.hh"
#include "sstables/shared_sstable.hh"
#include "database.hh"

namespace db::view {

/*
 * A consumer that pushes materialized view updates for each consumed mutation.
 * It is expected to be run in seastar::async threaded context through consume_in_thread()
 */
class view_updating_consumer {
    schema_ptr _schema;
    lw_shared_ptr<table> _table;
    std::vector<sstables::shared_sstable> _excluded_sstables;
    const seastar::abort_source& _as;
    std::optional<mutation> _m;
public:
    view_updating_consumer(schema_ptr schema, table& table, std::vector<sstables::shared_sstable> excluded_sstables, const seastar::abort_source& as)
            : _schema(std::move(schema))
            , _table(table.shared_from_this())
            , _excluded_sstables(std::move(excluded_sstables))
            , _as(as)
            , _m()
    { }

    view_updating_consumer(const view_updating_consumer& o) = delete;
    view_updating_consumer& operator=(const view_updating_consumer& o) = delete;
    view_updating_consumer(view_updating_consumer&& o) = default;
    view_updating_consumer& operator=(view_updating_consumer&& o) = default;
    ~view_updating_consumer() = default;

    void consume_new_partition(const dht::decorated_key& dk) {
        _m = mutation(_schema, dk, mutation_partition(_schema));
    }

    void consume(tombstone t) {
        _m->partition().apply(std::move(t));
    }

    stop_iteration consume(static_row&& sr) {
        if (_as.abort_requested()) {
            return stop_iteration::yes;
        }
        _m->partition().apply(*_schema, std::move(sr));
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) {
        if (_as.abort_requested()) {
            return stop_iteration::yes;
        }
        _m->partition().apply(*_schema, std::move(cr));
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone&& rt) {
        if (_as.abort_requested()) {
            return stop_iteration::yes;
        }
        _m->partition().apply(*_schema, std::move(rt));
        return stop_iteration::no;
    }

    // Expected to be run in seastar::async threaded context (consume_in_thread())
    stop_iteration consume_end_of_partition();

    stop_iteration consume_end_of_stream() {
        return stop_iteration(_as.abort_requested());
    }
};

}

