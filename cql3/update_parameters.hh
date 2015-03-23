/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#ifndef CQL3_UPDATE_PARAMETERS_HH
#define CQL3_UPDATE_PARAMETERS_HH

#include "database.hh"
#include "exceptions/exceptions.hh"

namespace cql3 {

/**
 * A simple container that simplify passing parameters for collections methods.
 */
class update_parameters final {
public:
    struct prefetch_data {
        query::partition_slice slice;
        query::result result;
    };
    // Note: value (mutation) only required to contain the rows we are interested in
    using prefetched_rows_type = std::experimental::optional<prefetch_data>;
private:
    const gc_clock::duration _ttl;
    const prefetched_rows_type _prefetched; // For operation that require a read-before-write
public:
    const api::timestamp_type _timestamp;
    const gc_clock::time_point _local_deletion_time;
    const schema_ptr _schema;
    const query_options& _options;

    update_parameters(const schema_ptr schema_, const query_options& options,
            api::timestamp_type timestamp, gc_clock::duration ttl, prefetched_rows_type prefetched)
        : _ttl(ttl)
        , _prefetched(std::move(prefetched))
        , _timestamp(timestamp)
        , _local_deletion_time(gc_clock::now())
        , _schema(std::move(schema_))
        , _options(options)
    {
        // We use MIN_VALUE internally to mean the absence of of timestamp (in Selection, in sstable stats, ...), so exclude
        // it to avoid potential confusion.
        if (timestamp < api::min_timestamp || timestamp > api::max_timestamp) {
            throw exceptions::invalid_request_exception(sprint("Out of bound timestamp, must be in [%d, %d]",
                    api::min_timestamp, api::max_timestamp));
        }
    }

    atomic_cell make_dead_cell() const {
        return atomic_cell::make_dead(_timestamp, _local_deletion_time);
    }

    atomic_cell make_cell(bytes_view value) const {
        auto ttl = _ttl;

        if (ttl.count() <= 0) {
            ttl = _schema->default_time_to_live;
        }

        return atomic_cell::make_live(_timestamp,
            ttl.count() > 0 ? ttl_opt{_local_deletion_time + ttl} : ttl_opt{}, value);
    };

#if 0
     public Cell makeCounter(CellName name, long delta) throws InvalidRequestException
     {
         QueryProcessor.validateCellName(name, metadata.comparator);
         return new BufferCounterUpdateCell(name, delta, FBUtilities.timestampMicros());
     }
#endif

    tombstone make_tombstone() const {
        return {_timestamp, _local_deletion_time};
    }

#if 0
    public RangeTombstone makeRangeTombstone(ColumnSlice slice) throws InvalidRequestException
    {
        QueryProcessor.validateComposite(slice.start, metadata.comparator);
        QueryProcessor.validateComposite(slice.finish, metadata.comparator);
        return new RangeTombstone(slice.start, slice.finish, timestamp, localDeletionTime);
    }

    public RangeTombstone makeTombstoneForOverwrite(ColumnSlice slice) throws InvalidRequestException
    {
        QueryProcessor.validateComposite(slice.start, metadata.comparator);
        QueryProcessor.validateComposite(slice.finish, metadata.comparator);
        return new RangeTombstone(slice.start, slice.finish, timestamp - 1, localDeletionTime);
    }
#endif

    collection_mutation::view get_prefetched_list(const partition_key& pkey,
            const clustering_key& row_key, const column_definition& column) const {
        if (!_prefetched) {
            return {};
        }
        auto&& parts = _prefetched->result.partitions;
        auto&& pkcmp = partition_key::equality(*_schema);
        auto i = std::find_if(parts.begin(), parts.end(), [&] (auto&& key_and_part) {
            return pkcmp(pkey, key_and_part.first);
        });
        if (i == parts.end()) {
            return {};
        }
        auto&& part = i->second;
        auto which = &query::partition_slice::regular_columns;
        if (column.is_static()) {
            which = &query::partition_slice::static_columns;
        }
        auto&& idxvec = _prefetched->slice.*which;
        auto&& j = std::find(idxvec.begin(), idxvec.end(), column.id);
        assert(j != idxvec.end());
        auto colidx = j - idxvec.begin();
        auto* row = &part.static_row;
        auto&& keycmp = clustering_key::equality(*_schema);
        if (column.is_static()) {
            auto k = std::find_if(part.rows.begin(), part.rows.end(), [&] (auto&& key_and_row) {
                return keycmp(row_key, key_and_row.first);
            });
            if (k == part.rows.end()) {
                return {};
            }
            row = &k->second;
        }
        auto&& cell = row->cells[colidx];
        if (!cell) {
            return {};
        }
        return cell->as_collection_mutation();
    }
};

}

#endif
