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
 * Modified by Cloudius Systems
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <memory>

#include "utils/data_output.hh"
#include "core/future.hh"
#include "core/shared_ptr.hh"
#include "core/stream.hh"
#include "utils/UUID.hh"
#include "replay_position.hh"

class file;

namespace db {

class config;

using cf_id_type = utils::UUID;

/*
 * Commit Log tracks every write operation into the system. The aim of
 * the commit log is to be able to successfully recover data that was
 * not stored to disk via the Memtable.
 *
 * This impl is cassandra log format compatible (for what it is worth).
 * The behaviour is similar, but not 100% identical as "stock cl".
 *
 * Files are managed with "normal" file writes (as normal as seastar
 * gets) - no mmapping. Data is kept in internal buffers which, when
 * full, are written to disk (see below). Files are also flushed
 * periodically (or always), ensuring all data is written + writes are
 * complete.
 *
 * In BATCH mode, every write to the log will also send the data to disk
 * + issue a flush and wait for both to complete.
 *
 * In PERIODIC mode, most writes will only add to the internal memory
 * buffers. If the mem buffer is saturated, data is sent to disk, but we
 * don't wait for the write to complete. However, if periodic (timer)
 * flushing has not been done in X ms, we will write + flush to file. In
 * which case we wait for it.
 *
 * The commitlog does not guarantee any ordering between "add" callers
 * (due to the above). The actual order in the commitlog is however
 * identified by the replay_position returned.
 *
 * Like the stock cl, the log segments keep track of the highest dirty
 * (added) internal position for a given table id (cf_id_type / UUID).
 * Code should ensure to use discard_completed_segments with UUID +
 * highest rp once a memtable has been flushed. This will allow
 * discarding used segments. Failure to do so will keep stuff
 * indefinately.
 */
class commitlog {
public:
    class segment_manager;
    class segment;
    class descriptor;

private:
    std::unique_ptr<segment_manager> _segment_manager;
public:
    enum class sync_mode {
        PERIODIC, BATCH
    };
    struct config {
        config() = default;
        config(const config&) = default;
        config(const db::config&);

        sstring commit_log_location;
        uint64_t commitlog_total_space_in_mb = 0; // TODO: not respected yet.
        uint64_t commitlog_segment_size_in_mb = 32;
        uint64_t commitlog_sync_period_in_ms = 10 * 1000; //TODO: verify default!

        sync_mode mode = sync_mode::PERIODIC;
    };

    commitlog(commitlog&&);
    ~commitlog();

    /**
     * Commitlog is created via a factory func.
     * This of course because it needs to access disk to get up to speed.
     * Optionally, could have an "init" func and require calling this.
     */
    static future<commitlog> create_commitlog(config);

    /**
     * Note: To be able to keep impl out of header file,
     * actual data writing is done via a std::function.
     * If it is proven that this has unacceptable overhead, this can be replace
     * by iter an interface or move segments and stuff into the header. But
     * I hope not.
     *
     * A serializing func is provided along with a parameter indicating the size
     * of data to be written. (See add).
     * Don't write less, absolutely don't write more...
     */
    using output = data_output;
    using serializer_func = std::function<void(output&)>;

    /**
     * Add a "Mutation" to the commit log.
     *
     * @param mutation_func a function that writes 'size' bytes to the log, representing the mutation.
     */
    future<replay_position> add(const cf_id_type& id, size_t size, serializer_func mutation_func);

    /**
     * Template version of add.
     * @param mu an invokable op that generates the serialized data. (Of size bytes)
     */
    template<typename _MutationOp>
    future<replay_position> add_mutation(const cf_id_type& id, size_t size, _MutationOp&& mu) {
        return add(id, size, [mu = std::forward<_MutationOp>(mu)](output& out) {
            mu(out);
        });
    }

    /**
     * Modifies the per-CF dirty cursors of any commit log segments for the column family according to the position
     * given. Discards any commit log segments that are no longer used.
     *
     * @param cfId    the column family ID that was flushed
     * @param context the replay position of the flush
     */
    void discard_completed_segments(const cf_id_type&, const replay_position&);

    /**
     * Returns a vector of the segment names
     */
    std::vector<sstring> get_active_segment_names() const;

    /**
     * Returns the largest amount of data that can be written in a single "mutation".
     */
    size_t max_record_size() const;

    future<> clear();

    const config& active_config() const;

    typedef std::function<future<>(temporary_buffer<char>)> commit_load_reader_func;

    static subscription<temporary_buffer<char>> read_log_file(file, commit_load_reader_func);
    static future<subscription<temporary_buffer<char>>> read_log_file(const sstring&, commit_load_reader_func);
private:
    commitlog(config);
};

}
