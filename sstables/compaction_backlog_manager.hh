/*
 * Copyright (C) 2017 ScyllaDB
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

#include <unordered_set>
#include <memory>
#include <seastar/core/shared_ptr.hh>
#include "shared_sstable.hh"
#include "sstables/progress_monitor.hh"

class compaction_backlog_manager;

// Manages one individual source of compaction backlog, usually a column family.
class compaction_backlog_tracker {
public:
    // Every strategy object being compacted can return a structure like this representing how
    // backlogged it is. It is expected to be a number between 0 and 1, where 1 represents a backlog
    // so big that the strategy may need the entire CPU for itself to get rid of it.
    //
    // The strategy also needs to return how many bytes it took into account to get to that backlog
    // estimation. If the strategy is computing the backlog subtracting already partially compacted
    // bytes, for instance, that needs to be reflected in total bytes.
    struct backlog {
        double   backlog = 0;
        uint64_t total_bytes = 0;
    };

    struct impl {
        virtual backlog get_backlog() = 0;
        virtual void add_sstable(sstables::shared_sstable sst) = 0;
        virtual void remove_sstable(sstables::shared_sstable sst) = 0;

        virtual void register_partially_written_sstable(const sstables::writer_offset_tracker& wp) = 0;
        virtual void seal_partially_written_sstable(const sstables::writer_offset_tracker& wp) = 0;

        virtual void register_compacting_sstable(const sstables::reader_position_tracker& rp) = 0;
        virtual void finish_compacting_sstable(const sstables::reader_position_tracker& rp) = 0;
        virtual ~impl() { }
    };

    compaction_backlog_tracker(std::unique_ptr<impl> impl) : _impl(std::move(impl)) {}
    compaction_backlog_tracker(compaction_backlog_tracker&&) = default;
    compaction_backlog_tracker(const compaction_backlog_tracker&) = delete;

    backlog get_backlog();
    void add_sstable(sstables::shared_sstable sst);
    void remove_sstable(sstables::shared_sstable sst);
    void register_partially_written_sstable(const sstables::writer_offset_tracker& wp);
    void seal_partially_written_sstable(const sstables::writer_offset_tracker& wp);
    void register_compacting_sstable(const sstables::reader_position_tracker& rp);
    void finish_compacting_sstable(const sstables::reader_position_tracker& rp);
    void stop_tracking_backlog();
private:
    std::unique_ptr<impl> _impl;
    compaction_backlog_manager* _manager = nullptr;
    uint64_t _total_bytes = 0;
    bool _stop_requested = false;
    uint64_t _changes_in_progress = 0;
    void maybe_remove();
    friend class compaction_backlog_manager;
};

// System-wide manager for compaction backlog.
//
// The number of backlogs is the same as the number of column families, which can be very high.
// As an optimization we could keep column families that are not undergoing any change in a special
// list and only iterate over the ones that are changing.
//
// However, the calculation of the backlog is not that expensive: I have benchmarked the cost of
// computing the backlog of 3500 column families to less than 200 microseconds, and that is only
// requested periodically, for relatively large periods of hundreds of milliseconds.
//
// Keeping a static part for the backlog complicates the code significantly, though, so this will
// be left for a future optimization.
class compaction_backlog_manager {
    std::unordered_set<compaction_backlog_tracker*> _backlog_trackers;
    uint64_t _total_bytes = 0;
    void remove_backlog_tracker(compaction_backlog_tracker& tracker);

    friend class compaction_backlog_tracker;
public:
    double backlog() const;
    void register_backlog_tracker(compaction_backlog_tracker& tracker);
};
