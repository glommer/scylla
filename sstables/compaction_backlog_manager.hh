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

// Read and write progress are provided by structures present in progress_manager.hh
// However, we don't want to be tied to their lifetimes and for that reason we will not
// manipulate them directly in the backlog manager.
//
// The structure that register those progress structures - the monitors - will let us know when does
// the sstable-side tracking structures go out of scope. And in that case we can do something about
// it.
//
// These objects lifetime are tied to the SSTable shared object lifetime and are expected to be kept
// alive until the SSTable is no longer being written/compacted. In other words, they are expected
// to be kept alive until one of compaction_backlog_tracker's add_sstable, remove_sstable or
// revert_charges are called.
//
// The reason for us to bundle this into the add_sstable/remove_sstable lifetime, instead of dealing
// with them separately and removing when the data transfer is done is that if we unregister a
// progrees manager and only later on add/remove the corresponding SSTable we can see large valley
// or peaks in the backlog.
//
// Those valleys/peaks originate from the fact that we may be removing/adding a fair chunk of
// backlog that will only be corrected much later, when we call add_sstable / remove_sstable.
// Because of that, we want to keep the backlog originating from the progress manager around for as
// long as possible, up to the moment the sstable list is fixed. The compaction object hosting this
// will certainly be gone by then.
struct backlog_write_progress_manager {
    virtual uint64_t written() const = 0;
};

struct backlog_read_progress_manager {
    virtual uint64_t compacted() const = 0;
};

// Manages one individual source of compaction backlog, usually a column family.
class compaction_backlog_tracker : public enable_lw_shared_from_this<compaction_backlog_tracker> {
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

        virtual void register_partially_written_sstable(sstables::shared_sstable sst, backlog_write_progress_manager& wp) = 0;
        virtual void register_compacting_sstable(sstables::shared_sstable sst, backlog_read_progress_manager& rp) = 0;

        virtual void revert_charges(sstables::shared_sstable sst) = 0;
        virtual ~impl() { }
    };

    compaction_backlog_tracker(std::unique_ptr<impl> impl) : _impl(std::move(impl)) {}
    compaction_backlog_tracker(compaction_backlog_tracker&&) = default;
    compaction_backlog_tracker(const compaction_backlog_tracker&) = delete;
    ~compaction_backlog_tracker();

    backlog get_backlog();
    void add_sstable(sstables::shared_sstable sst);
    void remove_sstable(sstables::shared_sstable sst);
    void register_partially_written_sstable(sstables::shared_sstable sst, backlog_write_progress_manager& wp);
    void register_compacting_sstable(sstables::shared_sstable sst, backlog_read_progress_manager& rp);
    void transfer_ongoing_writes(lw_shared_ptr<compaction_backlog_tracker> new_bt);
    void revert_charges(sstables::shared_sstable sst);
private:
    std::unique_ptr<impl> _impl;
    // We keep track of this so that we can transfer to a new tracker if the compaction strategy is
    // changed in the middle of a compaction.
    std::unordered_map<sstables::shared_sstable, backlog_write_progress_manager*> _ongoing_writes;
    compaction_backlog_manager* _manager = nullptr;
    uint64_t _total_bytes = 0;
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
    void remove_backlog_tracker(compaction_backlog_tracker* tracker);
    friend class compaction_backlog_tracker;
public:
    double backlog() const;
    void register_backlog_tracker(lw_shared_ptr<compaction_backlog_tracker> tracker);
};
