/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "core/semaphore.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/gate.hh"
#include "log.hh"
#include "utils/exponential_backoff_retry.hh"
#include <deque>
#include <vector>
#include <functional>

class column_family;

// Compaction manager is a feature used to manage compaction jobs from multiple
// column families pertaining to the same database.
// For each compaction job handler, there will be one fiber that will check for
// jobs, and if any, run it. FIFO ordering is implemented here.
class compaction_manager {
public:
    struct stats {
        int64_t pending_tasks = 0;
        int64_t completed_tasks = 0;
    };
private:
    struct task {
        future<> compaction_done = make_ready_future<>();
        semaphore compaction_sem = semaphore(0);
        seastar::gate compaction_gate;
        exponential_backoff_retry compaction_retry = exponential_backoff_retry(std::chrono::seconds(5), std::chrono::seconds(300));
        // CF being currently compacted.
        column_family* compacting_cf = nullptr;
    };

    // compaction manager may have N fibers to allow parallel compaction per shard.
    std::vector<lw_shared_ptr<task>> _tasks;

    // Queue shared among all tasks containing all column families to be compacted.
    std::deque<column_family*> _cfs_to_compact;

    // Used to assert that compaction_manager was explicitly stopped, if started.
    bool _stopped = true;

    stats _stats;
private:
    void task_start(lw_shared_ptr<task>& task);

    future<> task_stop(lw_shared_ptr<task>& task);
public:
    compaction_manager();
    ~compaction_manager();

    // Creates N fibers that will allow N compaction jobs to run in parallel.
    // Defaults to only one fiber.
    void start(int task_nr = 1);

    // Stop all fibers. Ongoing compactions will be waited.
    future<> stop();

    // Submit a column family to be compacted.
    void submit(column_family* cf);

    // Remove a column family from the compaction manager.
    // Cancel requests on cf and wait for a possible ongoing compaction on cf.
    future<> remove(column_family* cf);

    const stats& get_stats() const {
        return _stats;
    }
};

