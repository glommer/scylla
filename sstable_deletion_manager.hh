/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>

class sstable_deletion_manager {
    class impl;
    class real_impl;
    class dummy_impl;
    std::unique_ptr<impl> _impl;
private:
    sstable_deletion_manager(std::unique_ptr<impl> impl);
public:
    sstable_deletion_manager(sstable_deletion_manager&&) noexcept;
    ~sstable_deletion_manager();
    future<> start();
    future<> stop();
    void may_delete(sstring sstable_toc_name, bool is_shared);

    friend sstable_deletion_manager make_sstable_deletion_manager();
    friend sstable_deletion_manager make_dummy_sstable_deletion_manager();
};

sstable_deletion_manager make_sstable_deletion_manager();
sstable_deletion_manager make_dummy_sstable_deletion_manager();
