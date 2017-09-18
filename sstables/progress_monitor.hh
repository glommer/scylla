/*
 * Copyright (C) 2017 ScyllaDB
 *
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


#include <seastar/core/shared_ptr.hh>
#include "shared_sstable.hh"

namespace sstables {
class write_monitor {
public:
    virtual ~write_monitor() { }
    virtual void on_write_completed() = 0;
    virtual void on_flush_completed() = 0;
};

struct noop_write_monitor final : public write_monitor {
    virtual void on_write_completed() override { }
    virtual void on_flush_completed() override { }
};

seastar::shared_ptr<write_monitor> default_write_monitor();

class read_monitor {
public:
    virtual ~read_monitor() { }
    // parameters are the current position in the data file
    virtual void on_read(uint64_t current_pos) = 0;
    virtual void on_fast_forward_to(uint64_t current_pos) = 0;
};

struct noop_read_monitor final: public read_monitor {
    virtual void on_read(uint64_t pos) override {}
    virtual void on_fast_forward_to(uint64_t pos) override {}
};

seastar::shared_ptr<read_monitor> default_read_monitor();

struct read_monitor_generator {
    virtual seastar::shared_ptr<sstables::read_monitor> operator()(sstables::shared_sstable sst) = 0;
    virtual ~read_monitor_generator() {}
};

struct no_read_monitoring final : public read_monitor_generator {
    virtual seastar::shared_ptr<sstables::read_monitor> operator()(sstables::shared_sstable sst) override {
        return sstables::default_read_monitor();
    };
};

seastar::shared_ptr<read_monitor_generator> default_read_monitor_generator();
}
