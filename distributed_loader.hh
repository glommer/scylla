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


#include <seastar/core/future.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/file.hh>
#include <vector>
#include <functional>
#include <filesystem>
#include "seastarx.hh"
#include "sstables/sstables.hh"

class database;
class table;
using column_family = table;
namespace db {
class system_distributed_keyspace;
namespace view {
class view_update_generator;
}
}

namespace sstables {

class entry_descriptor;
class foreign_sstable_open_info;

}

namespace service {

class storage_proxy;
class migration_manager;

}

// Handles a directory containing SSTables. It could be an auxiliary directory (like upload),
// or the main directory.
class directory_with_sstables_handler {
public:
    struct lack_of_toc_fatal_tag{ };
    using lack_of_toc_fatal = bool_class<lack_of_toc_fatal_tag>;

    struct need_mutate_level_tag{ };
    using need_mutate_level = bool_class<need_mutate_level_tag>;
private:
    distributed<database>& _db;
    sstring _ks_name;
    sstring _cf_name;

    const ::io_priority_class& _io_priority;

    std::filesystem::path _sstable_main_dir;

    bool _enable_dangerous_direct_import_of_cassandra_counters;
    need_mutate_level _need_mutate_level;
    lack_of_toc_fatal _throw_on_missing_toc;

    std::atomic<int64_t> _generation = { 1 };

    // prevents this table from disappearing in the middle of the operation.
    // We need to get a phaser per shard. The table will not be dropped while
    // this object is alive. They will be destroyed when the object is destroyed.
    std::vector<utils::phased_barrier::operation> _shard_phasers;

    // If we must reshard, we want to have each shard resharding more
    // or less the same amount of data. This auxiliary structure make
    // it easier. We will store this in a vector, one position per shard.
    // As we find an SSTable that needs resharding, we add it to the bucket
    // that has the least total amount of data so far.
    struct shard_bucket {
        size_t total_size = 0;
        std::vector<sstables::foreign_sstable_open_info> sstable_info;
        std::vector<sstables::shared_sstable> opened_sstables;
        const bool operator<(shard_bucket& b) const {
            return total_size < b.total_size;
        }
    };
    std::vector<shard_bucket> _shared_sstables;

    // SSTables that do not need to be resharded, merely need to be
    // moved to the right location or compacted with the others
    std::vector<shard_bucket> _unshared_sstables;

    using scan_multimap = std::unordered_multimap<int64_t, std::filesystem::path>;
    using scan_descriptors = std::vector<sstables::entry_descriptor>;

    scan_multimap _generations_found;

    scan_descriptors _temp_toc_found;
    scan_descriptors _descriptors;
    // SSTables with temporary toc and statistics.
    std::unordered_set<sstring> _files_for_removal;

    std::filesystem::path _aux_dir;

    shard_id _coordinator;

    table& local_table();

protected:
    // protected members are for test
    future<> open_sstables(std::vector<shard_bucket>& bucket);
    future<> collect_newly_unshared_sstables(std::vector<sstables::shared_sstable> new_sstables);
    void validate(sstables::shared_sstable sst);
    int64_t new_generation();
    std::filesystem::path dir();
    sstables::shared_sstable get_destination_unshared_sstable(shard_id shard);
    future<sstables::shared_sstable> get_local_sstable(sstables::foreign_sstable_open_info info);
    future<> handle_component(sstables::entry_descriptor desc, std::filesystem::path filename);
    future<> process_descriptor(sstables::entry_descriptor desc);

    static bool manifest_json_filter(const std::filesystem::path& path, const directory_entry& entry);
public:
    directory_with_sstables_handler(const directory_with_sstables_handler&) = delete;
    directory_with_sstables_handler(directory_with_sstables_handler&&) = delete;

    directory_with_sstables_handler(distributed<database>& db, sstring ks_name, sstring cf_name,
            std::filesystem::path sstable_main_dir,
            std::filesystem::path aux_dir = "",
            need_mutate_level need_mutate = need_mutate_level::no,
            lack_of_toc_fatal fatal_nontoc = lack_of_toc_fatal::yes);

    future<> lock_local_tables();

    future<> reshard();
    future<> process_sstable_dir();
    future<> make_unshared_sstables_available();

    size_t num_opened_shared() const;
    size_t num_unopened_shared() const;

    size_t num_opened_unshared() const;
    size_t num_unopened_unshared() const;

};

class distributed_loader {
public:
    static void reshard(distributed<database>& db, sstring ks_name, sstring cf_name);
    static future<> open_sstable(distributed<database>& db, sstables::entry_descriptor comps,
        std::function<future<> (column_family&, sstables::foreign_sstable_open_info)> func,
        const io_priority_class& pc = default_priority_class());
    static future<> verify_owner_and_mode(std::filesystem::path path);
    static future<> load_new_sstables(distributed<database>& db, distributed<db::view::view_update_generator>& view_update_generator,
            sstring ks, sstring cf, std::vector<sstables::entry_descriptor> new_tables);
    static future<std::vector<sstables::entry_descriptor>> flush_upload_dir(distributed<database>& db, distributed<db::system_distributed_keyspace>& sys_dist_ks, sstring ks_name, sstring cf_name);
    static future<> process_upload_dir(distributed<database>& db, sstring ks_name, sstring cf_name);
    static future<sstables::entry_descriptor> probe_file(distributed<database>& db, sstring sstdir, sstring fname);
    static future<> populate_column_family(distributed<database>& db, sstring sstdir, sstring ks, sstring cf);
    static future<> populate_keyspace(distributed<database>& db, sstring datadir, sstring ks_name);
    static future<> init_system_keyspace(distributed<database>& db);
    static future<> ensure_system_table_directories(distributed<database>& db);
    static future<> init_non_system_keyspaces(distributed<database>& db, distributed<service::storage_proxy>& proxy, distributed<service::migration_manager>& mm);
private:
    static future<> cleanup_column_family_temp_sst_dirs(sstring sstdir);
    static future<> handle_sstables_pending_delete(sstring pending_deletes_dir);
    static future<> do_populate_column_family(distributed<database>& db, sstring sstdir, sstring ks, sstring cf);
};
