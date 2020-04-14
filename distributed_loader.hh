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
#include "sstables/compaction.hh"
#include "sstables/compaction_manager.hh"
#include "utils/chunked_vector.hh"

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
class sstable_directory {
public:
    using lack_of_toc_fatal = bool_class<class lack_of_toc_fatal_tag>;
    using need_mutate_level = bool_class<class need_mutate_level_tag>;
    using enable_dangerous_direct_import_of_cassandra_counters = bool_class<class enable_dangerous_direct_import_of_cassandra_counters_tag>;
    using allow_loading_materialized_view = bool_class<class allow_loading_materialized_view_tag>;

    using sstable_object_from_existing_fn =
        noncopyable_function<sstables::shared_sstable(std::filesystem::path,
                                                      int64_t,
                                                      sstables::sstable_version_types,
                                                      sstables::sstable_format_types)>;
private:
    // favor chunked vectors when dealing with file lists: they can grow to hundreds of thousands
    // of elements.
    using scan_multimap = std::unordered_multimap<int64_t, std::filesystem::path>;
    using scan_descriptors = utils::chunked_vector<sstables::entry_descriptor>;
    using sstable_info_vector = utils::chunked_vector<sstables::foreign_sstable_open_info>;

    struct scan_state {
        scan_multimap generations_found;

        scan_descriptors temp_toc_found;
        scan_descriptors descriptors;
    };

    // SSTable files to be deleted: things with a Temporary TOC, missing TOC files,
    // TemporaryStatistics, etc. Not part of the scan state, because we want to do a 2-phase
    // delete: maybe one of the shards will have signaled an error. And in the case of an error
    // we don't want to delete anything.
    std::unordered_set<sstring> _files_for_removal;

    const ::io_priority_class& _io_priority;
    // prevents an object that respects a phaser (usually a table) from disappearing in the middle of the operation.
    // Will be destroyed when this object is destroyed.
    std::optional<utils::phased_barrier::operation> _operation_barrier;

    std::filesystem::path _sstable_dir;

    // We may have hundreds of thousands of files to load. To protect against OOMs we will limit
    // how many of them we process at the same time.
    semaphore _load_semaphore;

    // Flags below control how to behave when scanning new SSTables.
    need_mutate_level _need_mutate_level;
    lack_of_toc_fatal _throw_on_missing_toc;
    enable_dangerous_direct_import_of_cassandra_counters _enable_dangerous_direct_import_of_cassandra_counters;
    allow_loading_materialized_view _allow_loading_materialized_view;

    // How to create an SSTable object from an existing SSTable file (respecting generation, etc)
    sstable_object_from_existing_fn _sstable_object_from_existing_sstable;

    int64_t _max_generation_seen = 0;
    sstables::sstable::version_types _max_version_seen = sstables::sstable::version_types::ka;

    // SSTables that are unshared and belong to this shard. They are already stored as an
    // SSTable object.
    utils::chunked_vector<sstables::shared_sstable> _unshared_local_sstables;
    // SSTables that are unshared and belong to foreign shards. Because they are more conveniently
    // stored as a foreign_sstable_info object, they are in a different attribute separate from the
    // local SSTables.
    //
    // The indexes of the outer vector represent the shards. Having anything in the index
    // representing this shard is illegal.
    std::vector<sstable_info_vector> _unshared_remote_sstables;

    // SSTables that are shared. We also keep the total size to be able to be able to make
    // sure that all shards are resharding about the same amount of data.
    struct shared_descriptor {
        std::vector<sstables::foreign_sstable_open_info> info;
        size_t total_size = 0;
    };
    sstable_info_vector _shared_sstable_info;

    future<> process_descriptor(sstables::entry_descriptor desc);
    future<> commit_file_removals();
    void validate(sstables::shared_sstable sst) const;
    void handle_component(scan_state& state, sstables::entry_descriptor desc, std::filesystem::path filename);
    static bool manifest_json_filter(const std::filesystem::path& path, const directory_entry& entry);

    future<> move_foreign_sstables(sharded<sstable_directory>& source_directory);
    future<> remove_resharded_sstables(const std::vector<sstables::shared_sstable>& sstlist);
    future<> collect_resharded_sstables(std::vector<sstables::shared_sstable> resharded_sstables);
    int64_t highest_generation_seen() const;
    sstables::sstable::version_types highest_version_seen() const;
    future<> process_sstable_dir();
    future<> reshard(sstable_info_vector info, compaction_manager& cm, table& table, sstables::creator_fn creator);
    future<> do_for_each_sstable(std::function<future<>(sstables::shared_sstable)> func);
    void store_phaser(utils::phased_barrier::operation op);
public:
    sstable_directory(std::filesystem::path sstable_dir,
            unsigned load_parallelism,
            need_mutate_level need_mutate,
            lack_of_toc_fatal fatal_nontoc,
            enable_dangerous_direct_import_of_cassandra_counters eddiocc,
            allow_loading_materialized_view,
            sstable_object_from_existing_fn sstable_from_existing);

    // Helper functions to operate on the distributed version of this class

    // globally locks a table, preventing it from disappearing during an operation
    static future<> lock_table(sharded<sstable_directory>& dir, sharded<database>& db, sstring ks_name, sstring cf_name);

    // Process the SSTable directory and fills in information about shared and unshared SSTables.
    // When this function returns each instance of the sharded object will have its own unshared
    // and potentially some shared SSTables.
    static future<> process_sstable_dir(sharded<sstable_directory>& dir);

    // Reshards the entire directory, making sure that the load is well spread around the various
    // shards. The table name and keyspace name are arguments to this function, together with a
    // creator function that will be invoked when the resharding process needs to create a new
    // SSTable. The creator function needs to make sure that the generation of the new SSTable will
    // conform to its destination shard.
    static future<> reshard(sharded<sstable_directory>& dir, sharded<database>& db, sstring ks_name, sstring table_name, sstables::creator_fn creator);

    // Returns the highest generation seen. SSTables created after this function is called should
    // produce generations that are higher than this.
    static future<int64_t> highest_generation_seen(sharded<sstable_directory>& dir);

    // Returns the highest generation seen while scanning this directory across all instances.
    // This is especially important when scanning system tables: we may not know what is the highest
    // version we support, but want to make sure at least don't want to downgrade SSTables.
    static future<sstables::sstable::version_types> highest_version_seen(sharded<sstable_directory>& dir, sstables::sstable::version_types sysver);

    // Helper function that executes a function for each unshared sstable, respecting the parallelism limit that we are
    // configured to uphold
    static future<> do_for_each_sstable(sharded<sstable_directory>& sstdir, std::function<future<>(sstables::shared_sstable)> func);
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
    /**
     * Marks a keyspace (by name) as "prioritized" on bootstrap.
     * This will effectively let it bypass concurrency control.
     * The only real use for this is to avoid certain chicken and
     * egg issues.
     *
     * May only be called pre-bootstrap on main shard.
     */
    static void mark_keyspace_as_load_prio(const sstring&);
private:
    static future<> cleanup_column_family_temp_sst_dirs(sstring sstdir);
    static future<> handle_sstables_pending_delete(sstring pending_deletes_dir);
    static future<> do_populate_column_family(distributed<database>& db, sstring sstdir, sstring ks, sstring cf);
};
