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

#include "distributed_loader.hh"
#include "database.hh"
#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/schema_tables.hh"
#include "lister.hh"
#include "sstables/compaction.hh"
#include "sstables/compaction_manager.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/remove.hh"
#include "service/priority_manager.hh"
#include "auth/common.hh"
#include "tracing/trace_keyspace_helper.hh"
#include "db/view/view_update_checks.hh"
#include <unordered_map>
#include <boost/range/adaptor/map.hpp>
#include "db/view/view_update_generator.hh"

extern logging::logger dblog;

static future<> execute_futures(std::vector<future<>>& futures);

static const std::unordered_set<sstring> system_keyspaces = {
                db::system_keyspace::NAME, db::schema_tables::NAME
};

// Not super nice. Adding statefulness to the file. 
static std::unordered_set<sstring> load_prio_keyspaces;
static bool population_started = false;

void distributed_loader::mark_keyspace_as_load_prio(const sstring& ks) {
    assert(!population_started);
    load_prio_keyspaces.insert(ks);
}

bool is_system_keyspace(const sstring& name) {
    return system_keyspaces.find(name) != system_keyspaces.end();
}

static const std::unordered_set<sstring> internal_keyspaces = {
        db::system_distributed_keyspace::NAME,
        db::system_keyspace::NAME,
        db::schema_tables::NAME,
        auth::meta::AUTH_KS,
        tracing::trace_keyspace_helper::KEYSPACE_NAME
};

bool is_internal_keyspace(const sstring& name) {
    return internal_keyspaces.find(name) != internal_keyspaces.end();
}

static io_error_handler error_handler_for_upload_dir() {
    return [] (std::exception_ptr eptr) {
        // do nothing about sstable exception and caller will just rethrow it.
    };
}

// TODO: possibly move it to seastar
template <typename Service, typename PtrType, typename Func>
static future<> invoke_shards_with_ptr(std::unordered_set<shard_id> shards, distributed<Service>& s, PtrType ptr, Func&& func) {
    return parallel_for_each(std::move(shards), [&s, &func, ptr] (shard_id id) {
        return s.invoke_on(id, [func, foreign = make_foreign(ptr)] (Service& s) mutable {
            return func(s, std::move(foreign));
        });
    });
}

// global_column_family_ptr provides a way to easily retrieve local instance of a given column family.
class global_column_family_ptr {
    distributed<database>& _db;
    utils::UUID _id;
private:
    column_family& get() const { return _db.local().find_column_family(_id); }
public:
    global_column_family_ptr(distributed<database>& db, sstring ks_name, sstring cf_name)
        : _db(db)
        , _id(_db.local().find_column_family(ks_name, cf_name).schema()->id()) {
    }

    column_family* operator->() const {
        return &get();
    }
    column_family& operator*() const {
        return get();
    }
};

template <typename... Args>
static inline
future<> verification_error(fs::path path, const char* fstr, Args&&... args) {
    auto emsg = fmt::format(fstr, std::forward<Args>(args)...);
    dblog.error("{}: {}", path.string(), emsg);
    return make_exception_future<>(std::runtime_error(emsg));
}

// Verify that all files and directories are owned by current uid
// and that files can be read and directories can be read, written, and looked up (execute)
// No other file types may exist.
future<> distributed_loader::verify_owner_and_mode(fs::path path) {
    return file_stat(path.string(), follow_symlink::no).then([path = std::move(path)] (stat_data sd) {
        // Under docker, we run with euid 0 and there is no reasonable way to enforce that the
        // in-container uid will have the same uid as files mounted from outside the container. So
        // just allow euid 0 as a special case. It should survive the file_accessible() checks below.
        // See #4823.
        if (geteuid() != 0 && sd.uid != geteuid()) {
            return verification_error(std::move(path), "File not owned by current euid: {}. Owner is: {}", geteuid(), sd.uid);
        }
        switch (sd.type) {
        case directory_entry_type::regular: {
            auto f = file_accessible(path.string(), access_flags::read);
            return f.then([path = std::move(path)] (bool can_access) {
                if (!can_access) {
                    return verification_error(std::move(path), "File cannot be accessed for read");
                }
                return make_ready_future<>();
            });
            break;
        }
        case directory_entry_type::directory: {
            auto f = file_accessible(path.string(), access_flags::read | access_flags::write | access_flags::execute);
            return f.then([path = std::move(path)] (bool can_access) {
                if (!can_access) {
                    return verification_error(std::move(path), "Directory cannot be accessed for read, write, and execute");
                }
                return lister::scan_dir(path, {}, [] (fs::path dir, directory_entry de) {
                    return verify_owner_and_mode(dir / de.name);
                });
            });
            break;
        }
        default:
            return verification_error(std::move(path), "Must be either a regular file or a directory (type={})", static_cast<int>(sd.type));
        }
    });
};

sstable_directory::sstable_directory(fs::path sstable_dir,
        unsigned load_parallelism,
        need_mutate_level need_mutate_level,
        lack_of_toc_fatal throw_on_missing_toc,
        enable_dangerous_direct_import_of_cassandra_counters eddiocc,
        allow_loading_materialized_view allow_mv,
        sstable_object_from_existing_fn sstable_from_existing)
    : _io_priority(service::get_local_streaming_read_priority())
    , _sstable_dir(std::move(sstable_dir))
    , _load_semaphore(load_parallelism)
    , _need_mutate_level(need_mutate_level)
    , _throw_on_missing_toc(throw_on_missing_toc)
    , _enable_dangerous_direct_import_of_cassandra_counters(eddiocc)
    , _allow_loading_materialized_view(allow_mv)
    , _sstable_object_from_existing_sstable(std::move(sstable_from_existing))
    , _unshared_remote_sstables(smp::count)
    , _sstable_reshard_list(1)
{}

bool sstable_directory::manifest_json_filter(const fs::path& path, const directory_entry& entry) {
    return table::manifest_json_filter(path, entry);
}

void
sstable_directory::handle_component(sstables::entry_descriptor desc, fs::path filename) {
    // If not owned by us, skip
    if ((desc.generation % smp::count) != this_shard_id()) {
        return;
    }

    if (desc.component != component_type::TemporaryStatistics) {
        // Will delete right away so don't store here. We could just ignore the
        // ENOENT error, but better to not even issue the operation.
        _generations_found.emplace(desc.generation, filename);
    }

    switch (desc.component) {
    case component_type::TemporaryStatistics:
        _files_for_removal.insert(filename.native());
        break;
    case component_type::TOC:
        _descriptors.push_back(std::move(desc));
        break;
    case component_type::TemporaryTOC:
        _temp_toc_found.push_back(std::move(desc));
        break;
    default:
        // Do nothing, and will validate when trying to load the file.
        break;
    }
}

void sstable_directory::validate(sstables::shared_sstable sst) const {
    schema_ptr s = sst->get_schema();
    if (s->is_counter() && !sst->has_scylla_component()) {
        sstring error = "Direct loading non-Scylla SSTables containing counters is not supported.";
        if (_enable_dangerous_direct_import_of_cassandra_counters == sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::yes) {
            dblog.info("{} But trying to continue on user's request.", error);
        } else {
            dblog.error("{} Use sstableloader instead.", error);
            throw std::runtime_error(fmt::format("{} Use sstableloader instead.", error));
        }
    }
    if (s->is_view() && (_allow_loading_materialized_view == allow_loading_materialized_view::no)) {
        throw std::runtime_error("Loading Materialized View SSTables is not supported. Re-create the view instead.");
    }
}

future<>
sstable_directory::process_descriptor(sstables::entry_descriptor desc) {
    if (sstables::is_later(desc.version, _latest_version_seen)) {
        _latest_version_seen = desc.version;
    }

    auto sst = _sstable_object_from_existing_sstable(_sstable_dir, desc.generation, desc.version, desc.format);
    return sst->load(_io_priority).then([this, sst] {
        validate(sst);
        if (_need_mutate_level == sstable_directory::need_mutate_level::yes) {
            return sst->mutate_sstable_level(0);
        } else {
            return make_ready_future<>();
        }
    }).then([sst, this] {
        return sst->get_open_info().then([sst, this] (sstables::foreign_sstable_open_info info) {
            auto shards = sst->get_shards_for_this_sstable();
            if (shards.size() == 1) {
                if (shards[0] == this_shard_id()) {
                    _unshared_local_sstables.push_back(sst);
                } else {
                    _unshared_remote_sstables[shards[0]].push_back(std::move(info));
                }
            } else {
                _shared_sstable_info.push_back(std::move(info));
            }
            return make_ready_future<>();
        });
    });
}

int64_t
sstable_directory::highest_generation_seen() const {
    return _max_generation_seen;
}

sstables::sstable::version_types
sstable_directory::highest_version_seen() const {
    return _max_version_seen;
}

future<>
sstable_directory::process_sstable_dir() {
    return lister::scan_dir(_sstable_dir, { directory_entry_type::regular },
            [this] (fs::path parent_dir, directory_entry de) {
        // Dir here may not be the same as the path we are scanning. For example we may
        // be scanning data/ks/table/upload, but for the descriptor we still want to use
        // data/ks/table.
        auto comps = sstables::entry_descriptor::make_descriptor(_sstable_dir.native(), de.name);
        handle_component(std::move(comps), parent_dir / fs::path(de.name));
        return make_ready_future<>();
    }, &manifest_json_filter).then([this] {
        // Always okay to delete files with a temporary TOC.
        for (auto& desc: _temp_toc_found) {
            auto range = _generations_found.equal_range(desc.generation);
            for (auto it = range.first; it != range.second; ++it) {
                auto& path = it->second;
                dblog.info("Scheduling to remove file {}, from an SSTable with a Temporary TOC", path.native());
                _files_for_removal.insert(path.native());
            }
            _generations_found.erase(range.first, range.second);
        }

        if (_generations_found.size()) {
            _max_generation_seen =  *(boost::max_element(_generations_found | boost::adaptors::map_keys));
        }

        dblog.debug("{} After {} scanned, seen generation {}. {} descriptors found, {} different files found ",
                _sstable_dir, _max_generation_seen, _descriptors.size(), _generations_found.size());


        // _descriptors is everything with a TOC. So after we remove this, what's left is
        // SSTables for which a TOC was not found.
        return parallel_for_each(_descriptors, [this] (sstables::entry_descriptor desc) {
            return with_semaphore(_load_semaphore, 1, [this, desc = std::move(desc)] () mutable {
                _generations_found.erase(desc.generation);
                // This will try to pre-load this file and throw an exception if it is invalid
                return process_descriptor(std::move(desc));
            });
        }).then([this] {
            // For files missing TOC, it depends on where this is coming from.
            // If scylla was supposed to have generated this SSTable, this is not okay and
            // we refuse to proceed. If this coming from, say, an import, then we just delete
            // log and proceed.
            for (auto& path : _generations_found | boost::adaptors::map_values) {
                if (_throw_on_missing_toc == lack_of_toc_fatal::yes) {
                    throw sstables::malformed_sstable_exception(format("At directory: {}: no TOC found for SSTable {}!. Refusing to boot", _sstable_dir.native(), path.native()));
                } else {
                    dblog.info("Found incomplete SSTable {} at directory {}. Removing", path.native(), _sstable_dir.native());
                    _files_for_removal.insert(path.native());
                }
            }

            // Remove all files scheduled for removal
            return parallel_for_each(_files_for_removal, [] (sstring path) {
                return remove_file(std::move(path));
            });
        });
    });
}

future<>
sstable_directory::move_foreign_sstables(sharded<sstable_directory>& source_directory) {
    return parallel_for_each(boost::irange(0u, smp::count), [this, &source_directory] (unsigned shard_id) mutable {
        auto info_vec = std::exchange(_unshared_remote_sstables[shard_id], {});
        if (info_vec.empty()) {
            return make_ready_future<>();
        }
        // Should be empty, since an SSTable that belongs to this shard is not remote.
        assert(shard_id != this_shard_id());
        dblog.debug("{} Moving {} unshared SSTables to shard {} ", info_vec.size(), shard_id);
        return source_directory.invoke_on(shard_id, [info_vec = std::move(info_vec)] (sstable_directory& remote) mutable {
            return parallel_for_each(info_vec, [&remote] (sstables::foreign_sstable_open_info& info) {
                return with_semaphore(remote._load_semaphore, 1, [info = std::move(info), &remote] () mutable {
                    auto sst = remote._sstable_object_from_existing_sstable(remote._sstable_dir, info.generation, info.version, info.format);
                    return sst->load(std::move(info)).then([sst, &remote] {
                        remote._unshared_local_sstables.push_back(sst);
                        return make_ready_future<>();
                    });
                });
            });
        });
    });
}

future<>
sstable_directory::remove_resharded_sstables(const std::vector<sstables::shared_sstable>& sstlist) {
    return parallel_for_each(sstlist, [] (sstables::shared_sstable sst) {
        return sst->unlink();
    });
}

future<>
sstable_directory::collect_resharded_sstables(std::vector<sstables::shared_sstable> resharded_sstables) {
    return parallel_for_each(std::move(resharded_sstables), [this] (sstables::shared_sstable sst) {
        auto shards = sst->get_shards_for_this_sstable();
        assert(shards.size() == 1);
        auto shard = shards[0];

        if (shard == this_shard_id()) {
            _unshared_local_sstables.push_back(std::move(sst));
            return make_ready_future<>();
        }
        return sst->get_open_info().then([this, shard, sst] (sstables::foreign_sstable_open_info info) {
            _unshared_remote_sstables[shard].push_back(std::move(info));
            return make_ready_future<>();
        });
    });
}

future<>
sstable_directory::reshard(compaction_manager& cm, table& table, sstables::creator_fn creator) {
    dblog.debug("{} Considering reshard on a batch of {} SSTables", _shared_sstable_info.size());
    // Resharding doesn't like empty sstable sets, so bail early. There is nothing
    // to reshard in this shard.
    if (_shared_sstable_info.size() == 0) {
        return make_ready_future<>();
    }

    // We want to reshard many SSTables at a time for efficiency. However if we have to many we may
    // be risking OOM.
    auto num_jobs = _shared_sstable_info.size() / table.schema()->max_compaction_threshold() + 1;
    auto sstables_per_job = _shared_sstable_info.size() / num_jobs;

    return parallel_for_each(_shared_sstable_info, [this, sstables_per_job] (sstables::foreign_sstable_open_info& info) {
        auto sst = _sstable_object_from_existing_sstable(_sstable_dir, info.generation, info.version, info.format);
        return sst->load(std::move(info)).then([this, sstables_per_job, sst = std::move(sst)] () mutable {
            if (_sstable_reshard_list.back().size() >= sstables_per_job) {
                _sstable_reshard_list.emplace_back();
            }
            _sstable_reshard_list.back().push_back(std::move(sst));
        });
    }).then([this, &cm, &table, creator = std::move(creator)] () mutable {
        // There is a semaphore inside the compaction manager in run_resharding_jobs. So we
        // parallel_for_each so the statistics about pending jobs are updated to reflect all
        // jobs. But only one will run in parallel at a time
        return parallel_for_each(_sstable_reshard_list, [this, &cm, &table, creator = std::move(creator)] (std::vector<sstables::shared_sstable>& sstlist) mutable {
            return cm.run_resharding_job(&table, [this, &cm, &table, creator, &sstlist] () {
                sstables::compaction_descriptor desc(_sstable_reshard_list[0]);
                desc.options = sstables::compaction_options::make_reshard();
                desc.io_priority = _io_priority;
                desc.creator = std::move(creator);

                return sstables::compact_sstables(std::move(desc), table).then([this, &sstlist] (sstables::compaction_info result) {
                    return when_all_succeed(collect_resharded_sstables(std::move(result.new_sstables)), remove_resharded_sstables(sstlist));
                });
            });
        });
    });
}

future<>
sstable_directory::do_for_each_sstable(std::function<future<>(sstables::shared_sstable)> func) {
    return parallel_for_each(_unshared_local_sstables, [this, func = std::move(func)] (sstables::shared_sstable sst) mutable {
        return with_semaphore(_load_semaphore, 1, [this, func,  sst = std::move(sst)] () mutable {
            return func(sst);
        });
    });
}

void
sstable_directory::store_phaser(utils::phased_barrier::operation op) {
    _operation_barrier.emplace(std::move(op));
}

future<>
sstable_directory::process_sstable_dir(sharded<sstable_directory>& dir) {
    return dir.invoke_on_all([&dir] (sstable_directory& d) {
        return d.process_sstable_dir().then([&dir, &d] {
            return d.move_foreign_sstables(dir);
        });
    });
}

future<int64_t>
sstable_directory::highest_generation_seen(sharded<sstable_directory>& dir) {
    // Because all shards scan directories, they should all see the same generation
    // However this is an implementation detail that may change in the future.
    //
    // For example, different shards may scan different directories, or filter
    // earlier and not see some files
    return dir.map_reduce0([] (sstable_directory& dir) {
        return dir.highest_generation_seen();
    }, int64_t(0), [] (int64_t a, int64_t b) { return std::max(a, b); });
}

future<sstables::sstable::version_types>
sstable_directory::highest_version_seen(sharded<sstable_directory>& dir, sstables::sstable::version_types system_version) {
    using version = sstables::sstable::version_types;
    return dir.map_reduce0([] (sstable_directory& dir) {
        return dir.highest_version_seen();
    }, system_version, [] (version a, version b) {
        return sstables::is_later(a, b) ? a : b;
    });
}

future<>
sstable_directory::do_for_each_sstable(sharded<sstable_directory>& dir, std::function<future<>(sstables::shared_sstable)> func) {
    return dir.invoke_on_all([func] (sstable_directory& d) {
        return d.do_for_each_sstable(func);
    });
}

future<>
sstable_directory::reshard(sharded<sstable_directory>& dir, sharded<database>& db, sstring ks_name, sstring table_name, sstables::creator_fn creator) {
    return dir.invoke_on_all([&dir, &db, ks_name, table_name, creator] (sstable_directory& d) {
        auto& table = db.local().find_column_family(ks_name, table_name);
        return d.reshard(table.get_compaction_manager(), table, creator).then([&d, &dir] {
            return d.move_foreign_sstables(dir);
        });
    });
}

future<>
distributed_loader::process_upload_dir(distributed<database>& db, sstring ks, sstring cf) {
    return seastar::async([&db, ks = std::move(ks), cf = std::move(cf)] {
        auto eddiocc = [&db] {
            if (db.local().get_config().enable_dangerous_direct_import_of_cassandra_counters()) {
                return sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::yes;
            } else {
                return sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no;
            }
        };

        global_column_family_ptr global_table(db, ks, cf);

        sharded<sstable_directory> directory;
        auto upload = fs::path(global_table->dir()) / "upload";
        directory.start(upload, 4,
            sstable_directory::need_mutate_level::yes,
            sstable_directory::lack_of_toc_fatal::no,
            eddiocc(),
            sstable_directory::allow_loading_materialized_view::no,
            [&global_table] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                return global_table->make_sstable(dir.native(), gen, v, f);

        }).get();
        auto stop = defer([&directory] {
            directory.stop().get();
        });

        directory.invoke_on_all([&global_table] (sstable_directory& dir) {
            dir.store_phaser(global_table->write_in_progress());
            return make_ready_future<>();
        }).get();

        sstable_directory::process_sstable_dir(directory).get();
        sstable_directory::reshard(directory, db, ks, cf, [&global_table, upload] (shard_id shard) {
            // we need generation calculated by instance of cf at requested shard
            auto gen = smp::submit_to(shard, [&global_table] () {
                return global_table->calculate_generation_for_new_table();
            }).get0();

            return global_table->make_sstable(upload.native(), gen,
                    global_table->get_sstables_manager().get_highest_supported_format(),
                    sstables::sstable::format_types::big);
        }).get();
    });
}

// This function will iterate through upload directory in column family,
// and will do the following for each sstable found:
// 1) Mutate sstable level to 0.
// 2) Check if view updates need to be generated from this sstable. If so, leave it intact for now.
// 3) Otherwise, create hard links to its components in column family dir.
// 4) Remove all of its components in upload directory.
// At the end, it's expected that upload dir contains only staging sstables
// which need to wait until view updates are generated from them.
//
// Return a vector containing descriptor of sstables to be loaded.
future<std::vector<sstables::entry_descriptor>>
distributed_loader::flush_upload_dir(distributed<database>& db, distributed<db::system_distributed_keyspace>& sys_dist_ks, sstring ks_name, sstring cf_name) {
    return seastar::async([&db, &sys_dist_ks, ks_name = std::move(ks_name), cf_name = std::move(cf_name)] {
        std::unordered_map<int64_t, sstables::entry_descriptor> descriptors;
        std::vector<sstables::entry_descriptor> flushed;

        auto& cf = db.local().find_column_family(ks_name, cf_name);
        auto upload_dir = fs::path(cf._config.datadir) / "upload";
        verify_owner_and_mode(upload_dir).get();
        lister::scan_dir(upload_dir, { directory_entry_type::regular }, [&descriptors] (fs::path parent_dir, directory_entry de) {
              auto comps = sstables::entry_descriptor::make_descriptor(parent_dir.native(), de.name);
              if (comps.component != component_type::TOC) {
                  return make_ready_future<>();
              }
              descriptors.emplace(comps.generation, std::move(comps));
              return make_ready_future<>();
        }, &column_family::manifest_json_filter).get();

        flushed.reserve(descriptors.size());
        for (auto& [generation, comps] : descriptors) {
            auto descriptors = db.invoke_on(column_family::calculate_shard_from_sstable_generation(generation), [&sys_dist_ks, ks_name, cf_name, comps] (database& db) {
                return seastar::async([&db, &sys_dist_ks, ks_name = std::move(ks_name), cf_name = std::move(cf_name), comps = std::move(comps)] () mutable {
                    auto& cf = db.find_column_family(ks_name, cf_name);
                    auto sst = cf.make_sstable(cf._config.datadir + "/upload", comps.generation, comps.version, comps.format,
                        [] (disk_error_signal_type&) { return error_handler_for_upload_dir(); });
                    auto gen = cf.calculate_generation_for_new_table();

                    sst->read_toc().get();
                    schema_ptr s = cf.schema();
                    if (s->is_counter() && !sst->has_scylla_component()) {
                        sstring error = "Direct loading non-Scylla SSTables containing counters is not supported.";
                        if (db.get_config().enable_dangerous_direct_import_of_cassandra_counters()) {
                            dblog.info("{} But trying to continue on user's request.", error);
                        } else {
                            dblog.error("{} Use sstableloader instead.", error);
                            throw std::runtime_error(fmt::format("{} Use sstableloader instead.", error));
                        }
                    }
                    if (s->is_view()) {
                        throw std::runtime_error("Loading Materialized View SSTables is not supported. Re-create the view instead.");
                    }
                    sst->mutate_sstable_level(0).get();
                    const bool use_view_update_path = db::view::check_needs_view_update_path(sys_dist_ks.local(), cf, streaming::stream_reason::repair).get0();
                    sstring datadir = cf._config.datadir;
                    if (use_view_update_path) {
                        // Move to staging directory to avoid clashes with future uploads. Unique generation number ensures no collisions.
                        datadir += "/staging";
                    }
                    sst->create_links(datadir, gen).get();
                    sstables::remove_by_toc_name(sst->toc_filename(), error_handler_for_upload_dir()).get();
                    comps.generation = gen;
                    comps.sstdir = std::move(datadir);
                    return std::move(comps);
                });
            }).get0();

            flushed.push_back(std::move(descriptors));
        }
        return std::vector<sstables::entry_descriptor>(std::move(flushed));
    });
}

future<> distributed_loader::open_sstable(distributed<database>& db, sstables::entry_descriptor comps,
        std::function<future<> (column_family&, sstables::foreign_sstable_open_info)> func, const io_priority_class& pc) {
    // loads components of a sstable from shard S and share it with all other
    // shards. Which shard a sstable will be opened at is decided using
    // calculate_shard_from_sstable_generation(), which is the inverse of
    // calculate_generation_for_new_table(). That ensures every sstable is
    // shard-local if reshard wasn't performed. This approach is also expected
    // to distribute evenly the resource usage among all shards.
    static thread_local std::unordered_map<sstring, named_semaphore> named_semaphores;    

    return db.invoke_on(column_family::calculate_shard_from_sstable_generation(comps.generation),
            [&db, comps = std::move(comps), func = std::move(func), &pc] (database& local) {

        // check if we should bypass concurrency throttle for this keyspace
        // we still only allow a single sstable per shard extra to be loaded, 
        // to avoid concurrency explosion
        auto& sem = load_prio_keyspaces.count(comps.ks)
            ? named_semaphores.try_emplace(comps.ks, 1, named_semaphore_exception_factory{comps.ks}).first->second
            : local.sstable_load_concurrency_sem()
            ;

        return with_semaphore(sem, 1, [&db, &local, comps = std::move(comps), func = std::move(func), &pc] {
            auto& cf = local.find_column_family(comps.ks, comps.cf);
            auto sst = cf.make_sstable(comps.sstdir, comps.generation, comps.version, comps.format);
            auto f = sst->load(pc).then([sst = std::move(sst)] {
                return sst->load_shared_components();
            });
            return f.then([&db, comps = std::move(comps), func = std::move(func)] (sstables::sstable_open_info info) {
                // shared components loaded, now opening sstable in all shards that own it with shared components
                return do_with(std::move(info), [&db, comps = std::move(comps), func = std::move(func)] (auto& info) {
                    // All shards that own the sstable is interested in it in addition to shard that
                    // is responsible for its generation. We may need to add manually this shard
                    // because sstable may not contain data that belong to it.
                    auto shards_interested_in_this_sstable = boost::copy_range<std::unordered_set<shard_id>>(info.owners);
                    shard_id shard_responsible_for_generation = column_family::calculate_shard_from_sstable_generation(comps.generation);
                    shards_interested_in_this_sstable.insert(shard_responsible_for_generation);

                    return invoke_shards_with_ptr(std::move(shards_interested_in_this_sstable), db, std::move(info.components),
                            [owners = info.owners, data = info.data.dup(), index = info.index.dup(), comps, func] (database& db, auto components) {
                        auto& cf = db.find_column_family(comps.ks, comps.cf);
                        return func(cf, sstables::foreign_sstable_open_info{std::move(components), owners, data, index});
                    });
                });
            });
        });
    });
}

future<> distributed_loader::load_new_sstables(distributed<database>& db, distributed<db::view::view_update_generator>& view_update_generator,
        sstring ks, sstring cf, std::vector<sstables::entry_descriptor> new_tables) {
    return parallel_for_each(new_tables, [&] (auto comps) {
        auto cf_sstable_open = [comps] (column_family& cf, sstables::foreign_sstable_open_info info) {
            auto f = cf.open_sstable(std::move(info), comps.sstdir, comps.generation, comps.version, comps.format);
            return f.then([&cf] (sstables::shared_sstable sst) mutable {
                if (sst) {
                    cf._sstables_opened_but_not_loaded.push_back(sst);
                }
                return make_ready_future<>();
            });
        };
        return distributed_loader::open_sstable(db, comps, cf_sstable_open, service::get_local_compaction_priority())
            .handle_exception([comps, ks, cf] (std::exception_ptr ep) {
                auto name = sstables::sstable::filename(comps.sstdir, ks, cf, comps.version, comps.generation, comps.format, sstables::component_type::TOC);
                dblog.error("Failed to open {}: {}", name, ep);
                return make_exception_future<>(ep);
            });
    }).then([&db, &view_update_generator, ks, cf] {
        return db.invoke_on_all([&view_update_generator, ks = std::move(ks), cfname = std::move(cf)] (database& db) {
            auto& cf = db.find_column_family(ks, cfname);
            return cf.get_row_cache().invalidate([&view_update_generator, &cf] () noexcept {
                // FIXME: this is not really noexcept, but we need to provide strong exception guarantees.
                // atomically load all opened sstables into column family.
                for (auto& sst : cf._sstables_opened_but_not_loaded) {
                    try {
                        cf.load_sstable(sst, true);
                    } catch(...) {
                        dblog.error("Failed to load {}: {}. Aborting.", sst->toc_filename(), std::current_exception());
                        abort();
                    }
                    if (sst->requires_view_building()) {
                        // FIXME: discarded future.
                        (void)view_update_generator.local().register_staging_sstable(sst, cf.shared_from_this());
                    }
                }
                cf._sstables_opened_but_not_loaded.clear();
                cf.trigger_compaction();
            });
        });
    }).handle_exception([&db, ks, cf] (std::exception_ptr ep) {
        return db.invoke_on_all([ks = std::move(ks), cfname = std::move(cf)] (database& db) {
            auto& cf = db.find_column_family(ks, cfname);
            cf._sstables_opened_but_not_loaded.clear();
        }).then([ep] {
            return make_exception_future<>(ep);
        });
    });
}

future<sstables::entry_descriptor> distributed_loader::probe_file(distributed<database>& db, sstring sstdir, sstring fname) {
    using namespace sstables;

    entry_descriptor comps = entry_descriptor::make_descriptor(sstdir, fname);

    // Every table will have a TOC. Using a specific file as a criteria, as
    // opposed to, say verifying _sstables.count() to be zero is more robust
    // against parallel loading of the directory contents.
    if (comps.component != component_type::TOC) {
        return make_ready_future<entry_descriptor>(std::move(comps));
    }
    auto cf_sstable_open = [sstdir, comps, fname] (column_family& cf, sstables::foreign_sstable_open_info info) {
        cf.update_sstables_known_generation(comps.generation);
        if (shared_sstable sst = cf.get_staging_sstable(comps.generation)) {
            dblog.warn("SSTable {} is already present in staging/ directory. Moving from staging will be retried.", sst->get_filename());
            return sst->move_to_new_dir(comps.sstdir, comps.generation);
        }
        {
            auto i = boost::range::find_if(*cf._sstables->all(), [gen = comps.generation] (sstables::shared_sstable sst) { return sst->generation() == gen; });
            if (i != cf._sstables->all()->end()) {
                auto new_toc = sstdir + "/" + fname;
                throw std::runtime_error(format("Attempted to add sstable generation {:d} twice: new={} existing={}",
                                                comps.generation, new_toc, (*i)->toc_filename()));
            }
        }
        return cf.open_sstable(std::move(info), sstdir, comps.generation, comps.version, comps.format).then([&cf] (sstables::shared_sstable sst) mutable {
            if (sst) {
                return cf.get_row_cache().invalidate([&cf, sst = std::move(sst)] () mutable noexcept {
                    // FIXME: this is not really noexcept, but we need to provide strong exception guarantees.
                    cf.load_sstable(sst);
                });
            }
            return make_ready_future<>();
        });
    };

    return distributed_loader::open_sstable(db, comps, cf_sstable_open).then_wrapped([fname] (future<> f) {
        try {
            f.get();
        } catch (malformed_sstable_exception& e) {
            dblog.error("malformed sstable {}: {}. Refusing to boot", fname, e.what());
            throw;
        } catch(...) {
            dblog.error("Unrecognized error while processing {}: {}. Refusing to boot",
                fname, std::current_exception());
            throw;
        }
        return make_ready_future<>();
    }).then([comps] () mutable {
        return make_ready_future<entry_descriptor>(std::move(comps));
    });
}

static future<> execute_futures(std::vector<future<>>& futures) {
    return seastar::when_all(futures.begin(), futures.end()).then([] (std::vector<future<>> ret) {
        std::exception_ptr eptr;

        for (auto& f : ret) {
            try {
                if (eptr) {
                    f.ignore_ready_future();
                } else {
                    f.get();
                }
            } catch(...) {
                eptr = std::current_exception();
            }
        }

        if (eptr) {
            return make_exception_future<>(eptr);
        }
        return make_ready_future<>();
    });
}

future<> distributed_loader::cleanup_column_family_temp_sst_dirs(sstring sstdir) {
    return do_with(std::vector<future<>>(), [sstdir = std::move(sstdir)] (std::vector<future<>>& futures) {
        return lister::scan_dir(sstdir, { directory_entry_type::directory }, [&futures] (fs::path sstdir, directory_entry de) {
            // push futures that remove files/directories into an array of futures,
            // so that the supplied callback will not block scan_dir() from
            // reading the next entry in the directory.
            fs::path dirpath = sstdir / de.name;
            if (sstables::sstable::is_temp_dir(dirpath)) {
                dblog.info("Found temporary sstable directory: {}, removing", dirpath);
                futures.push_back(io_check([dirpath = std::move(dirpath)] () { return lister::rmdir(dirpath); }));
            }
            return make_ready_future<>();
        }).then([&futures] {
            return execute_futures(futures);
        });
    });
}

future<> distributed_loader::handle_sstables_pending_delete(sstring pending_delete_dir) {
    return do_with(std::vector<future<>>(), [dir = std::move(pending_delete_dir)] (std::vector<future<>>& futures) {
        return lister::scan_dir(dir, { directory_entry_type::regular }, [&futures] (fs::path dir, directory_entry de) {
            // push nested futures that remove files/directories into an array of futures,
            // so that the supplied callback will not block scan_dir() from
            // reading the next entry in the directory.
            fs::path file_path = dir / de.name;
            if (file_path.extension() == ".tmp") {
                dblog.info("Found temporary pending_delete log file: {}, deleting", file_path);
                futures.push_back(remove_file(file_path.string()));
            } else if (file_path.extension() == ".log") {
                dblog.info("Found pending_delete log file: {}, replaying", file_path);
                auto f = sstables::replay_pending_delete_log(file_path.string()).then([file_path = std::move(file_path)] {
                    dblog.debug("Replayed {}, removing", file_path);
                    return remove_file(file_path.string());
                });
                futures.push_back(std::move(f));
            } else {
                dblog.debug("Found unknown file in pending_delete directory: {}, ignoring", file_path);
            }
            return make_ready_future<>();
        }).then([&futures] {
            return execute_futures(futures);
        });
    });
}

future<> distributed_loader::do_populate_column_family(distributed<database>& db, sstring sstdir, sstring ks, sstring cf) {
    // We can catch most errors when we try to load an sstable. But if the TOC
    // file is the one missing, we won't try to load the sstable at all. This
    // case is still an invalid case, but it is way easier for us to treat it
    // by waiting for all files to be loaded, and then checking if we saw a
    // file during scan_dir, without its corresponding TOC.
    enum class component_status {
        has_some_file,
        has_toc_file,
        has_temporary_toc_file,
    };

    struct sstable_descriptor {
        component_status status;
        sstables::sstable::version_types version;
        sstables::sstable::format_types format;
    };

    auto verifier = make_lw_shared<std::unordered_map<unsigned long, sstable_descriptor>>();

    return do_with(std::vector<future<>>(), [&db, sstdir = std::move(sstdir), verifier, ks, cf] (std::vector<future<>>& futures) {
        return lister::scan_dir(sstdir, { directory_entry_type::regular }, [&db, verifier, &futures] (fs::path sstdir, directory_entry de) {
            // FIXME: The secondary indexes are in this level, but with a directory type, (starting with ".")

            // push future returned by probe_file into an array of futures,
            // so that the supplied callback will not block scan_dir() from
            // reading the next entry in the directory.
            auto f = distributed_loader::probe_file(db, sstdir.native(), de.name).then([verifier, sstdir, de] (auto entry) {
                if (entry.component == component_type::TemporaryStatistics) {
                    return remove_file(sstables::sstable::filename(sstdir.native(), entry.ks, entry.cf, entry.version, entry.generation,
                        entry.format, component_type::TemporaryStatistics));
                }

                if (verifier->count(entry.generation)) {
                    if (verifier->at(entry.generation).status == component_status::has_toc_file) {
                        fs::path file_path(sstdir / de.name);
                        if (entry.component == component_type::TOC) {
                            throw sstables::malformed_sstable_exception("Invalid State encountered. TOC file already processed", file_path.native());
                        } else if (entry.component == component_type::TemporaryTOC) {
                            throw sstables::malformed_sstable_exception("Invalid State encountered. Temporary TOC file found after TOC file was processed", file_path.native());
                        }
                    } else if (entry.component == component_type::TOC) {
                        verifier->at(entry.generation).status = component_status::has_toc_file;
                    } else if (entry.component == component_type::TemporaryTOC) {
                        verifier->at(entry.generation).status = component_status::has_temporary_toc_file;
                    }
                } else {
                    if (entry.component == component_type::TOC) {
                        verifier->emplace(entry.generation, sstable_descriptor{component_status::has_toc_file, entry.version, entry.format});
                    } else if (entry.component == component_type::TemporaryTOC) {
                        verifier->emplace(entry.generation, sstable_descriptor{component_status::has_temporary_toc_file, entry.version, entry.format});
                    } else {
                        verifier->emplace(entry.generation, sstable_descriptor{component_status::has_some_file, entry.version, entry.format});
                    }
                }
                return make_ready_future<>();
            });

            futures.push_back(std::move(f));

            return make_ready_future<>();
        }, &column_family::manifest_json_filter).then([&futures] {
            return execute_futures(futures);
        }).then([verifier, sstdir, ks = std::move(ks), cf = std::move(cf)] {
            return do_for_each(*verifier, [sstdir = std::move(sstdir), ks = std::move(ks), cf = std::move(cf), verifier] (auto v) {
                if (v.second.status == component_status::has_temporary_toc_file) {
                    unsigned long gen = v.first;
                    sstables::sstable::version_types version = v.second.version;
                    sstables::sstable::format_types format = v.second.format;

                    if (this_shard_id() != 0) {
                        dblog.debug("At directory: {}, partial SSTable with generation {} not relevant for this shard, ignoring", sstdir, v.first);
                        return make_ready_future<>();
                    }
                    // shard 0 is the responsible for removing a partial sstable.
                    return sstables::sstable::remove_sstable_with_temp_toc(ks, cf, sstdir, gen, version, format);
                } else if (v.second.status != component_status::has_toc_file) {
                    throw sstables::malformed_sstable_exception(format("At directory: {}: no TOC found for SSTable with generation {:d}!. Refusing to boot", sstdir, v.first));
                }
                return make_ready_future<>();
            });
        });
    });

}

future<> distributed_loader::populate_column_family(distributed<database>& db, sstring sstdir, sstring ks, sstring cf) {
    return async([&db, sstdir = std::move(sstdir), ks = std::move(ks), cf = std::move(cf)] {
        assert(this_shard_id() == 0);
        // First pass, cleanup temporary sstable directories and sstables pending delete.
        cleanup_column_family_temp_sst_dirs(sstdir).get();
        auto pending_delete_dir = sstdir + "/" + sstables::sstable::pending_delete_dir_basename();
        auto exists = file_exists(pending_delete_dir).get0();
        if (exists) {
            handle_sstables_pending_delete(pending_delete_dir).get();
        }

        auto eddiocc = [&db] {
            if (db.local().get_config().enable_dangerous_direct_import_of_cassandra_counters()) {
                return sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::yes;
            } else {
                return sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no;
            }
        };

        global_column_family_ptr global_table(db, ks, cf);

        sharded<sstable_directory> directory;
        directory.start(fs::path(sstdir), 10,
            sstable_directory::need_mutate_level::no,
            sstable_directory::lack_of_toc_fatal::yes,
            eddiocc(),
            sstable_directory::allow_loading_materialized_view::yes,
            [&global_table] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                return global_table->make_sstable(dir.native(), gen, v, f);

        }).get();
        auto stop = defer([&directory] {
            directory.stop().get();
        });

        // Prevent the table from disappearing. This is early boot, but it can still
        // be that a drop will hit another node and get propagated here. We also disable
        // auto compaction in this table so the resharded tables don't start getting
        // compacted until it is time to do so.
        directory.invoke_on_all([&global_table] (sstable_directory& dir) {
            dir.store_phaser(global_table->write_in_progress());
            global_table->disable_auto_compaction();
            return make_ready_future<>();
        }).get();

        sstable_directory::process_sstable_dir(directory).get();
        // If we are resharding system tables before we can read them, we will not
        // know which is the highest format we support: this information is itself stored
        // in the system tables. In that case we'll rely on what we find on disk: we'll
        // at least not downgrade any files. If we already know that we support a higher
        // format than the one we see then we use that.
        auto sys_format = global_table->get_sstables_manager().get_highest_supported_format();
        auto format = sstable_directory::highest_version_seen(directory, sys_format).get0();
        auto generation = sstable_directory::highest_generation_seen(directory).get0();;

        db.invoke_on_all([&global_table, generation] (database& db) {
            global_table->update_sstables_known_generation(generation);
            return make_ready_future<>();
        }).get();

        sstable_directory::reshard(directory, db, ks, cf, [&global_table, sstdir, format] (shard_id shard) {
            // we need generation calculated by instance of cf at requested shard
            auto gen = smp::submit_to(shard, [&global_table] () {
                return global_table->calculate_generation_for_new_table();
            }).get0();

            return global_table->make_sstable(sstdir, gen, format, sstables::sstable::format_types::big);
        }).get();

        sstable_directory::do_for_each_sstable(directory, [&global_table] (sstables::shared_sstable sst) {
            return global_table->add_sstable_and_update_cache(sst);
        }).get();
    });
}

future<> distributed_loader::populate_keyspace(distributed<database>& db, sstring datadir, sstring ks_name) {
    auto ksdir = datadir + "/" + ks_name;
    auto& keyspaces = db.local().get_keyspaces();
    auto i = keyspaces.find(ks_name);
    if (i == keyspaces.end()) {
        dblog.warn("Skipping undefined keyspace: {}", ks_name);
        return make_ready_future<>();
    } else {
        dblog.info("Populating Keyspace {}", ks_name);
        auto& ks = i->second;
        auto& column_families = db.local().get_column_families();

        return parallel_for_each(ks.metadata()->cf_meta_data() | boost::adaptors::map_values,
            [ks_name, ksdir, &ks, &column_families, &db] (schema_ptr s) {
                utils::UUID uuid = s->id();
                lw_shared_ptr<column_family> cf = column_families[uuid];
                sstring cfname = cf->schema()->cf_name();
                auto sstdir = ks.column_family_directory(ksdir, cfname, uuid);
                dblog.info("Keyspace {}: Reading CF {} id={} version={}", ks_name, cfname, uuid, s->version());
                return ks.make_directory_for_column_family(cfname, uuid).then([&db, sstdir, uuid, ks_name, cfname] {
                    return distributed_loader::populate_column_family(db, sstdir + "/staging", ks_name, cfname);
                }).then([&db, sstdir, uuid, ks_name, cfname] {
                    return distributed_loader::populate_column_family(db, sstdir, ks_name, cfname);
                }).handle_exception([ks_name, cfname, sstdir](std::exception_ptr eptr) {
                    std::string msg =
                        format("Exception while populating keyspace '{}' with column family '{}' from file '{}': {}",
                               ks_name, cfname, sstdir, eptr);
                    dblog.error("Exception while populating keyspace '{}' with column family '{}' from file '{}': {}",
                                ks_name, cfname, sstdir, eptr);
                    throw std::runtime_error(msg.c_str());
                });
            });
    }
}

future<> distributed_loader::init_system_keyspace(distributed<database>& db) {
    population_started = true;

    return seastar::async([&db] {
        // We need to init commitlog on shard0 before it is inited on other shards
        // because it obtains the list of pre-existing segments for replay, which must
        // not include reserve segments created by active commitlogs.
        db.invoke_on(0, [] (database& db) {
            return db.init_commitlog();
        }).get();
        db.invoke_on_all([] (database& db) {
            if (this_shard_id() == 0) {
                return make_ready_future<>();
            }
            return db.init_commitlog();
        }).get();

        db.invoke_on_all([] (database& db) {
            auto& cfg = db.get_config();
            bool durable = cfg.data_file_directories().size() > 0;
            db::system_keyspace::make(db, durable, cfg.volatile_system_keyspace_for_testing());
        }).get();

        const auto& cfg = db.local().get_config();
        for (auto& data_dir : cfg.data_file_directories()) {
            for (auto ksname : system_keyspaces) {
                io_check([name = data_dir + "/" + ksname] { return touch_directory(name); }).get();
                distributed_loader::populate_keyspace(db, data_dir, ksname).get();
            }
        }

        db.invoke_on_all([] (database& db) {
            for (auto ksname : system_keyspaces) {
                auto& ks = db.find_keyspace(ksname);
                for (auto& pair : ks.metadata()->cf_meta_data()) {
                    auto cfm = pair.second;
                    auto& cf = db.find_column_family(cfm);
                    cf.mark_ready_for_writes();
                }
                // for system keyspaces, we only do this post all population, and
                // only as a consistency measure.
                // change this if it is ever needed to sync system keyspace
                // population
                ks.mark_as_populated();
            }
            return make_ready_future<>();
        }).get();
    });
}

future<> distributed_loader::ensure_system_table_directories(distributed<database>& db) {
    return parallel_for_each(system_keyspaces, [&db](sstring ksname) {
        auto& ks = db.local().find_keyspace(ksname);
        return parallel_for_each(ks.metadata()->cf_meta_data(), [&ks] (auto& pair) {
            auto cfm = pair.second;
            return ks.make_directory_for_column_family(cfm->cf_name(), cfm->id());
        });
    });
}

future<> distributed_loader::init_non_system_keyspaces(distributed<database>& db,
        distributed<service::storage_proxy>& proxy, distributed<service::migration_manager>& mm) {
    return seastar::async([&db, &proxy, &mm] {
        db.invoke_on_all([&proxy, &mm] (database& db) {
            return db.parse_system_tables(proxy, mm);
        }).get();

        const auto& cfg = db.local().get_config();
        using ks_dirs = std::unordered_multimap<sstring, sstring>;

        ks_dirs dirs;

        parallel_for_each(cfg.data_file_directories(), [&db, &dirs] (sstring directory) {
            // we want to collect the directories first, so we can get a full set of potential dirs
            return lister::scan_dir(directory, { directory_entry_type::directory }, [&dirs] (fs::path datadir, directory_entry de) {
                if (!is_system_keyspace(de.name)) {
                    dirs.emplace(de.name, datadir.native());
                }
                return make_ready_future<>();
            });
        }).get();

        db.invoke_on_all([&dirs] (database& db) {
            for (auto& [name, ks] : db.get_keyspaces()) {
                // mark all user keyspaces that are _not_ on disk as already
                // populated.
                if (!dirs.count(ks.metadata()->name())) {
                    ks.mark_as_populated();
                }
            }
        }).get();

        std::vector<future<>> futures;

        // treat "dirs" as immutable to avoid modifying it while still in 
        // a range-iteration. Also to simplify the "finally"
        for (auto i = dirs.begin(); i != dirs.end();) {
            auto& ks_name = i->first;
            auto e = dirs.equal_range(ks_name).second;
            auto j = i++;
            // might have more than one dir for a keyspace iff data_file_directories is > 1 and
            // somehow someone placed sstables in more than one of them for a given ks. (import?) 
            futures.emplace_back(parallel_for_each(j, e, [&](const std::pair<sstring, sstring>& p) {
                auto& datadir = p.second;
                return distributed_loader::populate_keyspace(db, datadir, ks_name);
            }).finally([&] {
                return db.invoke_on_all([ks_name] (database& db) {
                    // can be false if running test environment
                    // or ks_name was just a borked directory not representing
                    // a keyspace in schema tables.
                    if (db.has_keyspace(ks_name)) {
                        db.find_keyspace(ks_name).mark_as_populated();
                    }
                    return make_ready_future<>();
                });
            }));
        }

        execute_futures(futures).get();

        db.invoke_on_all([] (database& db) {
            return parallel_for_each(db.get_non_system_column_families(), [] (lw_shared_ptr<table> table) {
                // Make sure this is called even if the table is empty
                table->mark_ready_for_writes();
                return make_ready_future<>();
            });
        }).get();
    });
}

