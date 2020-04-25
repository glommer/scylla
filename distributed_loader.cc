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

void directory_with_sstables_handler::validate(sstables::shared_sstable sst) {
    assert(_coordinator == this_shard_id());

    schema_ptr s = local_table().schema();
    if (s->is_counter() && !sst->has_scylla_component()) {
        sstring error = "Direct loading non-Scylla SSTables containing counters is not supported.";
        if (_enable_dangerous_direct_import_of_cassandra_counters) {
            dblog.info("{} But trying to continue on user's request.", error);
        } else {
            dblog.error("{} Use sstableloader instead.", error);
            throw std::runtime_error(fmt::format("{} Use sstableloader instead.", error));
        }
    }
    if (s->is_view()) {
        throw std::runtime_error("Loading Materialized View SSTables is not supported. Re-create the view instead.");
    }
}

int64_t directory_with_sstables_handler::new_generation() {
    return _generation.fetch_add(1, std::memory_order_relaxed);
}

fs::path directory_with_sstables_handler::dir() {
    return fs::canonical(_sstable_main_dir / _aux_dir);
}

table&
directory_with_sstables_handler::local_table() {
    return _db.local().find_column_family(_ks_name, _cf_name);
}

sstables::shared_sstable
directory_with_sstables_handler::get_destination_unshared_sstable(shard_id shard) {
    auto sst = local_table().make_sstable(dir().native(), new_generation(),
        local_table().get_sstables_manager().get_highest_supported_format(),
        sstables::sstable::format_types::big);
    sst->set_unshared();
    return sst;
};

future<sstables::shared_sstable>
directory_with_sstables_handler::get_local_sstable(sstables::foreign_sstable_open_info info) {
    auto sst = local_table().make_sstable(dir().native(), info.generation, info.version, info.format,
            [] (disk_error_signal_type&) { return error_handler_for_upload_dir(); });
    return sst->load(std::move(info)).then([sst] {
        return make_ready_future<sstables::shared_sstable>(sst);
    });
}

future<>
directory_with_sstables_handler::process_descriptor(sstables::entry_descriptor desc) {
    assert(_coordinator == this_shard_id());

    auto sstdir = fs::path(desc.sstdir) / _aux_dir;

    auto sst = local_table().make_sstable(sstdir.native(), desc.generation, desc.version, desc.format, [] (disk_error_signal_type&)
            { return error_handler_for_upload_dir();
    });

    return sst->load(_io_priority).then([this, sst] {
        validate(sst);
        if (_need_mutate_level == need_mutate_level::yes) {
            return sst->mutate_sstable_level(0);
        } else {
            return make_ready_future<>();
        }
    }).then([sst, this] {
        return sst->get_open_info().then([sst, this] (sstables::foreign_sstable_open_info info) {
            auto shards = sst->get_shards_for_this_sstable();
            if (shards.size() == 1) {
                _unshared_sstables[shards[0]].sstable_info.push_back(std::move(info));
            } else {
                // Make sure that every shard has about the same amount of data to reshard.
                auto shard_it = boost::min_element(_shared_sstables);
                shard_it->total_size += sst->data_size();
                shard_it->sstable_info.push_back(std::move(info));
            }
            return make_ready_future<>();
        });
    });
}

bool directory_with_sstables_handler::manifest_json_filter(const fs::path& path, const directory_entry& entry) {
    return table::manifest_json_filter(path, entry);
}

future<>
directory_with_sstables_handler::handle_component(sstables::entry_descriptor desc, fs::path filename) {
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
    return make_ready_future<>();
}

future<>
directory_with_sstables_handler::process_sstable_dir() {
    assert(_coordinator == this_shard_id());

    return distributed_loader::verify_owner_and_mode(dir()).then([this] {
        return lister::scan_dir(dir(), { directory_entry_type::regular },
                [this] (fs::path parent_dir, directory_entry de) {

            // Dir here may not be the same as the path we are scanning. For example we may
            // be scanning data/ks/table/upload, but for the descriptor we still want to use
            // data/ks/table.
            auto filename = fs::canonical(_sstable_main_dir / fs::path(de.name));
            auto comps = sstables::entry_descriptor::make_descriptor(_sstable_main_dir.native(), de.name);
            return handle_component(std::move(comps), filename);
        }, &manifest_json_filter);
    }).then([this] {
        if (_generations_found.size()) {
            auto max_it = boost::max_element(_generations_found | boost::adaptors::map_keys);
            _generation.store(*max_it + 1, std::memory_order_relaxed);
        }

        // Always okay to delete files with a temporary TOC.
        for (auto& desc: _temp_toc_found) {
            auto range = _generations_found.equal_range(desc.generation);
            for (auto it = range.first; it != range.second; ++it) {
                auto& path = it->second;
                dblog.info("Scheduling to remove file {}, from an SSTable with a Temporary TOC", path.native());
                _files_for_removal.insert(path.native());
            }
            _generations_found.erase(desc.generation);
        }

        // _descriptors is everything with a TOC. So after we remove this, what's left is
        // SSTables for which a TOC was not found.
        return parallel_for_each(_descriptors, [this] (sstables::entry_descriptor desc) {
            _generations_found.erase(desc.generation);
            // This will try to pre-load this file and throw an exception if it is invalid
            return process_descriptor(desc);
        }).then([this] {
            // For files missing TOC, it depends on where this is coming from.
            // If scylla was supposed to have generated this SSTable, this is not okay and
            // we refuse to proceed. If this coming from, say, an import, then we just delete
            // log and proceed.
            for (auto& path : _generations_found | boost::adaptors::map_values) {
                if (_throw_on_missing_toc == lack_of_toc_fatal::yes) {
                  throw sstables::malformed_sstable_exception(format("At directory: {}: no TOC found for SSTable {}!. Refusing to boot", dir().native(), path.native()));
                } else {
                    dblog.info("Found incomplete SSTable {} in auxiliary dir. Removing", path.native());
                    _files_for_removal.insert(path.native());
                }
            }

            // Remove all files scheduled for removal
            return parallel_for_each(_files_for_removal, [] (sstring path) {
                return remove_file(path);
            });
        });
    });
}

directory_with_sstables_handler::directory_with_sstables_handler(distributed<database>& db, sstring ks_name, sstring cf_name,
        fs::path sstable_main_dir, fs::path aux_dir,
        need_mutate_level need_mutate_level,
        lack_of_toc_fatal throw_on_missing_toc)
    : _db(db)
    , _ks_name(ks_name)
    , _cf_name(cf_name)
    , _io_priority(service::get_local_streaming_read_priority())
    , _sstable_main_dir(std::move(sstable_main_dir))
    , _enable_dangerous_direct_import_of_cassandra_counters(db.local().get_config().enable_dangerous_direct_import_of_cassandra_counters())
    , _need_mutate_level(need_mutate_level)
    , _throw_on_missing_toc(throw_on_missing_toc)
    , _aux_dir(std::move(aux_dir))
    , _coordinator(this_shard_id())
{
    _shared_sstables.resize(smp::count);
    _unshared_sstables.resize(smp::count);
    _shard_phasers.resize(smp::count);
}

future<>
directory_with_sstables_handler::lock_local_tables() {
    return _db.invoke_on_all([this] (database& db) {
        auto& table = db.find_column_family(_ks_name, _cf_name);
        _shard_phasers[this_shard_id()] = table.write_in_progress();
    });
}

future<>
directory_with_sstables_handler::open_sstables(std::vector<shard_bucket>& bucket) {
    return parallel_for_each(bucket[this_shard_id()].sstable_info,
            [this, &bucket] (sstables::foreign_sstable_open_info& info) {
        return get_local_sstable(std::move(info)).then([this, &bucket] (sstables::shared_sstable sst) {
            bucket[this_shard_id()].opened_sstables.push_back(sst);
            return make_ready_future<>();
        });
    });
}

future<>
directory_with_sstables_handler::collect_newly_unshared_sstables(std::vector<sstables::shared_sstable> new_sstables) {
    // Collect all info structures first to save on smp round trips
    return do_with(std::vector<sstables::foreign_sstable_open_info>(),
            [this, new_sstables = std::move(new_sstables)] (std::vector<sstables::foreign_sstable_open_info>& new_info) {
        return parallel_for_each(new_sstables, [this, &new_info] (sstables::shared_sstable sst) {
            auto shards = sst->get_shards_for_this_sstable();
            return sst->get_open_info().then([this, &new_info] (sstables::foreign_sstable_open_info info) {
                assert(info.owners.size() == 1);
                new_info.push_back(std::move(info));
            }).finally([sst] {}); // keep sstable alive until we are doing with get_open_info()
        }).then([this, &new_info] {
            return smp::submit_to(_coordinator, [this, &new_info] {
                for (auto& info : new_info) {
                    auto shard = info.owners[0];
                    _unshared_sstables[shard].sstable_info.push_back(std::move(info));
                }
                return make_ready_future<>();
            });
        });
    });
}

future<>
directory_with_sstables_handler::reshard() {
    // Step 1: open all shared SSTables that are assigned to this shard.
    return open_sstables(_shared_sstables).then([this] {
    // Step 2: Reshard shared SSTables together
        return local_table().get_compaction_manager().run_resharding_job(&local_table(), [this] {
            // Resharding doesn't like empty sstable sets, so bail early. There is nothing
            // to reshard in this shard.
            auto sst_list = std::move(_shared_sstables[this_shard_id()].opened_sstables);
            if (sst_list.size() == 0) {
                return make_ready_future<>();
            }

            sstables::compaction_descriptor desc(sst_list, 0, std::numeric_limits<uint64_t>::max());
            desc.options = sstables::compaction_options::make_reshard();
            desc.io_priority = _io_priority;
            desc.creator = [this] (shard_id shard) mutable {
                return get_destination_unshared_sstable(shard);
            };
            return sstables::compact_sstables(std::move(desc), local_table()).then(
                    [this, sst_list = std::move(sst_list)] (sstables::compaction_info result) mutable {
                // Only if it succeeds.
                return parallel_for_each(sst_list, [] (sstables::shared_sstable sst) {
                    return sst->unlink();
                }).then([this, new_sstables = std::move(result.new_sstables)] {
                    return collect_newly_unshared_sstables(std::move(new_sstables));
                });
            });
        });
    }).finally([this] {
        // clear memory in the shard we created
        std::exchange(_shared_sstables[this_shard_id()].opened_sstables, std::vector<sstables::shared_sstable>());
        return make_ready_future<>();
    });
}

future<>
directory_with_sstables_handler::make_unshared_sstables_available() {
    return open_sstables(_unshared_sstables).then([this] {
        auto& table = local_table();
        table.update_sstables_known_generation(_generation.load(std::memory_order_relaxed));
        return parallel_for_each(_unshared_sstables[this_shard_id()].opened_sstables, [&table, this] (sstables::shared_sstable sst) {
            return table.add_sstable_and_update_cache(sst);
        });
    });
}

size_t
directory_with_sstables_handler::num_opened_unshared() const {
    size_t sum = 0;
    for (auto& shard : _unshared_sstables) {
        sum += shard.opened_sstables.size();
    }
    return sum;
}

size_t
directory_with_sstables_handler::num_opened_shared() const {
    size_t sum = 0;
    for (auto& shard : _shared_sstables) {
        sum += shard.opened_sstables.size();
    }
    return sum;
}

size_t
directory_with_sstables_handler::num_unopened_unshared() const {
    size_t sum = 0;
    for (auto& shard : _unshared_sstables) {
        sum += shard.sstable_info.size();
    }
    return sum;
}

size_t
directory_with_sstables_handler::num_unopened_shared() const {
    size_t sum = 0;
    for (auto& shard : _shared_sstables) {
        sum += shard.sstable_info.size();
    }
    return sum;
}

future<>
distributed_loader::process_upload_dir(distributed<database>& db, sstring ks_name, sstring cf_name) {
    return seastar::async([&db, ks_name = std::move(ks_name), cf_name = std::move(cf_name)] {
        auto& cf = db.local().find_column_family(ks_name, cf_name);
        directory_with_sstables_handler sstable_dir_handler(db, ks_name, cf_name, fs::path(cf._config.datadir), "upload",
                directory_with_sstables_handler::need_mutate_level::yes,
                directory_with_sstables_handler::lack_of_toc_fatal::no);
        sstable_dir_handler.process_sstable_dir().get();

        db.invoke_on_all([&sstable_dir_handler] (database& db) mutable {
            return sstable_dir_handler.reshard();
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

    return db.invoke_on(column_family::calculate_shard_from_sstable_generation(comps.generation),
            [&db, comps = std::move(comps), func = std::move(func), &pc] (database& local) {

        return with_semaphore(local.sstable_load_concurrency_sem(), 1, [&db, &local, comps = std::move(comps), func = std::move(func), &pc] {
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

        directory_with_sstables_handler sstable_dir_handler(db, ks, cf, fs::path(sstdir), "");
        sstable_dir_handler.process_sstable_dir().get();

        // Compaction manager is on at this time so we can submit resharding jobs.
        // We need to make sure that we don't compact the resulting SSTables from
        // resharding, so disable auto compaction. Do this in a separate pass to be sure.
        // Otherwise resharding in shardX can generate an SSTable in shardY before we
        // managed to disable it there.
        //
        // We will not enable them back when we are done because we don't want compactions
        // in this table affecting reshards in tables we will load later.
        db.invoke_on_all([ks, cf] (database& db) mutable {
            auto& table = db.find_column_family(ks, cf);
            table.disable_auto_compaction();
        }).get();

        db.invoke_on_all([&sstable_dir_handler] (database& dummy) mutable {
            return sstable_dir_handler.reshard();
        }).get();

        db.invoke_on_all([&sstable_dir_handler] (database& db) mutable {
            return sstable_dir_handler.make_unshared_sstables_available();
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

