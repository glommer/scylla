/*
 * Copyright (C) 2020 ScyllaDB
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


#include <seastar/testing/test_case.hh>
#include <seastar/core/sstring.hh>
#include "test/lib/sstable_utils.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/tmpdir.hh"
#include "distributed_loader.hh"
#include "db/config.hh"

#include "fmt/format.h"

/// Create a shard-local SSTable for the following schema: "create table cf (p text PRIMARY KEY, c int)"
///
/// Arguments passed to the function are passed to table::make_sstable
template <typename... Args>
future<sstables::shared_sstable>
make_sstable_for_this_shard(database& db, table& table, Args&&... args) {
    auto s = table.schema();
    auto mt = make_lw_shared<memtable>(s);
    auto msb = db.get_config().murmur3_partitioner_ignore_msb_bits();
    auto key_token_pair = token_generation_for_shard(1, this_shard_id(), msb);
    auto key = partition_key::from_exploded(*s, {to_bytes(key_token_pair[0].first)});
    mutation m(s, key);
    m.set_clustered_cell(clustering_key::make_empty(), bytes("c"), data_value(int32_t(0)), api::timestamp_type(0));
    mt->apply(std::move(m));
    auto sst = table.make_sstable(args...);
    return write_memtable_to_sstable(*mt, sst, table.get_sstables_manager().configure_writer()).then([mt] {
        return mt->clear_gently();
    }).then([sst] {
        return make_ready_future<sstables::shared_sstable>(sst);
    });
}

/// Create a shared SSTable belonging to all shards for the following schema: "create table cf (p text PRIMARY KEY, c int)"
///
/// Arguments passed to the function are passed to table::make_sstable
template <typename... Args>
future<sstables::shared_sstable>
make_sstable_for_all_shards(database& db, table& table, fs::path sstdir, int64_t generation, Args&&... args) {
    // Unlike the previous helper, we'll assume we're in a thread here. It's less flexible
    // but the users are usually in a thread, and rewrite_toc_without_scylla_component requires
    // a thread. We could fix that, but deferring that for now.
    auto s = table.schema();
    auto mt = make_lw_shared<memtable>(s);
    auto msb = db.get_config().murmur3_partitioner_ignore_msb_bits();
    for (shard_id shard = 0; shard < smp::count; ++shard) {
        auto key_token_pair = token_generation_for_shard(1, shard, msb);
        auto key = partition_key::from_exploded(*s, {to_bytes(key_token_pair[0].first)});
        mutation m(s, key);
        m.set_clustered_cell(clustering_key::make_empty(), bytes("c"), data_value(int32_t(0)), api::timestamp_type(0));
        mt->apply(std::move(m));
    }
    auto sst = table.make_sstable(sstdir.native(), generation++, args...);
    write_memtable_to_sstable(*mt, sst, table.get_sstables_manager().configure_writer()).get();
    mt->clear_gently().get();
    // We can't write an SSTable with bad sharding, so pretend
    // it came from Cassandra
    sstables::test(sst).remove_component(sstables::component_type::Scylla).get();
    sstables::test(sst).rewrite_toc_without_scylla_component();
    return make_ready_future<sstables::shared_sstable>(sst);
}

SEASTAR_TEST_CASE(sstable_directory_test_table_simple_empty_directory_scan) {
    return test_setup::do_with_tmp_directory([] (test_env& env, sstring tmpdir_path) {
        auto tmpdir = fs::path(tmpdir_path);

        // Write a manifest file to make sure it's ignored
        auto manifest = tmpdir / "manifest.json";
        auto f = open_file_dma(manifest.native(), open_flags::wo | open_flags::create | open_flags::truncate).get0();
        f.close().get();

        sharded<sstable_directory> sstdir;
        sstdir.start(tmpdir, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::no,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&env] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    return nullptr;
                }).get();

        auto stop = defer([&sstdir] {
            sstdir.stop().get();
        });

        sstable_directory::process_sstable_dir(sstdir).get();
        int64_t max_generation_seen = sstable_directory::highest_generation_seen(sstdir).get0();
        // No generation found on empty directory.
        BOOST_REQUIRE_EQUAL(max_generation_seen, 0);
        return make_ready_future<>();
    });
}

// Test unrecoverable SSTable: missing a file that is expected in the TOC.
SEASTAR_TEST_CASE(sstable_directory_test_table_scan_incomplete_sstables) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto& cf = e.local_db().find_column_family("ks", "cf");
        auto upload_path = fs::path(cf.dir()) / "upload";

        auto sst = make_sstable_for_this_shard(e.local_db(), cf, upload_path.native()).get0();
        // Now there is one sstable to the upload directory, but it is incomplete and one component is missing.
        // We should fail validation and leave the directory untouched
        remove_file(sst->filename(sstables::component_type::Statistics)).get();

        sharded<sstable_directory> sstdir;
        sstdir.start(upload_path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::no,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    auto& cf = e.local_db().find_column_family("ks", "cf");
                    return cf.make_sstable(dir.native(), gen, v, f);
                }).get();

        auto stop = defer([&sstdir] {
            sstdir.stop().get();
        });

        auto expect_malformed_sstable = sstable_directory::process_sstable_dir(sstdir);
        BOOST_REQUIRE_THROW(expect_malformed_sstable.get(), sstables::malformed_sstable_exception);
    });
}

// Test always-benign incomplete SSTable: temporaryTOC found
SEASTAR_TEST_CASE(sstable_directory_test_table_temporary_toc) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto& cf = e.local_db().find_column_family("ks", "cf");
        auto upload_path = fs::path(cf.dir()) / "upload";

        auto sst = make_sstable_for_this_shard(e.local_db(), cf, upload_path.native()).get0();
        rename_file(sst->filename(sstables::component_type::TOC), sst->filename(sstables::component_type::TemporaryTOC)).get();

        sharded<sstable_directory> sstdir;
        sstdir.start(upload_path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::yes,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    auto& cf = e.local_db().find_column_family("ks", "cf");
                    return cf.make_sstable(dir.native(), gen, v, f);
                }).get();

        auto stop = defer([&sstdir] {
            sstdir.stop().get();
        });

        auto expect_ok = sstable_directory::process_sstable_dir(sstdir);
        BOOST_REQUIRE_NO_THROW(expect_ok.get());
    });
}

// Test the absence of TOC. Behavior is controllable by a flag
SEASTAR_TEST_CASE(sstable_directory_test_table_missing_toc) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto& cf = e.local_db().find_column_family("ks", "cf");
        auto upload_path = fs::path(cf.dir()) / "upload";

        auto sst = make_sstable_for_this_shard(e.local_db(), cf, upload_path.native()).get0();
        remove_file(sst->filename(sstables::component_type::TOC)).get();

        sharded<sstable_directory> sstdir_fatal;
        sstdir_fatal.start(upload_path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::yes,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    auto& cf = e.local_db().find_column_family("ks", "cf");
                    return cf.make_sstable(dir.native(), gen, v, f);
                }).get();

        auto stop_fatal = defer([&sstdir_fatal] {
            sstdir_fatal.stop().get();
        });

        auto expect_malformed_sstable  = sstable_directory::process_sstable_dir(sstdir_fatal);
        BOOST_REQUIRE_THROW(expect_malformed_sstable.get(), sstables::malformed_sstable_exception);

        sharded<sstable_directory> sstdir_ok;
        sstdir_ok.start(upload_path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::no,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    auto& cf = e.local_db().find_column_family("ks", "cf");
                    return cf.make_sstable(dir.native(), gen, v, f);
                }).get();

        auto stop_ok = defer([&sstdir_ok] {
            sstdir_ok.stop().get();
        });

        auto expect_ok = sstable_directory::process_sstable_dir(sstdir_ok);
        BOOST_REQUIRE_NO_THROW(expect_ok.get());
    });
}

// Test the presence of TemporaryStatistics. If the old Statistics file is around
// this is benign and we'll just delete it and move on. If the old Statistics file
// is not around (but mentioned in the TOC), then this is an error.
SEASTAR_TEST_CASE(sstable_directory_test_temporary_statistics) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto& cf = e.local_db().find_column_family("ks", "cf");
        auto upload_path = fs::path(cf.dir()) / "upload";

        auto sst = make_sstable_for_this_shard(e.local_db(), cf, upload_path.native()).get0();
        auto tempstr = sst->filename(upload_path.native(), component_type::TemporaryStatistics);
        auto f = open_file_dma(tempstr, open_flags::rw | open_flags::create | open_flags::truncate).get0();
        f.close().get();
        auto tempstat = fs::canonical(fs::path(tempstr));

        sharded<sstable_directory> sstdir_ok;
        sstdir_ok.start(upload_path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::no,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    auto& cf = e.local_db().find_column_family("ks", "cf");
                    return cf.make_sstable(dir.native(), gen, v, f);
                }).get();

        auto stop_ok= defer([&sstdir_ok] {
            sstdir_ok.stop().get();
        });

        auto expect_ok = sstable_directory::process_sstable_dir(sstdir_ok);
        BOOST_REQUIRE_NO_THROW(expect_ok.get());
        lister::scan_dir(upload_path, { directory_entry_type::regular }, [tempstat] (fs::path parent_dir, directory_entry de) {
            BOOST_REQUIRE(fs::canonical(parent_dir / fs::path(de.name)) != tempstat);
            return make_ready_future<>();
        }).get();

        remove_file(sst->filename(sstables::component_type::Statistics)).get();

        sharded<sstable_directory> sstdir_fatal;
        sstdir_fatal.start(upload_path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::no,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    auto& cf = e.local_db().find_column_family("ks", "cf");
                    return cf.make_sstable(dir.native(), gen, v, f);
                }).get();

        auto stop_fatal = defer([&sstdir_fatal] {
            sstdir_fatal.stop().get();
        });

        auto expect_malformed_sstable  = sstable_directory::process_sstable_dir(sstdir_fatal);
        BOOST_REQUIRE_THROW(expect_malformed_sstable.get(), sstables::malformed_sstable_exception);

    });
}

// Test that the sstable_dir object can keep the table alive against a drop
SEASTAR_TEST_CASE(sstable_directory_test_table_lock_works) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto ks_name = "ks";
        auto cf_name = "cf";
        auto& cf = e.local_db().find_column_family(ks_name, cf_name);
        auto path = fs::path(cf.dir());

        sharded<sstable_directory> sstdir;
        sstdir.start(path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::no,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    return nullptr;
                }).get();

        // stop cleanly in case we fail early for unexpected reasons
        auto stop = defer([&sstdir] {
            sstdir.stop().get();
        });

        sstable_directory::lock_table(sstdir, e.db(), ks_name, cf_name).get();

        auto drop = e.execute_cql("drop table cf");
        later().get();

        auto table_ok = e.db().invoke_on_all([ks_name, cf_name] (database& db) {
            db.find_column_family(ks_name, cf_name);
        });
        BOOST_REQUIRE_NO_THROW(table_ok.get());

        // Stop manually now, to allow for the object to be destroyed and take the
        // phaser with it.
        stop.cancel();
        sstdir.stop().get();
        drop.get();

        auto no_such_table = e.db().invoke_on_all([ks_name, cf_name] (database& db) {
            db.find_column_family(ks_name, cf_name);
            return make_ready_future<>();
        });
        BOOST_REQUIRE_THROW(no_such_table.get(), no_such_column_family);
    });
}

// Test that we see the right generation during the scan. Temporary files are skipped
SEASTAR_TEST_CASE(sstable_directory_test_generation_sanity) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto& cf = e.local_db().find_column_family("ks", "cf");
        cf.disable_auto_compaction();
        auto upload_path = fs::path(cf.dir()) / "upload";

        make_sstable_for_this_shard(e.local_db(), cf, upload_path.native(), 3333, sstables::sstable::version_types::mc, sstables::sstable::format_types::big).get();
        auto sst = make_sstable_for_this_shard(e.local_db(), cf, upload_path.native(), 6666, sstables::sstable::version_types::mc, sstables::sstable::format_types::big).get0();
        rename_file(sst->filename(sstables::component_type::TOC), sst->filename(sstables::component_type::TemporaryTOC)).get();

        sharded<sstable_directory> sstdir;
        sstdir.start(upload_path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::yes,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    auto& cf = e.local_db().find_column_family("ks", "cf");
                    return cf.make_sstable(dir.native(), gen, v, f);
                }).get();

        auto stop = defer([&sstdir] {
            sstdir.stop().get();
        });

        sstable_directory::process_sstable_dir(sstdir).get();
        int64_t max_generation_seen = sstable_directory::highest_generation_seen(sstdir).get0();
        BOOST_REQUIRE_EQUAL(max_generation_seen, 3333);
    });
}

future<> verify_that_all_sstables_are_local(sharded<sstable_directory>& sstdir, unsigned expected_sstables) {
    return do_with(std::make_unique<std::atomic<unsigned>>(0), [&sstdir, expected_sstables] (std::unique_ptr<std::atomic<unsigned>>& count) {
        return sstable_directory::do_for_each_sstable(sstdir, [&count] (sstables::shared_sstable sst) {
            count->fetch_add(1, std::memory_order_relaxed);
            auto shards = sst->get_shards_for_this_sstable();
            BOOST_REQUIRE_EQUAL(shards.size(), 1);
            BOOST_REQUIRE_EQUAL(shards[0], this_shard_id());
            return make_ready_future<>();
        }).then([&count, expected_sstables] {
            BOOST_REQUIRE_EQUAL(count->load(std::memory_order_relaxed), expected_sstables);
            return make_ready_future<>();
        });
    });
}

// Test that all SSTables are seen as unshared, if the generation numbers match what their
// shard-assignments expect
SEASTAR_TEST_CASE(sstable_directory_unshared_sstables_sanity_matched_generations) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto& cf = e.local_db().find_column_family("ks", "cf");
        cf.disable_auto_compaction();
        auto upload_path = fs::path(cf.dir()) / "upload";

        e.db().invoke_on_all([upload_path] (database& db) {
            auto& cf = db.find_column_family("ks", "cf");
            return make_sstable_for_this_shard(db, cf, upload_path.native()).discard_result();
        }).get();

        sharded<sstable_directory> sstdir;
        sstdir.start(upload_path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::yes,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    auto& cf = e.local_db().find_column_family("ks", "cf");
                    return cf.make_sstable(dir.native(), gen, v, f);
                }).get();

        auto stop = defer([&sstdir] {
            sstdir.stop().get();
        });

        sstable_directory::process_sstable_dir(sstdir).get();
        verify_that_all_sstables_are_local(sstdir, smp::count).get();
    });
}

// Test that all SSTables are seen as unshared, even if the generation numbers do not match what their
// shard-assignments expect
SEASTAR_TEST_CASE(sstable_directory_unshared_sstables_sanity_unmatched_generations) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto& cf = e.local_db().find_column_family("ks", "cf");
        cf.disable_auto_compaction();
        auto upload_path = fs::path(cf.dir()) / "upload";

        e.db().invoke_on_all([upload_path] (database& db) {
            auto generation = this_shard_id() + 1;
            auto& cf = db.find_column_family("ks", "cf");
            return make_sstable_for_this_shard(db, cf, upload_path.native(), generation,
                    sstables::sstable::version_types::mc, sstables::sstable::format_types::big).discard_result();
        }).get();

        sharded<sstable_directory> sstdir;
        sstdir.start(upload_path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::yes,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    auto& cf = e.local_db().find_column_family("ks", "cf");
                    return cf.make_sstable(dir.native(), gen, v, f);
                }).get();

        auto stop = defer([&sstdir] {
            sstdir.stop().get();
        });

        sstable_directory::process_sstable_dir(sstdir).get();
        verify_that_all_sstables_are_local(sstdir, smp::count).get();
    });
}

SEASTAR_TEST_CASE(sstable_directory_shared_sstables_reshard_correctly) {
    if (smp::count == 1) {
        fmt::print("Skipping sstable_directory_shared_sstables_reshard_correctly, smp == 1\n");
        return make_ready_future<>();
    }

    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto& cf = e.local_db().find_column_family("ks", "cf");
        auto upload_path = fs::path(cf.dir()) / "upload";

        e.db().invoke_on_all([] (database& db) {
            auto& cf = db.find_column_family("ks", "cf");
            cf.disable_auto_compaction();
        }).get();

        unsigned num_sstables = 10 * smp::count;
        auto generation = 0;
        for (unsigned nr = 0; nr < num_sstables; ++nr) {
            make_sstable_for_all_shards(e.db().local(), cf, upload_path.native(), generation++, sstables::sstable_version_types::mc, sstables::sstable::format_types::big).get();
        }

        sharded<sstable_directory> sstdir;
        sstdir.start(upload_path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::yes,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    auto& cf = e.local_db().find_column_family("ks", "cf");
                    return cf.make_sstable(dir.native(), gen, v, f);
                }).get();

        auto stop = defer([&sstdir] {
            sstdir.stop().get();
        });

        sstable_directory::process_sstable_dir(sstdir).get();
        verify_that_all_sstables_are_local(sstdir, 0).get();

        int64_t max_generation_seen = sstable_directory::highest_generation_seen(sstdir).get0();
        std::atomic<int64_t> generation_for_test = {};
        generation_for_test.store(max_generation_seen + 1, std::memory_order_relaxed);

        sstable_directory::reshard(sstdir, e.db(), "ks", "cf", [&e, upload_path, &generation_for_test] (shard_id id) {
            auto generation = generation_for_test.fetch_add(1, std::memory_order_relaxed);
            auto& cf = e.local_db().find_column_family("ks", "cf");
            return cf.make_sstable(upload_path.native(), generation, sstables::sstable::version_types::mc, sstables::sstable::format_types::big);
        }).get();
        verify_that_all_sstables_are_local(sstdir, smp::count * smp::count).get();
    });
}

SEASTAR_TEST_CASE(sstable_directory_shared_sstables_reshard_distributes_well_even_if_files_are_not_well_distributed) {
    if (smp::count == 1) {
        fmt::print("Skipping sstable_directory_shared_sstables_reshard_correctly, smp == 1\n");
        return make_ready_future<>();
    }

    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto& cf = e.local_db().find_column_family("ks", "cf");
        auto upload_path = fs::path(cf.dir()) / "upload";

        e.db().invoke_on_all([] (database& db) {
            auto& cf = db.find_column_family("ks", "cf");
            cf.disable_auto_compaction();
        }).get();

        unsigned num_sstables = 10 * smp::count;
        auto generation = 0;
        for (unsigned nr = 0; nr < num_sstables; ++nr) {
            make_sstable_for_all_shards(e.db().local(), cf, upload_path.native(), generation++ * smp::count, sstables::sstable_version_types::mc, sstables::sstable::format_types::big).get();
        }

        sharded<sstable_directory> sstdir;
        sstdir.start(upload_path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::yes,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    auto& cf = e.local_db().find_column_family("ks", "cf");
                    return cf.make_sstable(dir.native(), gen, v, f);
                }).get();

        auto stop = defer([&sstdir] {
            sstdir.stop().get();
        });

        sstable_directory::process_sstable_dir(sstdir).get();
        verify_that_all_sstables_are_local(sstdir, 0).get();

        int64_t max_generation_seen = sstable_directory::highest_generation_seen(sstdir).get0();
        std::atomic<int64_t> generation_for_test = {};
        generation_for_test.store(max_generation_seen + 1, std::memory_order_relaxed);

        sstable_directory::reshard(sstdir, e.db(), "ks", "cf", [&e, upload_path, &generation_for_test] (shard_id id) {
            auto generation = generation_for_test.fetch_add(1, std::memory_order_relaxed);
            auto& cf = e.local_db().find_column_family("ks", "cf");
            return cf.make_sstable(upload_path.native(), generation, sstables::sstable::version_types::mc, sstables::sstable::format_types::big);
        }).get();
        verify_that_all_sstables_are_local(sstdir, smp::count * smp::count).get();
    });
}

SEASTAR_TEST_CASE(sstable_directory_shared_sstables_reshard_respect_max_threshold) {
    if (smp::count == 1) {
        fmt::print("Skipping sstable_directory_shared_sstables_reshard_correctly, smp == 1\n");
        return make_ready_future<>();
    }

    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto& cf = e.local_db().find_column_family("ks", "cf");
        auto upload_path = fs::path(cf.dir()) / "upload";

        e.db().invoke_on_all([] (database& db) {
            auto& cf = db.find_column_family("ks", "cf");
            cf.disable_auto_compaction();
        }).get();

        unsigned num_sstables = (cf.schema()->max_compaction_threshold() + 1) * smp::count;
        auto generation = 0;
        for (unsigned nr = 0; nr < num_sstables; ++nr) {
            make_sstable_for_all_shards(e.db().local(), cf, upload_path.native(), generation++, sstables::sstable_version_types::mc, sstables::sstable::format_types::big).get();
        }

        sharded<sstable_directory> sstdir;
        sstdir.start(upload_path, 1,
                sstable_directory::need_mutate_level::no,
                sstable_directory::lack_of_toc_fatal::yes,
                sstable_directory::enable_dangerous_direct_import_of_cassandra_counters::no,
                sstable_directory::allow_loading_materialized_view::no,
                [&e] (fs::path dir, int64_t gen, sstables::sstable_version_types v, sstables::sstable_format_types f) {
                    auto& cf = e.local_db().find_column_family("ks", "cf");
                    return cf.make_sstable(dir.native(), gen, v, f);
                }).get();

        auto stop = defer([&sstdir] {
            sstdir.stop().get();
        });

        sstable_directory::process_sstable_dir(sstdir).get();
        verify_that_all_sstables_are_local(sstdir, 0).get();

        int64_t max_generation_seen = sstable_directory::highest_generation_seen(sstdir).get0();
        std::atomic<int64_t> generation_for_test = {};
        generation_for_test.store(max_generation_seen + 1, std::memory_order_relaxed);

        sstable_directory::reshard(sstdir, e.db(), "ks", "cf", [&e, upload_path, &generation_for_test] (shard_id id) {
            auto generation = generation_for_test.fetch_add(1, std::memory_order_relaxed);
            auto& cf = e.local_db().find_column_family("ks", "cf");
            return cf.make_sstable(upload_path.native(), generation, sstables::sstable::version_types::mc, sstables::sstable::format_types::big);
        }).get();
        verify_that_all_sstables_are_local(sstdir, 2 * smp::count * smp::count).get();
    });
}
