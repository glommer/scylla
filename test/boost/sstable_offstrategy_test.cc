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
#include "distributed_loader.hh"
#include "db/config.hh"

#include "fmt/format.h"

class test_directory_with_sstables_handler : public directory_with_sstables_handler {
    unsigned _file_count = 0;
    std::vector<unsigned> _inode_list = {};
public:
    using directory_with_sstables_handler::get_destination_unshared_sstable;
    using directory_with_sstables_handler::process_sstable_dir;

    test_directory_with_sstables_handler(distributed<database>& db, sstring ks_name, sstring cf_name, fs::path sstable_main_dir, fs::path aux_dir)
        : directory_with_sstables_handler(db, std::move(ks_name), std::move(cf_name), std::move(sstable_main_dir), std::move(aux_dir))
    {}
    future<unsigned> count_files(fs::path path) {
        _file_count = 0;
        return lister::scan_dir(path, { directory_entry_type::regular }, [this] (fs::path parent_dir, directory_entry de) {
            _file_count++;
            return make_ready_future<>();
        }, &directory_with_sstables_handler::manifest_json_filter).then([this] {
            return make_ready_future<unsigned>(_file_count);
        });
    }
    future<std::vector<unsigned>> scan_inode_list(fs::path path) {
        std::exchange(_inode_list, std::vector<unsigned>());
        return lister::scan_dir(path, { directory_entry_type::regular }, [this] (fs::path parent_dir, directory_entry de) {
            return file_stat((parent_dir / de.name).native(), follow_symlink::no).then([this] (stat_data stat) {
                _inode_list.push_back(stat.inode_number);
                return make_ready_future<>();
            });
        }, &directory_with_sstables_handler::manifest_json_filter).then([this] {
            return make_ready_future<std::vector<unsigned>>(std::move(_inode_list));
        });
    }
};

// Tests that locking fails if the table is not present.
SEASTAR_TEST_CASE(offstrategy_test_table_lock_fails_no_table) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        test_directory_with_sstables_handler handler(e.db(), "ks", "cf", fs::path("/var/lib/scylla"), "");
        BOOST_REQUIRE_THROW(handler.lock_local_tables().get(), no_such_column_family);
    });
}

// Tests that locking the table successfully prevents it from disappearing in the middle of the
// operation
SEASTAR_TEST_CASE(offstrategy_test_table_lock) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto ks_name = "ks";
        auto cf_name = "cf";
        auto& cf = e.local_db().find_column_family(ks_name, cf_name);
        test_directory_with_sstables_handler handler(e.db(), ks_name, cf_name, fs::path(cf.dir()), "");
        BOOST_REQUIRE_NO_THROW(handler.lock_local_tables().get());

        auto drop = e.execute_cql("drop table cf");
        later().get();

        e.db().invoke_on_all([ks_name, cf_name] (database& db) {
            BOOST_REQUIRE_NO_THROW(db.find_column_family(ks_name, cf_name));
        }).get();

        BOOST_REQUIRE_NO_THROW(handler.unlock_local_tables().get());

        drop.get();
        e.db().invoke_on_all([ks_name, cf_name] (database& db) {
            BOOST_REQUIRE_THROW(db.find_column_family(ks_name, cf_name), no_such_column_family);
        }).get();
    });
}

// Tests the sanity of the scan process and validation
SEASTAR_TEST_CASE(offstrategy_empty_and_validation_fail) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto ks_name = "ks";
        auto cf_name = "cf";
        auto s = e.local_db().find_schema(ks_name, cf_name);
        auto& cf = e.local_db().find_column_family(ks_name, cf_name);
        auto upload_path = fs::path(cf.dir()) / "upload";

        // Write a manifest file to make sure it's ignored
        auto manifest = upload_path / "manifest.json";
        auto f = open_file_dma(manifest.native(), open_flags::wo | open_flags::create | open_flags::truncate).get0();
        f.close().get();

        // upload directory being empty (but a manifest file is ok)
        test_directory_with_sstables_handler handler(e.db(), ks_name, cf_name, fs::path(cf.dir()), "upload");
        // 1. scan succeeds
        BOOST_REQUIRE_NO_THROW(handler.process_sstable_dir().get());
        auto sst = handler.get_destination_unshared_sstable(0);
        // 2. new sstable has generation of 1.
        BOOST_REQUIRE_GE(sst->generation(), 1);
        // 3. the output directory is sane.
        BOOST_REQUIRE_EQUAL(sst->get_dir(), upload_path.native());

        // Generates a temporary sstable just so we can write something
        auto mt = make_lw_shared<memtable>(s);
        auto msb = e.local_db().get_config().murmur3_partitioner_ignore_msb_bits();
        auto key_token_pair = token_generation_for_shard(1, this_shard_id(), msb);
        auto key = partition_key::from_exploded(*s, {to_bytes(key_token_pair[0].first)});
        mutation m(s, key);
        m.set_clustered_cell(clustering_key::make_empty(), bytes("c"), data_value(int32_t(0)), api::timestamp_type(0));
        mt->apply(std::move(m));
        write_memtable_to_sstable_for_test(*mt, sst).get();
        mt->clear_gently().get();

        // Now there is one sstable to the upload directory, but it is incomplete and one component is missing. 
        // We should fail validation and leave the directory untouched
        remove_file(sst->filename(sstables::component_type::Statistics)).get();
        BOOST_REQUIRE_NE(handler.count_files(upload_path).get0(), 0);

        // An SSTable missing its TOC, though, will be considered incompleted and discarded
        rename_file(sst->filename(sstables::component_type::TOC), sst->filename(sstables::component_type::TemporaryTOC)).get();

        test_directory_with_sstables_handler newhandler(e.db(), ks_name, cf_name, fs::path(cf.dir()), "upload");
        // 1. scan succeeds
        BOOST_REQUIRE_NO_THROW(newhandler.process_sstable_dir().get());
        // 3. The directory is left empty
        BOOST_REQUIRE_EQUAL(newhandler.count_files(upload_path).get0(), 0);
    });
}

// Tests that file that are already unshared are not touched during reshard
SEASTAR_TEST_CASE(offstrategy_reshard_skips_unshared) {
    if (smp::count == 1) {
        fmt::print("Skipping offstrategy_reshard_skips_unshared with smp == 1\n");
        return make_ready_future<>();
    }
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto ks_name = "ks";
        auto cf_name = "cf";
        auto s = e.local_db().find_schema(ks_name, cf_name);
        auto& cf = e.local_db().find_column_family(ks_name, cf_name);
        auto upload_path = fs::path(cf.dir()) / "upload";

        auto msb = e.local_db().get_config().murmur3_partitioner_ignore_msb_bits();
        test_directory_with_sstables_handler handler(e.db(), ks_name, cf_name, fs::path(cf.dir()), "upload");

        e.db().invoke_on_all([ks_name, cf_name, upload_path, msb] (database& db) {
            return seastar::async([ks_name, cf_name, upload_path, msb, &db] {
                auto& cf = db.find_column_family(ks_name, cf_name);
                auto s = cf.schema();

                // Each shard generates 10 SSTables. All of them are unshared, so resharding
                // should just skip them
                auto key_token_pair = token_generation_for_shard(10, this_shard_id(), msb);
                for (auto& kpr : key_token_pair) {
                    auto mt = make_lw_shared<memtable>(s);
                    auto key = partition_key::from_exploded(*s, {to_bytes(kpr.first)});
                    mutation m(s, key);
                    m.set_clustered_cell(clustering_key::make_empty(), bytes("c"), data_value(int32_t(0)), api::timestamp_type(0));
                    mt->apply(std::move(m));

                    auto sst = cf.make_sstable(upload_path.native());
                    write_memtable_to_sstable(*mt, sst, cf.get_sstables_manager().configure_writer()).get();
                    // We have to do the right thing even if the SSTable came from Cassandra
                    sstables::test(sst).rewrite_toc_without_scylla_component();
                }
            });
        }).get();

        BOOST_REQUIRE_NO_THROW(handler.process_sstable_dir().get());

        BOOST_REQUIRE_EQUAL(handler.num_unopened_unshared(), 10 * smp::count);
        BOOST_REQUIRE_EQUAL(handler.num_unopened_shared(), 0);

        auto inode_list_before = handler.scan_inode_list(upload_path).get0();
        e.db().invoke_on_all([&handler] (database& db) {
            return handler.reshard();
        }).get();

        auto inode_list_after = handler.scan_inode_list(upload_path).get0();
        BOOST_REQUIRE_EQUAL(inode_list_before, inode_list_after);
    });
}

// Tests that resharding generates the expected number of SSTables
SEASTAR_TEST_CASE(offstrategy_reshard) {
    if (smp::count == 1) {
        fmt::print("Skipping offstrategy_reshard smp == 1\n");
        return make_ready_future<>();
    }
    return do_with_cql_env_thread([] (cql_test_env& e) {
        e.execute_cql("create table cf (p text PRIMARY KEY, c int)").get();
        auto ks_name = "ks";
        auto cf_name = "cf";
        auto s = e.local_db().find_schema(ks_name, cf_name);
        auto& cf = e.local_db().find_column_family(ks_name, cf_name);
        auto upload_path = fs::path(cf.dir()) / "upload";

        auto msb = e.local_db().get_config().murmur3_partitioner_ignore_msb_bits();
        test_directory_with_sstables_handler handler(e.db(), ks_name, cf_name, fs::path(cf.dir()), "upload");

        unsigned num_sstables = 10 * smp::count;

        for (unsigned nr = 0; nr < num_sstables; ++nr) {
            auto mt = make_lw_shared<memtable>(s);
            for (shard_id shard = 0; shard < smp::count; ++shard) {
                // Each shard generates 10 SSTables. All of them are unshared, so resharding
                // should just skip them
                auto key_token_pair = token_generation_for_shard(1, shard, msb);
                auto key = partition_key::from_exploded(*s, {to_bytes(key_token_pair[0].first)});
                mutation m(s, key);
                m.set_clustered_cell(clustering_key::make_empty(), bytes("c"), data_value(int32_t(0)), api::timestamp_type(0));
                mt->apply(std::move(m));
            }
            auto sst = cf.make_sstable(upload_path.native());
            write_memtable_to_sstable_for_test(*mt, sst).get();
            // We can't write an SSTable with bad sharding, so pretend
            // it came from Cassandra
            sstables::test(sst).remove_component(sstables::component_type::Scylla).get();
            sstables::test(sst).rewrite_toc_without_scylla_component();
        }

        BOOST_REQUIRE_NO_THROW(handler.process_sstable_dir().get());
        BOOST_REQUIRE_EQUAL(handler.num_unopened_unshared(), 0);
        BOOST_REQUIRE_EQUAL(handler.num_unopened_shared(), 10 * smp::count);

        e.db().invoke_on_all([&handler] (database& db) {
            return handler.reshard();
        }).get();

        // Each source SSTable set generates one SSTable per shard
        BOOST_REQUIRE_EQUAL(handler.num_unopened_unshared(), smp::count * smp::count);

        // Open it again on a clean slate to make sure the SSTables are indeed recognized as unshared.
        // Also have to make sure that the old SSTables are gone
        test_directory_with_sstables_handler handler_unshared(e.db(), ks_name, cf_name, fs::path(cf.dir()), "upload");

        // Unfortunately SSTable deletion code is completely asynchronous and there is no way to
        // wait for it. reshard() will have triggered all deletes, but we will not be able to
        // see them right away, and if we try to scan the directory again right away we'll get
        // plenty of ENOENT.
        sleep(std::chrono::seconds(1)).get();
        BOOST_REQUIRE_NO_THROW(handler_unshared.process_sstable_dir().get());
        BOOST_REQUIRE_EQUAL(handler_unshared.num_unopened_unshared(), smp::count * smp::count);
        BOOST_REQUIRE_EQUAL(handler_unshared.num_unopened_shared(), 0);
    });
}
