/*
 * Copyright 2015 Cloudius Systems
 */

#include "storage_service.hh"
#include "api/api-doc/storage_service.json.hh"
#include <service/storage_service.hh>
#include <db/commitlog/commitlog.hh>
#include <gms/gossiper.hh>

namespace api {

namespace ss = httpd::storage_service_json;

void set_storage_service(http_context& ctx, routes& r) {
    ss::local_hostid.set(r, [](const_req req) {
        return "";
    });

    ss::get_tokens.set(r, [](std::unique_ptr<request> req) {
        return service::sorted_tokens().then([](const std::vector<dht::token>& tokens) {
            return make_ready_future<json::json_return_type>(container_to_vec(tokens));
        });
    });

    ss::get_node_tokens.set(r, [](std::unique_ptr<request> req) {
        gms::inet_address addr(req->param["endpoint"]);
        return service::get_tokens(addr).then([](const std::vector<dht::token>& tokens) {
            return make_ready_future<json::json_return_type>(container_to_vec(tokens));
        });
    });

    ss::get_commitlog.set(r, [&ctx](const_req req) {
        return ctx.db.local().commitlog()->active_config().commit_log_location;
    });

    ss::get_token_endpoint.set(r, [](std::unique_ptr<request> req) {
        return service::get_token_to_endpoint().then([] (const std::map<dht::token, gms::inet_address>& tokens){
            std::vector<storage_service_json::mapper> res;
            return make_ready_future<json::json_return_type>(map_to_key_value(tokens, res));
        });
    });

    ss::get_leaving_nodes.set(r, [](const_req req) {
        return container_to_vec(service::get_local_storage_service().get_token_metadata().get_leaving_endpoints());
    });

    ss::get_moving_nodes.set(r, [](const_req req) {
        auto points = service::get_local_storage_service().get_token_metadata().get_moving_endpoints();
        std::unordered_set<sstring> addr;
        for (auto i: points) {
            addr.insert(boost::lexical_cast<std::string>(i.second));
        }
        return container_to_vec(addr);
    });

    ss::get_joining_nodes.set(r, [](const_req req) {
        auto points = service::get_local_storage_service().get_token_metadata().get_bootstrap_tokens();
        std::unordered_set<sstring> addr;
        for (auto i: points) {
            addr.insert(boost::lexical_cast<std::string>(i.second));
        }
        return container_to_vec(addr);
    });

    ss::get_release_version.set(r, [](const_req req) {
        //TBD
        return "";
    });

    ss::get_schema_version.set(r, [](const_req req) {
        //TBD
        return "";
    });

    ss::get_all_data_file_locations.set(r, [&ctx](const_req req) {
        return container_to_vec(ctx.db.local().get_config().data_file_directories());
    });

    ss::get_saved_caches_location.set(r, [&ctx](const_req req) {
        return ctx.db.local().get_config().saved_caches_directory();
    });

    ss::get_range_to_endpoint_map.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = req->param["keyspace"];
        std::vector<ss::maplist_mapper> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::get_pending_range_to_endpoint_map.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = req->param["keyspace"];
        std::vector<ss::maplist_mapper> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::describe_ring_jmx.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = req->param["keyspace"];
        std::vector<sstring> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::get_host_id_map.set(r, [](const_req req) {
        std::vector<ss::mapper> res;
        return map_to_key_value(service::get_local_storage_service().
                get_token_metadata().get_endpoint_to_host_id_map_for_reading(), res);
    });

    ss::get_load.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::get_load_map.set(r, [](std::unique_ptr<request> req) {
        //TBD
        std::vector<ss::mapper> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::get_current_generation_number.set(r, [](std::unique_ptr<request> req) {
        gms::inet_address ep(utils::fb_utilities::get_broadcast_address());
        return gms::get_current_generation_number(ep).then([](int res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    ss::get_natural_endpoints.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = req->param["keyspace"];
        auto column_family = req->get_query_param("cf");
        auto key = req->get_query_param("key");

        std::vector<sstring> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::get_snapshot_details.set(r, [](std::unique_ptr<request> req) {
        //TBD
        std::vector<ss::snapshots> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::take_snapshot.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto tag = req->get_query_param("tag");
        auto keyname = req->get_query_param("kn");
        auto column_family = req->get_query_param("cf");
        return make_ready_future<json::json_return_type>("");
    });

    ss::del_snapshot.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto tag = req->get_query_param("tag");
        auto keyname = req->get_query_param("kn");
        return make_ready_future<json::json_return_type>("");
    });

    ss::true_snapshots_size.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::force_keyspace_compaction.set(r, [&ctx](std::unique_ptr<request> req) {
        auto keyspace = req->param["keyspace"];
        auto column_families = split_cf(req->get_query_param("cf"));
        if (column_families.empty()) {
            column_families = map_keys(ctx.db.local().find_keyspace(keyspace).metadata().get()->cf_meta_data());
        }
        return ctx.db.invoke_on_all([keyspace, column_families] (database& db) {
            std::vector<column_family*> column_families_vec;
            for (auto cf : column_families) {
                column_families_vec.push_back(&db.find_column_family(keyspace, cf));
            }
            return parallel_for_each(column_families_vec, [] (column_family* cf) {
                    return cf->compact_all_sstables();
            });
        }).then([]{
                return make_ready_future<json::json_return_type>("");
        });
    });

    ss::force_keyspace_cleanup.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = req->param["keyspace"];
        auto column_family = req->get_query_param("cf");
        return make_ready_future<json::json_return_type>("");
    });

    ss::scrub.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = req->param["keyspace"];
        auto column_family = req->get_query_param("cf");
        auto disable_snapshot = req->get_query_param("disable_snapshot");
        auto skip_corrupted = req->get_query_param("skip_corrupted");
        return make_ready_future<json::json_return_type>("");
    });

    ss::upgrade_sstables.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = req->param["keyspace"];
        auto column_family = req->get_query_param("cf");
        auto exclude_current_version = req->get_query_param("exclude_current_version");
        return make_ready_future<json::json_return_type>("");
    });

    ss::force_keyspace_flush.set(r, [&ctx](std::unique_ptr<request> req) {
        auto keyspace = req->param["keyspace"];
        auto column_families = split_cf(req->get_query_param("cf"));
        if (column_families.empty()) {
            column_families = map_keys(ctx.db.local().find_keyspace(keyspace).metadata().get()->cf_meta_data());
        }
        return ctx.db.invoke_on_all([keyspace, column_families] (database& db) {
            for (auto cf : column_families) {
                db.find_column_family(keyspace, cf).seal_active_memtable();
            }
        }).then([]{
                return make_ready_future<json::json_return_type>("");
        });
    });

    ss::repair_async.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = req->param["keyspace"];
        auto column_family = req->get_query_param("option");
        auto id = req->get_query_param("id");
        return make_ready_future<json::json_return_type>(0);
    });

    ss::force_terminate_all_repair_sessions.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::decommission.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::move.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto new_token = req->get_query_param("new_token");
        return make_ready_future<json::json_return_type>("");
    });

    ss::remove_node.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto token = req->get_query_param("token");
        return make_ready_future<json::json_return_type>("");
    });

    ss::get_removal_status.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::force_remove_completion.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::set_logging_level.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto class_qualifier = req->get_query_param("class_qualifier");
        auto level = req->get_query_param("level");
        return make_ready_future<json::json_return_type>("");
    });

    ss::get_logging_levels.set(r, [](std::unique_ptr<request> req) {
        //TBD
        std::vector<ss::mapper> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::get_operation_mode.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::is_starting.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(false);
    });

    ss::get_drain_progress.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::drain.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });
    ss::truncate.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = req->param["keyspace"];
        auto column_family = req->get_query_param("cf");
        return make_ready_future<json::json_return_type>("");
    });

    ss::get_keyspaces.set(r, [&ctx](const_req req) {
        auto non_system = req.get_query_param("non_system");
        return map_keys(ctx.db.local().keyspaces());
    });

    ss::update_snitch.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto ep_snitch_class_name = req->get_query_param("ep_snitch_class_name");
        auto dynamic = req->get_query_param("dynamic");
        auto dynamic_update_interval = req->get_query_param("dynamic_update_interval");
        auto dynamic_reset_interval = req->get_query_param("dynamic_reset_interval");
        auto dynamic_badness_threshold = req->get_query_param("dynamic_badness_threshold");
        return make_ready_future<json::json_return_type>("");
    });

    ss::stop_gossiping.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::start_gossiping.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::is_gossip_running.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(true);
    });

    ss::stop_daemon.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::is_initialized.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(false);
    });

    ss::stop_rpc_server.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::start_rpc_server.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::is_rpc_server_running.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(true);
    });

    ss::start_native_transport.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::stop_native_transport.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::is_native_transport_running.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(true);
    });

    ss::join_ring.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::is_joined.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(false);
    });

    ss::set_stream_throughput_mb_per_sec.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto value = req->get_query_param("value");
        return make_ready_future<json::json_return_type>("");
    });

    ss::get_stream_throughput_mb_per_sec.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::get_compaction_throughput_mb_per_sec.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::set_compaction_throughput_mb_per_sec.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto value = req->get_query_param("value");
        return make_ready_future<json::json_return_type>("");
    });

    ss::is_incremental_backups_enabled.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(false);
    });

    ss::set_incremental_backups_enabled.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto value = req->get_query_param("value");
        return make_ready_future<json::json_return_type>("");
    });

    ss::rebuild.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto source_dc = req->get_query_param("source_dc");
        return make_ready_future<json::json_return_type>("");
    });

    ss::bulk_load.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto path = req->param["path"];
        return make_ready_future<json::json_return_type>("");
    });

    ss::bulk_load_async.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto path = req->param["path"];
        return make_ready_future<json::json_return_type>("");
    });

    ss::reschedule_failed_deletions.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::load_new_ss_tables.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = req->param["keyspace"];
        auto column_family = req->get_query_param("cf");
        return make_ready_future<json::json_return_type>("");
    });

    ss::sample_key_range.set(r, [](std::unique_ptr<request> req) {
        //TBD
        std::vector<sstring> res;
        return make_ready_future<json::json_return_type>(res);
    });

    ss::reset_local_schema.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::set_trace_probability.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto probability = req->get_query_param("probability");
        return make_ready_future<json::json_return_type>("");
    });

    ss::get_trace_probability.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::enable_auto_compaction.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = req->param["keyspace"];
        auto column_family = req->get_query_param("cf");
        return make_ready_future<json::json_return_type>("");
    });

    ss::disable_auto_compaction.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto keyspace = req->param["keyspace"];
        auto column_family = req->get_query_param("cf");
        return make_ready_future<json::json_return_type>("");
    });

    ss::deliver_hints.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>("");
      });

    ss::get_cluster_name.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::get_partitioner_name.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    ss::get_tombstone_warn_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::set_tombstone_warn_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto debug_threshold = req->get_query_param("debug_threshold");
        return make_ready_future<json::json_return_type>("");
    });

    ss::get_tombstone_failure_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::set_tombstone_failure_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto debug_threshold = req->get_query_param("debug_threshold");
        return make_ready_future<json::json_return_type>("");
    });

    ss::get_batch_size_failure_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    ss::set_batch_size_failure_threshold.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto threshold = req->get_query_param("threshold");
        return make_ready_future<json::json_return_type>("");
    });

    ss::set_hinted_handoff_throttle_in_kb.set(r, [](std::unique_ptr<request> req) {
        //TBD
        auto debug_threshold = req->get_query_param("throttle");
        return make_ready_future<json::json_return_type>("throttle");
    });
}

}
