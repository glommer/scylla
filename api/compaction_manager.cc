/*
 * Copyright 2015 Cloudius Systems
 */

#include "compaction_manager.hh"
#include "api/api-doc/compaction_manager.json.hh"

namespace api {

using namespace scollectd;
namespace cm = httpd::compaction_manager_json;


static future<json::json_return_type> get_cm_stats(http_context& ctx,
        int64_t compaction_manager::stats::*f) {
    return ctx.db.map_reduce0([&](database& db) {
        return db.get_compaction_manager().get_stats().*f;
    }, int64_t(0), std::plus<int64_t>()).then([](const int64_t& res) {
        return make_ready_future<json::json_return_type>(res);
    });
}

void set_compaction_manager(http_context& ctx, routes& r) {
    cm::get_compactions.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        std::vector<cm::jsonmap> map;
        return make_ready_future<json::json_return_type>(map);
    });

    cm::get_compaction_summary.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        std::vector<sstring> res;
        return make_ready_future<json::json_return_type>(res);
    });

    cm::force_user_defined_compaction.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    cm::stop_compaction.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>("");
    });

    cm::get_pending_tasks.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cm_stats(ctx, &compaction_manager::stats::pending_tasks);
    });

    cm::get_completed_tasks.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cm_stats(ctx, &compaction_manager::stats::completed_tasks);
    });

    cm::get_total_compactions_completed.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cm::get_bytes_compacted.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });



}

}

