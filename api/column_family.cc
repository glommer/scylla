/*
 * Copyright 2015 Cloudius Systems
 */

#include "column_family.hh"
#include "api/api-doc/column_family.json.hh"
#include <vector>
#include "http/exception.hh"
#include "sstables/sstables.hh"
#include "sstables/estimated_histogram.hh"
#include <algorithm>

namespace api {
using namespace httpd;

using namespace std;
using namespace json;
namespace cf = httpd::column_family_json;

auto get_uuid(const sstring& name, const database& db) {
    auto pos = name.find("%3A");
    size_t end;
    if (pos == sstring::npos) {
        pos  = name.find(":");
        if (pos == sstring::npos) {
            throw bad_param_exception("Column family name should be in keyspace::column_family format");
        }
        end = pos + 1;
    } else {
        end = pos + 3;
    }
    try {
        return db.find_uuid(name.substr(0, pos), name.substr(end));
    } catch (std::out_of_range& e) {
        throw bad_param_exception("Column family '" + name.substr(0, pos) + ":"
                + name.substr(end) + "' not found");
    }
}

future<> foreach_column_family(http_context& ctx, const sstring& name, function<void(column_family&)> f) {
    auto uuid = get_uuid(name, ctx.db.local());

    return ctx.db.invoke_on_all([f, uuid](database& db) {
        f(db.find_column_family(uuid));
    });
}

template<class Mapper, class I, class Reducer>
future<json::json_return_type> map_reduce_cf(http_context& ctx, const sstring& name, I init,
        Mapper mapper, Reducer reducer) {
    auto uuid = get_uuid(name, ctx.db.local());
    return ctx.db.map_reduce0([mapper, uuid](database& db) {
        return mapper(db.find_column_family(uuid));
    }, init, reducer).then([](const I& res) {
        return make_ready_future<json::json_return_type>(res);
    });
}

template<class Mapper, class I, class Reducer, class Result>
future<json::json_return_type> map_reduce_cf(http_context& ctx, const sstring& name, I init,
        Mapper mapper, Reducer reducer, Result result) {
    auto uuid = get_uuid(name, ctx.db.local());
    return ctx.db.map_reduce0([mapper, uuid](database& db) {
        return mapper(db.find_column_family(uuid));
    }, init, reducer).then([result](const I& res) mutable {
        result = res;
        return make_ready_future<json::json_return_type>(result);
    });
}

template<class Mapper, class I, class Reducer>
std::enable_if_t<is_future<result_of_t<Mapper(column_family&)>>::value, future<json::json_return_type>>
map_reduce_cf(http_context& ctx, I init,
        Mapper mapper, Reducer reducer) {
    return ctx.db.map_reduce0([mapper, init, reducer](database& db) {
        return map_reduce(db.get_column_families().begin(), db.get_column_families().end(), [mapper] (auto i) {
            return mapper(*i.second.get());
        }, init, reducer);
    }, init, reducer).then([](const I& res) {
        return make_ready_future<json::json_return_type>(res);
    });
}

template<class Mapper, class I, class Reducer>
std::enable_if_t<!is_future<result_of_t<Mapper(column_family&)>>::value, future<json::json_return_type>>
map_reduce_cf(http_context& ctx, I init,
        Mapper mapper, Reducer reducer) {
    return ctx.db.map_reduce0([mapper, init, reducer](database& db) {
        auto res = init;
        for (auto i : db.get_column_families()) {
            res = reducer(res, mapper(*i.second.get()));
        }
        return res;
    }, init, reducer).then([](const I& res) {
        return make_ready_future<json::json_return_type>(res);
    });
}

static future<json::json_return_type>  get_cf_stats(http_context& ctx, const sstring& name,
        int64_t column_family::stats::*f) {
    return map_reduce_cf(ctx, name, 0, [f](const column_family& cf) {
        return cf.get_stats().*f;
    }, std::plus<int64_t>());
}

static future<json::json_return_type>  get_cf_stats(http_context& ctx,
        int64_t column_family::stats::*f) {
    return map_reduce_cf(ctx, 0, [f](const column_family& cf) {
        return cf.get_stats().*f;
    }, std::plus<int64_t>());
}

static future<json::json_return_type>  get_cf_stats_sum(http_context& ctx, const sstring& name,
        utils::ihistogram column_family::stats::*f) {
    return map_reduce_cf(ctx, name, 0, [f](const column_family& cf) {
        return (cf.get_stats().*f).count;
    }, std::plus<int64_t>());
}

static future<json::json_return_type>  get_cf_stats_sum(http_context& ctx,
        utils::ihistogram column_family::stats::*f) {
    return map_reduce_cf(ctx, 0, [f](const column_family& cf) {
        return (cf.get_stats().*f).count;
    }, std::plus<int64_t>());
}

static future<json::json_return_type>  get_cf_histogram(http_context& ctx, const sstring& name,
        utils::ihistogram column_family::stats::*f) {
    utils::UUID uuid = get_uuid(name, ctx.db.local());
    return ctx.db.map_reduce0([f, uuid](const database& p) {return p.find_column_family(uuid).get_stats().*f;},
            httpd::utils_json::histogram(),
            add_histogram)
            .then([](const httpd::utils_json::histogram& val) {
                return make_ready_future<json::json_return_type>(val);
    });
}

static future<json::json_return_type> get_cf_histogram(http_context& ctx, utils::ihistogram column_family::stats::*f) {
    std::function<httpd::utils_json::histogram(const database&)> fun = [f] (const database& db)  {
        httpd::utils_json::histogram res;
        for (auto i : db.get_column_families()) {
            res = add_histogram(res, i.second->get_stats().*f);
        }
        return res;
    };
    return ctx.db.map(fun).then([](const std::vector<httpd::utils_json::histogram> &res) {
        return make_ready_future<json::json_return_type>(res);
    });
}

static int64_t min_row_size(column_family& cf) {
    int64_t res = INT64_MAX;
    for (auto i: *cf.get_sstables() ) {
        res = std::min(res, i.second->get_stats_metadata().estimated_row_size.min());
    }
    return (res == INT64_MAX) ? 0 : res;
}

static int64_t max_row_size(column_family& cf) {
    int64_t res = 0;
    for (auto i: *cf.get_sstables() ) {
        res = std::max(i.second->get_stats_metadata().estimated_row_size.max(), res);
    }
    return res;
}

template<typename Func>
class sstable_stat;

template <typename Func>
sstable_stat<Func> make_sstable_stat(http_context& ctx, Func func) {
    return sstable_stat<Func>(ctx, func);
}

template<typename Func>
class sstable_stat {
    using fut_type = result_of_t<Func(sstables::sstable&)>;
    static_assert(is_future<fut_type>::value, "Expected future");
    using tupl_type = typename fut_type::value_type;
    static_assert(tuple_size<tupl_type>::value == 1, "Expected single value");
    using stat_type = typename std::tuple_element<0, tupl_type>::type;
    static_assert(is_arithmetic<stat_type>::value, "Expected arithmetic type");

    http_context& _ctx;
    Func _func;

    future<stat_type> inner_map_reduce(column_family& cf) {
        return map_reduce(cf.get_sstables()->begin(), cf.get_sstables()->end(), [this] (auto& pair) {
            return _func(*(pair.second));
        }, stat_type(0), std::plus<stat_type>());
    }
    sstable_stat(http_context& ctx, Func func) : _ctx(ctx), _func(func) {}

public:
    future<json::json_return_type> operator()(const sstring& name) {
        return map_reduce_cf(_ctx, name, stat_type(0), [this](column_family& cf) {
            return inner_map_reduce(cf);
        }, std::plus<stat_type>());
    }

    future<json::json_return_type> operator()() {
        return map_reduce_cf(_ctx, stat_type(0), [this](column_family& cf) {
            return inner_map_reduce(cf);
        }, std::plus<stat_type>());
    }

    template <typename F>
    friend sstable_stat<F> make_sstable_stat(http_context& ctx, F func);
};

void set_column_family(http_context& ctx, routes& r) {
    cf::get_column_family_name.set(r, [&ctx] (const_req req){
        vector<sstring> res;
        for (auto i: ctx.db.local().get_column_families_mapping()) {
            res.push_back(i.first.first + ":" + i.first.second);
        }
        return res;
    });

    cf::get_column_family.set(r, [&ctx] (const_req req){
            vector<cf::column_family_info> res;
            for (auto i: ctx.db.local().get_column_families_mapping()) {
                cf::column_family_info info;
                info.ks = i.first.first;
                info.cf =  i.first.second;
                info.type = "ColumnFamilies";
                res.push_back(info);
            }
            return res;
        });

    cf::get_column_family_name_keyspace.set(r, [&ctx] (const_req req){
        vector<sstring> res;
        for (auto i = ctx.db.local().get_keyspaces().cbegin(); i!=  ctx.db.local().get_keyspaces().cend(); i++) {
            res.push_back(i->first);
        }
        return res;
    });

    cf::get_memtable_columns_count.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf(ctx, req->param["name"], 0, [](column_family& cf) {
            return cf.active_memtable().partition_count();
        }, std::plus<int>());
    });

    cf::get_all_memtable_columns_count.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf(ctx, 0, [](column_family& cf) {
            return cf.active_memtable().partition_count();
        }, std::plus<int>());
    });

    cf::get_memtable_on_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_memtable_on_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_memtable_off_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_memtable_off_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_memtable_live_data_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_memtable_live_data_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_memtables_on_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_all_memtables_on_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_memtables_off_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_all_memtables_off_heap_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_memtables_live_data_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_all_memtables_live_data_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_memtable_switch_count.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats(ctx,req->param["name"] ,&column_family::stats::memtable_switch_count);
    });

    cf::get_all_memtable_switch_count.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats(ctx, &column_family::stats::memtable_switch_count);
    });

    cf::get_estimated_row_size_histogram.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf(ctx, req->param["name"], sstables::estimated_histogram(0), [](column_family& cf) {
            sstables::estimated_histogram res(0);
            for (auto i: *cf.get_sstables() ) {
                res.merge(i.second->get_stats_metadata().estimated_row_size);
            }
            return res;
        },
        sstables::merge, utils_json::estimated_histogram());
    });

    cf::get_estimated_column_count_histogram.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        std::vector<double> res;
        return make_ready_future<json::json_return_type>(res);
    });

    cf::get_compression_ratio.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_compression_ratio.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_pending_flushes.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats(ctx,req->param["name"] ,&column_family::stats::pending_flushes);
    });

    cf::get_all_pending_flushes.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats(ctx, &column_family::stats::pending_flushes);
    });

    cf::get_read.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats_sum(ctx,req->param["name"] ,&column_family::stats::reads);
    });

    cf::get_all_read.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats_sum(ctx, &column_family::stats::reads);
    });

    cf::get_write.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats_sum(ctx, req->param["name"] ,&column_family::stats::writes);
    });

    cf::get_all_write.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats_sum(ctx, &column_family::stats::writes);
    });

    cf::get_read_latency_histogram.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_histogram(ctx, req->param["name"], &column_family::stats::reads);
    });

    cf::get_all_read_latency_histogram.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_histogram(ctx, &column_family::stats::writes);
    });

    cf::get_write_latency_histogram.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_histogram(ctx, req->param["name"], &column_family::stats::reads);
    });

    cf::get_all_write_latency_histogram.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_histogram(ctx, &column_family::stats::writes);
    });

    cf::get_pending_compactions.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats(ctx, req->param["name"], &column_family::stats::pending_compactions);
    });

    cf::get_all_pending_compactions.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats(ctx, &column_family::stats::pending_compactions);
    });

    cf::get_live_ss_table_count.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats(ctx, req->param["name"], &column_family::stats::live_sstable_count);
    });

    cf::get_all_live_ss_table_count.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats(ctx, &column_family::stats::live_sstable_count);
    });

    cf::get_live_disk_space_used.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats(ctx, req->param["name"], &column_family::stats::live_disk_space_used);
    });

    cf::get_all_live_disk_space_used.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats(ctx, &column_family::stats::live_disk_space_used);
    });

    cf::get_total_disk_space_used.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats(ctx, req->param["name"], &column_family::stats::total_disk_space_used);
    });

    cf::get_all_total_disk_space_used.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cf_stats(ctx, &column_family::stats::total_disk_space_used);
    });

    cf::get_min_row_size.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf(ctx, req->param["name"], INT64_MAX, min_row_size, min_int64);
    });

    cf::get_all_min_row_size.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf(ctx, INT64_MAX, min_row_size, min_int64);
    });

    cf::get_max_row_size.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf(ctx, req->param["name"], 0, max_row_size, max_int64);
    });

    cf::get_all_max_row_size.set(r, [&ctx] (std::unique_ptr<request> req) {
        return map_reduce_cf(ctx, 0, max_row_size, max_int64);
    });

    cf::get_mean_row_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_mean_row_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_bloom_filter_false_positives.set(r, [&ctx] (std::unique_ptr<request> req) {
        return make_sstable_stat(ctx, [] (sstables::sstable& sst) {
            return sst.filter_get_false_positive();
        })(req->param["name"]);
    });

    cf::get_all_bloom_filter_false_positives.set(r, [&ctx] (std::unique_ptr<request> req) {
        return make_sstable_stat(ctx, [] (sstables::sstable& sst) {
            return sst.filter_get_false_positive();
        })();
    });

    cf::get_recent_bloom_filter_false_positives.set(r, [&ctx] (std::unique_ptr<request> req) {
        return make_sstable_stat(ctx, [] (sstables::sstable& sst) {
            return sst.filter_get_recent_false_positive();
        })(req->param["name"]);
    });

    cf::get_all_recent_bloom_filter_false_positives.set(r, [&ctx] (std::unique_ptr<request> req) {
        return make_sstable_stat(ctx, [] (sstables::sstable& sst) {
            return sst.filter_get_recent_false_positive();
        })();
    });

    cf::get_bloom_filter_false_ratio.set(r, [&ctx] (std::unique_ptr<request> req) {
        return make_sstable_stat(ctx, [] (sstables::sstable& sst) {
            return when_all(sst.filter_get_false_positive(),
                            sst.filter_get_true_positive()).then([] (auto futs) {
                double f = std::get<uint64_t>(std::get<0>(futs).get());
                double t = std::get<uint64_t>(std::get<1>(futs).get());

                return f / (f + t);
            });
        })(req->param["name"]);
    });

    cf::get_all_bloom_filter_false_ratio.set(r, [&ctx] (std::unique_ptr<request> req) {
        return make_sstable_stat(ctx, [] (sstables::sstable& sst) {
            return when_all(sst.filter_get_false_positive(),
                            sst.filter_get_true_positive()).then([] (auto futs) {
                double f = std::get<uint64_t>(std::get<0>(futs).get());
                double t = std::get<uint64_t>(std::get<1>(futs).get());

                return f / (f + t);
            });
        })();
    });

    cf::get_recent_bloom_filter_false_ratio.set(r, [&ctx] (std::unique_ptr<request> req) {
        return make_sstable_stat(ctx, [] (sstables::sstable& sst) {
            return when_all(sst.filter_get_recent_false_positive(),
                            sst.filter_get_recent_true_positive()).then([] (auto futs) {
                double f = std::get<uint64_t>(std::get<0>(futs).get());
                double t = std::get<uint64_t>(std::get<1>(futs).get());

                return f / (f + t);
            });
        })(req->param["name"]);
    });

    cf::get_all_recent_bloom_filter_false_ratio.set(r, [&ctx] (std::unique_ptr<request> req) {
        return make_sstable_stat(ctx, [] (sstables::sstable& sst) {
            return when_all(sst.filter_get_recent_false_positive(),
                            sst.filter_get_recent_true_positive()).then([] (auto futs) {
                double f = std::get<uint64_t>(std::get<0>(futs).get());
                double t = std::get<uint64_t>(std::get<1>(futs).get());

                return f / (f + t);
            });
        })();
    });

    cf::get_bloom_filter_disk_space_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_bloom_filter_disk_space_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_bloom_filter_off_heap_memory_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_bloom_filter_off_heap_memory_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_index_summary_off_heap_memory_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_index_summary_off_heap_memory_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_compression_metadata_off_heap_memory_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_compression_metadata_off_heap_memory_used.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_speculative_retries.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_speculative_retries.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_key_cache_hit_rate.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_true_snapshots_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_true_snapshots_size.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_row_cache_hit_out_of_range.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_row_cache_hit_out_of_range.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_row_cache_hit.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_row_cache_hit.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_row_cache_miss.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_all_row_cache_miss.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_cas_prepare.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_cas_propose.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_cas_commit.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        return make_ready_future<json::json_return_type>(0);
    });

    cf::get_sstables_per_read_histogram.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        std::vector<double> res;
        return make_ready_future<json::json_return_type>(res);
    });

    cf::get_tombstone_scanned_histogram.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        std::vector<double> res;
        return make_ready_future<json::json_return_type>(res);
    });

    cf::get_live_scanned_histogram.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        std::vector<double> res;
        return make_ready_future<json::json_return_type>(res);
    });

    cf::get_col_update_time_delta_histogram.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        //auto id = get_uuid(req->param["name"], ctx.db.local());
        std::vector<double> res;
        return make_ready_future<json::json_return_type>(res);
    });

}
}
