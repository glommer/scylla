/*
 * Copyright 2014 Cloudius Systems
 */


#include "database.hh"
#include "core/app-template.hh"
#include "core/distributed.hh"
#include "thrift/server.hh"
#include "transport/server.hh"
#include "http/httpd.hh"
#include "api/api.hh"
#include "db/config.hh"
#include "message/messaging_service.hh"
#include "service/storage_service.hh"
#include "db/system_keyspace.hh"
#include "dns.hh"
#include <cstdio>

namespace bpo = boost::program_options;

static future<>
read_config(bpo::variables_map& opts, db::config& cfg) {
    if (opts.count("options-file") == 0) {
        return make_ready_future<>();
    }
    return cfg.read_from_file(opts["options-file"].as<sstring>());
}

int main(int ac, char** av) {
    std::setvbuf(stdout, nullptr, _IOLBF, 1000);
    app_template app;
    auto opt_add = app.add_options();

    auto cfg = make_lw_shared<db::config>();
    cfg->add_options(opt_add)
        ("api-port", bpo::value<uint16_t>()->default_value(10000), "Http Rest API port")
        ("api-dir", bpo::value<sstring>()->default_value("swagger-ui/dist/"),
                "The directory location of the API GUI")
        // TODO : default, always read?
        ("options-file", bpo::value<sstring>(), "cassandra.yaml file to read options from")
        ;

    distributed<database> db;
    distributed<cql3::query_processor> qp;
    distributed<service::storage_proxy> proxy;
    api::http_context ctx(db);

    return app.run(ac, av, [&] {
        auto&& opts = app.configuration();

        return read_config(opts, *cfg).then([&cfg, &db, &qp, &proxy, &ctx, &opts]() {
            uint16_t thrift_port = cfg->rpc_port();
            uint16_t cql_port = cfg->native_transport_port();
            uint16_t api_port = opts["api-port"].as<uint16_t>();
            ctx.api_dir = opts["api-dir"].as<sstring>();
            sstring listen_address = cfg->listen_address();
            sstring rpc_address = cfg->rpc_address();
            auto seed_provider= cfg->seed_provider();
            using namespace locator;
            return i_endpoint_snitch::create_snitch(cfg->endpoint_snitch()).then(
                    [&db, cfg] () {
                return service::init_storage_service().then([&db, cfg] {
                    return db.start(std::move(*cfg)).then([&db] {
                        engine().at_exit([&db] {
                            return db.stop().then([] {
                                return i_endpoint_snitch::stop_snitch();
                            });
                        });
                    });
                });
            }).then([listen_address, seed_provider] {
                return net::init_messaging_service(listen_address, seed_provider);
            }).then([&proxy, &db] {
                return proxy.start(std::ref(db)).then([&proxy] {
                    engine().at_exit([&proxy] { return proxy.stop(); });
                });
            }).then([&db, &proxy, &qp] {
                return qp.start(std::ref(proxy), std::ref(db)).then([&qp] {
                    engine().at_exit([&qp] { return qp.stop(); });
                });
            }).then([&db, &proxy] {
                return db.invoke_on_all([&proxy] (database& db) {
                    return db.init_from_data_directory(proxy);
                });
            }).then([&db, &qp] {
                return db::system_keyspace::setup(db, qp);
            }).then([] {
                auto& ss = service::get_local_storage_service();
                return ss.init_server();
            }).then([rpc_address] {
                return dns::gethostbyname(rpc_address);
            }).then([&db, &proxy, &qp, cql_port, thrift_port] (dns::hostent e) {
                auto rpc_address = e.addresses[0].in.s_addr;
                auto cserver = new distributed<cql_server>;
                cserver->start(std::ref(proxy), std::ref(qp)).then([server = std::move(cserver), cql_port, rpc_address] () mutable {
                    server->invoke_on_all(&cql_server::listen, ipv4_addr{rpc_address, cql_port});
                }).then([cql_port] {
                    std::cout << "CQL server listening on port " << cql_port << " ...\n";
                });
                auto tserver = new distributed<thrift_server>;
                tserver->start(std::ref(db)).then([server = std::move(tserver), thrift_port, rpc_address] () mutable {
                    server->invoke_on_all(&thrift_server::listen, ipv4_addr{rpc_address, thrift_port});
                }).then([thrift_port] {
                    std::cout << "Thrift server listening on port " << thrift_port << " ...\n";
                });
            }).then([&db, api_port, &ctx]{
                ctx.http_server.start().then([api_port, &ctx] {
                    return set_server(ctx);
                }).then([&ctx, api_port] {
                    ctx.http_server.listen(api_port);
                }).then([api_port] {
                    std::cout << "Seastar HTTP server listening on port " << api_port << " ...\n";
                });
            }).or_terminate();
        });
    });
}
