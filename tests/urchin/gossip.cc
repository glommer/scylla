#include "core/reactor.hh"
#include "core/app-template.hh"
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "gms/application_state.hh"
#include "service/storage_service.hh"
#include <chrono>

namespace bpo = boost::program_options;

int main(int ac, char ** av) {
    app_template app;
    app.add_options()
        ("seed", bpo::value<std::vector<std::string>>(), "IP address of seed node")
        ("listen-address", bpo::value<std::string>()->default_value("0.0.0.0"), "IP address to listen");
    return app.run(ac, av, [&app] {
        auto config = app.configuration();
        const gms::inet_address listen = gms::inet_address(config["listen-address"].as<std::string>());
        service::init_storage_service().then([listen, config] {
            return net::get_messaging_service().start(listen);
        }).then([config] {
            auto& server = net::get_local_messaging_service();
            auto port = server.port();
            auto listen = server.listen_address();
            print("Messaging server listening on ip %s port %d ...\n", listen, port);
            return gms::get_failure_detector().start();
        }).then([config] {
            return gms::get_gossiper().start();
        }).then([config] {
            std::set<gms::inet_address> seeds;
            for (auto s : config["seed"].as<std::vector<std::string>>()) {
                seeds.emplace(std::move(s));
            }

            std::cout << "Start gossiper service ...\n";
            auto& gossiper = gms::get_local_gossiper();
            gossiper.set_seeds(std::move(seeds));

            std::map<gms::application_state, gms::versioned_value> app_states = {
                { gms::application_state::LOAD, gms::versioned_value::versioned_value_factory::load(0.5) },
            };

            using namespace std::chrono;
            auto now = high_resolution_clock::now().time_since_epoch();
            int generation_number = duration_cast<seconds>(now).count();
            return gossiper.start(generation_number, app_states);
        }).then([] () {
            auto reporter = std::make_shared<timer<lowres_clock>>();
            reporter->set_callback ([reporter] {
                auto& gossiper = gms::get_local_gossiper();
                gossiper.dump_endpoint_state_map();
                auto& fd = gms::get_local_failure_detector();
                print("%s", fd);
            });
            reporter->arm_periodic(std::chrono::milliseconds(1000));

            auto app_state_adder = std::make_shared<timer<lowres_clock>>();
            app_state_adder->set_callback ([app_state_adder] {
                static double load = 0.5;
                auto& gossiper = gms::get_local_gossiper();
                auto state = gms::application_state::LOAD;
                auto value = gms::versioned_value::versioned_value_factory::load(load);
                gossiper.add_local_application_state(state, value);
                load += 0.0001;
            });
            app_state_adder->arm_periodic(std::chrono::seconds(1));
        });
    });
}
