/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#include "gms/inet_address.hh"
#include "gms/endpoint_state.hh"
#include "gms/gossip_digest.hh"
#include "gms/gossip_digest_syn.hh"
#include "gms/gossip_digest_ack.hh"
#include "gms/gossip_digest_ack2.hh"
#include "gms/versioned_value.hh"
#include "gms/gossiper.hh"
#include "gms/application_state.hh"
#include "gms/failure_detector.hh"
#include "gms/i_failure_detection_event_listener.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "gms/i_failure_detector.hh"
#include "service/storage_service.hh"
#include "message/messaging_service.hh"
#include "dht/i_partitioner.hh"
#include "log.hh"
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <chrono>

namespace gms {

using clk = gossiper::clk;

logging::logger logger("gossip");

constexpr std::chrono::milliseconds gossiper::INTERVAL;
constexpr std::chrono::hours gossiper::A_VERY_LONG_TIME;
constexpr int64_t gossiper::MAX_GENERATION_DIFFERENCE;

distributed<gossiper> _the_gossiper;

std::chrono::milliseconds gossiper::quarantine_delay() {
    return std::chrono::milliseconds(service::storage_service::RING_DELAY * 2);
}

static auto storage_service_ring_delay() {
    return std::chrono::milliseconds(service::storage_service::RING_DELAY);
}

auto& storage_service_value_factory() {
    return service::get_local_storage_service().value_factory;
}

gossiper::gossiper() {
    // Gossiper's stuff below runs only on CPU0
    if (engine().cpu_id() != 0) {
        return;
    }

    _scheduled_gossip_task.set_callback([this] { run(); });
    // half of QUARATINE_DELAY, to ensure _just_removed_endpoints has enough leeway to prevent re-gossip
    fat_client_timeout = quarantine_delay() / 2;
    /* register with the Failure Detector for receiving Failure detector events */
    get_local_failure_detector().register_failure_detection_event_listener(this);
    // Register this instance with JMX
}

void gossiper::set_last_processed_message_at() {
    set_last_processed_message_at(now());
}

void gossiper::set_last_processed_message_at(clk::time_point tp) {
    _last_processed_message_at = tp;
}

/*
 * First construct a map whose key is the endpoint in the GossipDigest and the value is the
 * GossipDigest itself. Then build a list of version differences i.e difference between the
 * version in the GossipDigest and the version in the local state for a given InetAddress.
 * Sort this list. Now loop through the sorted list and retrieve the GossipDigest corresponding
 * to the endpoint from the map that was initially constructed.
*/
void gossiper::do_sort(std::vector<gossip_digest>& g_digest_list) {
    /* Construct a map of endpoint to GossipDigest. */
    std::map<inet_address, gossip_digest> ep_to_digest_map;
    for (auto g_digest : g_digest_list) {
        ep_to_digest_map.emplace(g_digest.get_endpoint(), g_digest);
    }

    /*
     * These digests have their maxVersion set to the difference of the version
     * of the local EndpointState and the version found in the GossipDigest.
    */
    std::vector<gossip_digest> diff_digests;
    for (auto g_digest : g_digest_list) {
        auto ep = g_digest.get_endpoint();
        auto ep_state = this->get_endpoint_state_for_endpoint(ep);
        int version = ep_state ? this->get_max_endpoint_state_version(*ep_state) : 0;
        int diff_version = ::abs(version - g_digest.get_max_version());
        diff_digests.emplace_back(gossip_digest(ep, g_digest.get_generation(), diff_version));
    }

    g_digest_list.clear();
    std::sort(diff_digests.begin(), diff_digests.end());
    int size = diff_digests.size();
    /*
     * Report the digests in descending order. This takes care of the endpoints
     * that are far behind w.r.t this local endpoint
    */
    for (int i = size - 1; i >= 0; --i) {
        g_digest_list.emplace_back(ep_to_digest_map[diff_digests[i].get_endpoint()]);
    }
}

future<gossip_digest_ack> gossiper::handle_syn_msg(gossip_digest_syn syn_msg) {
    this->set_last_processed_message_at();
    inet_address from;
    if (!this->is_enabled()) {
        return make_ready_future<gossip_digest_ack>(gossip_digest_ack());
    }

    /* If the message is from a different cluster throw it away. */
    if (syn_msg.cluster_id() != get_cluster_name()) {
        return make_ready_future<gossip_digest_ack>(gossip_digest_ack());
    }

    if (syn_msg.partioner() != "" && syn_msg.partioner() != get_partitioner_name()) {
        return make_ready_future<gossip_digest_ack>(gossip_digest_ack());
    }

    auto g_digest_list = syn_msg.get_gossip_digests();
    do_sort(g_digest_list);
    std::vector<gossip_digest> delta_gossip_digest_list;
    std::map<inet_address, endpoint_state> delta_ep_state_map;
    this->examine_gossiper(g_digest_list, delta_gossip_digest_list, delta_ep_state_map);
    gms::gossip_digest_ack ack_msg(std::move(delta_gossip_digest_list), std::move(delta_ep_state_map));
    return make_ready_future<gossip_digest_ack>(std::move(ack_msg));
}

future<> gossiper::handle_ack_msg(shard_id id, gossip_digest_ack ack_msg) {
    this->set_last_processed_message_at();
    if (!this->is_enabled() && !this->is_in_shadow_round()) {
        return make_ready_future<>();
    }

    auto g_digest_list = ack_msg.get_gossip_digest_list();
    auto ep_state_map = ack_msg.get_endpoint_state_map();

    auto f = make_ready_future<>();
    if (ep_state_map.size() > 0) {
        /* Notify the Failure Detector */
        this->notify_failure_detector(ep_state_map);
        f = this->apply_state_locally(ep_state_map);
    }

    return f.then([id, g_digest_list = std::move(g_digest_list), this] {
        if (this->is_in_shadow_round()) {
            this->finish_shadow_round();
            // don't bother doing anything else, we have what we came for
            return make_ready_future<>();
        }
        /* Get the state required to send to this gossipee - construct GossipDigestAck2Message */
        std::map<inet_address, endpoint_state> delta_ep_state_map;
        for (auto g_digest : g_digest_list) {
            inet_address addr = g_digest.get_endpoint();
            auto local_ep_state_ptr = this->get_state_for_version_bigger_than(addr, g_digest.get_max_version());
            if (local_ep_state_ptr) {
                delta_ep_state_map.emplace(addr, *local_ep_state_ptr);
            }
        }
        gms::gossip_digest_ack2 ack2_msg(std::move(delta_ep_state_map));
        logger.trace("Sending a GossipDigestACK2 to {}", id);
        return this->ms().send_gossip_digest_ack2(id, std::move(ack2_msg)).then_wrapped([id] (auto&& f) {
            try {
                f.get();
                logger.trace("Got GossipDigestACK2 Reply");
            } catch (...) {
                logger.warn("Fail to send GossipDigestACK2 to {}: {}", id, std::current_exception());
            }
            return make_ready_future<>();
        });
    });
}

void gossiper::init_messaging_service_handler() {
    ms().register_echo([] {
        return smp::submit_to(0, [] {
            auto& gossiper = gms::get_local_gossiper();
            gossiper.set_last_processed_message_at();
            return make_ready_future<>();
        });
    });
    ms().register_gossip_shutdown([] (inet_address from) {
        smp::submit_to(0, [from] {
            auto& gossiper = gms::get_local_gossiper();
            gossiper.set_last_processed_message_at();
            if (!gossiper.is_enabled()) {
                logger.debug("Ignoring shutdown message from {} because gossip is disabled", from);
                return make_ready_future<>();
            }
            return seastar::async([from, fd = get_local_failure_detector().shared_from_this()] {
                fd->force_conviction(from);
            });
        }).handle_exception([] (auto ep) {
            logger.warn("Fail to handle GOSSIP_SHUTDOWN: {}", ep);
        });
        return messaging_service::no_wait();
    });
    ms().register_gossip_digest_syn([] (gossip_digest_syn syn_msg) {
        return smp::submit_to(0, [syn_msg = std::move(syn_msg)] () mutable {
            auto& gossiper = gms::get_local_gossiper();
            return gossiper.handle_syn_msg(std::move(syn_msg));
        });
    });
    ms().register_gossip_digest_ack2([] (gossip_digest_ack2 msg) {
        smp::submit_to(0, [msg = std::move(msg)] () mutable {
            auto& gossiper = gms::get_local_gossiper();
            gossiper.set_last_processed_message_at();
            auto& remote_ep_state_map = msg.get_endpoint_state_map();
            /* Notify the Failure Detector */
            gossiper.notify_failure_detector(remote_ep_state_map);
            return gossiper.apply_state_locally(remote_ep_state_map);
        }).handle_exception([] (auto ep) {
            logger.warn("Fail to handle GOSSIP_DIGEST_ACK: {}", ep);
        });
        return messaging_service::no_wait();
    });
}

future<bool> gossiper::send_gossip(gossip_digest_syn message, std::set<inet_address> epset) {
    std::vector<inet_address> __live_endpoints(epset.begin(), epset.end());
    size_t size = __live_endpoints.size();
    if (size < 1) {
        return make_ready_future<bool>(false);
    }
    /* Generate a random number from 0 -> size */
    std::uniform_int_distribution<int> dist(0, size - 1);
    int index = dist(_random);
    inet_address to = __live_endpoints[index];
    auto id = get_shard_id(to);
    logger.trace("Sending a GossipDigestSyn to {} ...", id);
    return ms().send_gossip_digest_syn(id, std::move(message)).then_wrapped([this, id] (auto&& f) {
        try {
            auto ack_msg = f.get0();
            logger.trace("Got GossipDigestSyn Reply");
            return this->handle_ack_msg(id, std::move(ack_msg));
        } catch (...) {
            // It is normal to reach here because it is normal that a node
            // tries to send a SYN message to a peer node which is down before
            // failure_detector thinks that peer node is down.
            logger.trace("Fail to send GossipDigestSyn to {}: {}", id, std::current_exception());
        }
        return make_ready_future<>();
    }).then([this, to] {
        return make_ready_future<bool>(_seeds.count(to));
    });
}


void gossiper::notify_failure_detector(inet_address endpoint, endpoint_state remote_endpoint_state) {
    /*
     * If the local endpoint state exists then report to the FD only
     * if the versions workout.
    */
    auto it = endpoint_state_map.find(endpoint);
    if (it != endpoint_state_map.end()) {
        auto& local_endpoint_state = it->second;
        i_failure_detector& fd = get_local_failure_detector();
        int local_generation = local_endpoint_state.get_heart_beat_state().get_generation();
        int remote_generation = remote_endpoint_state.get_heart_beat_state().get_generation();
        if (remote_generation > local_generation) {
            local_endpoint_state.update_timestamp();
            // this node was dead and the generation changed, this indicates a reboot, or possibly a takeover
            // we will clean the fd intervals for it and relearn them
            if (!local_endpoint_state.is_alive()) {
                logger.debug("Clearing interval times for {} due to generation change", endpoint);
                fd.remove(endpoint);
            }
            fd.report(endpoint);
            return;
        }

        if (remote_generation == local_generation) {
            int local_version = get_max_endpoint_state_version(local_endpoint_state);
            int remote_version = remote_endpoint_state.get_heart_beat_state().get_heart_beat_version();
            if (remote_version > local_version) {
                local_endpoint_state.update_timestamp();
                // just a version change, report to the fd
                fd.report(endpoint);
            }
        }
    }
}

future<> gossiper::apply_state_locally(std::map<inet_address, endpoint_state>& map) {
    return seastar::async([this, g = this->shared_from_this(), map = std::move(map)] () mutable {
        for (auto& entry : map) {
            auto& ep = entry.first;
            if (ep == get_broadcast_address() && !is_in_shadow_round()) {
                continue;
            }
            if (_just_removed_endpoints.count(ep)) {
                logger.trace("Ignoring gossip for {} because it is quarantined", ep);
                continue;
            }
            /*
               If state does not exist just add it. If it does then add it if the remote generation is greater.
               If there is a generation tie, attempt to break it by heartbeat version.
               */
            endpoint_state& remote_state = entry.second;
            auto it = endpoint_state_map.find(ep);
            if (it != endpoint_state_map.end()) {
                endpoint_state& local_ep_state_ptr = it->second;
                int local_generation = local_ep_state_ptr.get_heart_beat_state().get_generation();
                int remote_generation = remote_state.get_heart_beat_state().get_generation();
                logger.trace("{} local generation {}, remote generation {}", ep, local_generation, remote_generation);
                // }
                if (local_generation != 0 && remote_generation > local_generation + MAX_GENERATION_DIFFERENCE) {
                    // assume some peer has corrupted memory and is broadcasting an unbelievable generation about another peer (or itself)
                    logger.warn("received an invalid gossip generation for peer {}; local generation = {}, received generation = {}",
                        ep, local_generation, remote_generation);
                } else if (remote_generation > local_generation) {
                    logger.trace("Updating heartbeat state generation to {} from {} for {}", remote_generation, local_generation, ep);
                    // major state change will handle the update by inserting the remote state directly
                    handle_major_state_change(ep, remote_state);
                } else if (remote_generation == local_generation) {  //generation has not changed, apply new states
                    /* find maximum state */
                    int local_max_version = get_max_endpoint_state_version(local_ep_state_ptr);
                    int remote_max_version = get_max_endpoint_state_version(remote_state);
                    if (remote_max_version > local_max_version) {
                        // apply states, but do not notify since there is no major change
                        apply_new_states(ep, local_ep_state_ptr, remote_state);
                    } else {
                        logger.trace("Ignoring remote version {} <= {} for {}", remote_max_version, local_max_version, ep);
                    }
                    if (!local_ep_state_ptr.is_alive() && !is_dead_state(local_ep_state_ptr)) { // unless of course, it was dead
                        mark_alive(ep, local_ep_state_ptr);
                    }
                } else {
                    logger.trace("Ignoring remote generation {} < {}", remote_generation, local_generation);
                }
            } else {
                // this is a new node, report it to the FD in case it is the first time we are seeing it AND it's not alive
                get_local_failure_detector().report(ep);
                handle_major_state_change(ep, remote_state);
            }
        }
    });
}

// Runs inside seastar::async context
void gossiper::remove_endpoint(inet_address endpoint) {
    // do subscribers first so anything in the subscriber that depends on gossiper state won't get confused
    for (auto& subscriber : _subscribers) {
        subscriber->on_remove(endpoint);
    }

    if(_seeds.count(endpoint)) {
        build_seeds_list();
        _seeds.erase(endpoint);
        // logger.info("removed {} from _seeds, updated _seeds list = {}", endpoint, _seeds);
    }

    _live_endpoints.erase(endpoint);
    _unreachable_endpoints.erase(endpoint);
    // do not remove endpointState until the quarantine expires
    get_local_failure_detector().remove(endpoint);
    quarantine_endpoint(endpoint);
    logger.debug("removing endpoint {}", endpoint);
}

// Runs inside seastar::async context
void gossiper::do_status_check() {
    logger.trace("Performing status check ...");

    auto now = this->now();

    auto fd = get_local_failure_detector().shared_from_this();
    for (auto it = endpoint_state_map.begin(); it != endpoint_state_map.end();) {
        auto endpoint = it->first;
        auto& ep_state = it->second;
        it++;

        bool is_alive = ep_state.is_alive();
        if (endpoint == get_broadcast_address()) {
            continue;
        }

        fd->interpret(endpoint);

        // check if this is a fat client. fat clients are removed automatically from
        // gossip after FatClientTimeout.  Do not remove dead states here.
        if (is_gossip_only_member(endpoint)
            && !_just_removed_endpoints.count(endpoint)
            && ((now - ep_state.get_update_timestamp()) > fat_client_timeout)) {
            logger.info("FatClient {} has been silent for {}ms, removing from gossip", endpoint, fat_client_timeout.count());
            remove_endpoint(endpoint); // will put it in _just_removed_endpoints to respect quarantine delay
            evict_from_membership(endpoint); // can get rid of the state immediately
        }

        // check for dead state removal
        auto expire_time = get_expire_time_for_endpoint(endpoint);
        if (!is_alive && (now > expire_time)
             && (!service::get_local_storage_service().get_token_metadata().is_member(endpoint))) {
            logger.debug("time is expiring for endpoint : {} ({})", endpoint, expire_time.time_since_epoch().count());
            evict_from_membership(endpoint);
        }
    }

    for (auto it = _just_removed_endpoints.begin(); it != _just_removed_endpoints.end();) {
        auto& t= it->second;
        if ((now - t) > quarantine_delay()) {
            logger.debug("{} ms elapsed, {} gossip quarantine over", quarantine_delay().count(), it->first);
            it = _just_removed_endpoints.erase(it);
        } else {
            it++;
        }
    }
}

void gossiper::run() {
    seastar::async([this, g = this->shared_from_this()] {
        logger.trace("=== Gossip round START");

        //wait on messaging service to start listening
        // MessagingService.instance().waitUntilListening();

        /* Update the local heartbeat counter. */
        auto br_addr = get_broadcast_address();
        heart_beat_state& hbs = endpoint_state_map[br_addr].get_heart_beat_state();
        hbs.update_heart_beat();

        //
        // We don't care about heart_beat change on other CPUs - so ingnore this
        // specific change.
        //
        _shadow_endpoint_state_map[br_addr].set_heart_beat_state(hbs);

        logger.trace("My heartbeat is now {}", endpoint_state_map[br_addr].get_heart_beat_state().get_heart_beat_version());
        std::vector<gossip_digest> g_digests;
        this->make_random_gossip_digest(g_digests);

        if (g_digests.size() > 0) {
            gossip_digest_syn message(get_cluster_name(), get_partitioner_name(), g_digests);

            /* Gossip to some random live member */
            bool gossiped_to_seed = std::get<0>(do_gossip_to_live_member(message).get());

            /* Gossip to some unreachable member with some probability to check if he is back up */
            do_gossip_to_unreachable_member(message).get();

            /* Gossip to a seed if we did not do so above, or we have seen less nodes
               than there are seeds.  This prevents partitions where each group of nodes
               is only gossiping to a subset of the seeds.

               The most straightforward check would be to check that all the seeds have been
               verified either as live or unreachable.  To avoid that computation each round,
               we reason that:

               either all the live nodes are seeds, in which case non-seeds that come online
               will introduce themselves to a member of the ring by definition,

               or there is at least one non-seed node in the list, in which case eventually
               someone will gossip to it, and then do a gossip to a random seed from the
               gossipedToSeed check.

               See CASSANDRA-150 for more exposition. */
            if (!gossiped_to_seed || _live_endpoints.size() < _seeds.size()) {
                do_gossip_to_seed(message).get();
            }

            do_status_check();
        }

        //
        // Gossiper task runs only on CPU0:
        //
        //    - If endpoint_state_map or _live_endpoints have changed - duplicate
        //      them across all other shards.
        //    - Reschedule the gossiper only after execution on all nodes is done.
        //
        bool endpoint_map_changed = (_shadow_endpoint_state_map != endpoint_state_map);
        bool live_endpoint_changed = (_live_endpoints != _shadow_live_endpoints);

        if (endpoint_map_changed || live_endpoint_changed) {
            if (endpoint_map_changed) {
                _shadow_endpoint_state_map = endpoint_state_map;
            }

            if (live_endpoint_changed) {
                _shadow_live_endpoints = _live_endpoints;
            }

            _the_gossiper.invoke_on_all([this, endpoint_map_changed,
                live_endpoint_changed] (gossiper& local_gossiper) {
                // Don't copy gossiper(CPU0) maps into themselves!
                if (engine().cpu_id() != 0) {
                    if (endpoint_map_changed) {
                        local_gossiper.endpoint_state_map = _shadow_endpoint_state_map;
                    }

                    if (live_endpoint_changed) {
                        local_gossiper._live_endpoints = _shadow_live_endpoints;
                    }
                }
            }).get();
        }
    }).then_wrapped([this] (auto&& f) {
        try {
            f.get();
            logger.trace("=== Gossip round OK");
        } catch (...) {
            logger.trace("=== Gossip round FAIL");
        }

        if (logger.is_enabled(logging::log_level::debug)) {
            for (auto& x : endpoint_state_map) {
                logger.debug("ep={}, eps={}", x.first, x.second);
            }
        }
        _scheduled_gossip_task.arm(INTERVAL);
    });
}

bool gossiper::seen_any_seed() {
    for (auto& entry : endpoint_state_map) {
        if (_seeds.count(entry.first)) {
            return true;
        }
        auto& state = entry.second;
        if (state.get_application_state_map().count(application_state::INTERNAL_IP) &&
                _seeds.count(inet_address(state.get_application_state(application_state::INTERNAL_IP)->value))) {
            return true;
        }
    }
    return false;
}

void gossiper::register_(i_endpoint_state_change_subscriber* subscriber) {
    _subscribers.push_back(subscriber);
}

void gossiper::unregister_(i_endpoint_state_change_subscriber* subscriber) {
    _subscribers.remove(subscriber);
}

std::set<inet_address> gossiper::get_live_members() {
    std::set<inet_address> live_members(_live_endpoints);
    if (!live_members.count(get_broadcast_address())) {
        live_members.insert(get_broadcast_address());
    }
    return live_members;
}

std::set<inet_address> gossiper::get_live_token_owners() {
    std::set<inet_address> token_owners;
    for (auto& member : get_live_members()) {
        auto it = endpoint_state_map.find(member);
        if (it != endpoint_state_map.end() && !is_dead_state(it->second) && service::get_local_storage_service().get_token_metadata().is_member(member)) {
            token_owners.insert(member);
        }
    }
    return token_owners;
}

std::set<inet_address> gossiper::get_unreachable_token_owners() {
    std::set<inet_address> token_owners;
    for (auto&& x : _unreachable_endpoints) {
        auto& endpoint = x.first;
        if (service::get_local_storage_service().get_token_metadata().is_member(endpoint)) {
            token_owners.insert(endpoint);
        }
    }
    return token_owners;
}

// Return downtime in microseconds
int64_t gossiper::get_endpoint_downtime(inet_address ep) {
    auto it = _unreachable_endpoints.find(ep);
    if (it != _unreachable_endpoints.end()) {
        auto& downtime = it->second;
        return std::chrono::duration_cast<std::chrono::microseconds>(now() - downtime).count();
    } else {
        return 0L;
    }
}

// Runs inside seastar::async context
void gossiper::convict(inet_address endpoint, double phi) {
    auto it = endpoint_state_map.find(endpoint);
    if (it == endpoint_state_map.end()) {
        return;
    }
    auto& state = it->second;
    logger.trace("convict ep={}, phi={}, is_alive={}, is_dead_state={}", endpoint, phi, state.is_alive(), is_dead_state(state));
    if (state.is_alive() && !is_dead_state(state)) {
        mark_dead(endpoint, state);
    } else {
        state.mark_dead();
    }
}

std::set<inet_address> gossiper::get_unreachable_members() {
    std::set<inet_address> ret;
    for (auto&& x : _unreachable_endpoints) {
        ret.insert(x.first);
    }
    return ret;
}

int gossiper::get_max_endpoint_state_version(endpoint_state state) {
    int max_version = state.get_heart_beat_state().get_heart_beat_version();
    for (auto& entry : state.get_application_state_map()) {
        auto& value = entry.second;
        max_version = std::max(max_version, value.version);
    }
    return max_version;
}

void gossiper::evict_from_membership(inet_address endpoint) {
    _unreachable_endpoints.erase(endpoint);
    endpoint_state_map.erase(endpoint);
    _expire_time_endpoint_map.erase(endpoint);
    quarantine_endpoint(endpoint);
    logger.debug("evicting {} from gossip", endpoint);
}

void gossiper::quarantine_endpoint(inet_address endpoint) {
    quarantine_endpoint(endpoint, now());
}

void gossiper::quarantine_endpoint(inet_address endpoint, clk::time_point quarantine_expiration) {
    _just_removed_endpoints[endpoint] = quarantine_expiration;
}

void gossiper::replacement_quarantine(inet_address endpoint) {
    // remember, quarantine_endpoint will effectively already add QUARANTINE_DELAY, so this is 2x
    // logger.debug("");
    quarantine_endpoint(endpoint, now() + quarantine_delay());
}

// Runs inside seastar::async context
void gossiper::replaced_endpoint(inet_address endpoint) {
    remove_endpoint(endpoint);
    evict_from_membership(endpoint);
    replacement_quarantine(endpoint);
}

void gossiper::make_random_gossip_digest(std::vector<gossip_digest>& g_digests) {
    int generation = 0;
    int max_version = 0;

    // local epstate will be part of endpoint_state_map
    std::vector<inet_address> endpoints;
    for (auto&& x : endpoint_state_map) {
        endpoints.push_back(x.first);
    }
    std::random_shuffle(endpoints.begin(), endpoints.end());
    for (auto& endpoint : endpoints) {
        auto it = endpoint_state_map.find(endpoint);
        if (it != endpoint_state_map.end()) {
            auto& eps = it->second;
            generation = eps.get_heart_beat_state().get_generation();
            max_version = get_max_endpoint_state_version(eps);
        }
        g_digests.push_back(gossip_digest(endpoint, generation, max_version));
    }
#if 0
    if (logger.isTraceEnabled()) {
        StringBuilder sb = new StringBuilder();
        for (GossipDigest g_digest : g_digests)
        {
            sb.append(g_digest);
            sb.append(" ");
        }
        logger.trace("Gossip Digests are : {}", sb);
    }
#endif
}

future<> gossiper::advertise_removing(inet_address endpoint, utils::UUID host_id, utils::UUID local_host_id) {
    return seastar::async([this, g = this->shared_from_this(), endpoint, host_id, local_host_id] {
        auto& state = endpoint_state_map.at(endpoint);
        // remember this node's generation
        int generation = state.get_heart_beat_state().get_generation();
        logger.info("Removing host: {}", host_id);
        logger.info("Sleeping for {}ms to ensure {} does not change", service::storage_service::RING_DELAY, endpoint);
        sleep(storage_service_ring_delay()).get();
        // make sure it did not change
        auto& eps = endpoint_state_map.at(endpoint);
        if (eps.get_heart_beat_state().get_generation() != generation) {
            throw std::runtime_error(sprint("Endpoint %s generation changed while trying to remove it", endpoint));
        }

        // update the other node's generation to mimic it as if it had changed it itself
        logger.info("Advertising removal for {}", endpoint);
        eps.update_timestamp(); // make sure we don't evict it too soon
        eps.get_heart_beat_state().force_newer_generation_unsafe();
        eps.add_application_state(application_state::STATUS, storage_service_value_factory().removing_nonlocal(host_id));
        eps.add_application_state(application_state::REMOVAL_COORDINATOR, storage_service_value_factory().removal_coordinator(local_host_id));
        endpoint_state_map[endpoint] = eps;
    });
}

future<> gossiper::advertise_token_removed(inet_address endpoint, utils::UUID host_id) {
    return seastar::async([this, g = this->shared_from_this(), endpoint, host_id] {
        auto& eps = endpoint_state_map.at(endpoint);
        eps.update_timestamp(); // make sure we don't evict it too soon
        eps.get_heart_beat_state().force_newer_generation_unsafe();
        auto expire_time = compute_expire_time();
        eps.add_application_state(application_state::STATUS, storage_service_value_factory().removed_nonlocal(host_id, expire_time.time_since_epoch().count()));
        logger.info("Completing removal of {}", endpoint);
        add_expire_time_for_endpoint(endpoint, expire_time);
        endpoint_state_map[endpoint] = eps;
        // ensure at least one gossip round occurs before returning
        sleep(INTERVAL * 2).get();
    });
}

future<> gossiper::unsafe_assassinate_endpoint(sstring address) {
    logger.warn("Gossiper.unsafeAssassinateEndpoint is deprecated and will be removed in the next release; use assassinate_endpoint instead");
    return assassinate_endpoint(address);
}

future<> gossiper::assassinate_endpoint(sstring address) {
    return seastar::async([this, g = this->shared_from_this(), address] {
        inet_address endpoint(address);
        auto is_exist = endpoint_state_map.count(endpoint);
        int gen = std::chrono::duration_cast<std::chrono::seconds>((now() + std::chrono::seconds(60)).time_since_epoch()).count();
        int ver = 9999;
        endpoint_state&& ep_state = is_exist ? endpoint_state_map.at(endpoint) :
                                               endpoint_state(heart_beat_state(gen, ver));
        std::vector<dht::token> tokens;
        logger.warn("Assassinating {} via gossip", endpoint);
        if (is_exist) {
            auto& ss = service::get_local_storage_service();
            auto tokens = ss.get_token_metadata().get_tokens(endpoint);
            if (tokens.empty()) {
                logger.warn("Unable to calculate tokens for {}.  Will use a random one", address);
                throw std::runtime_error(sprint("Unable to calculate tokens for %s", endpoint));
            }

            int generation = ep_state.get_heart_beat_state().get_generation();
            int heartbeat = ep_state.get_heart_beat_state().get_heart_beat_version();
            logger.info("Sleeping for {} ms to ensure {} does not change", service::storage_service::RING_DELAY, endpoint);
            // make sure it did not change
            sleep(storage_service_ring_delay()).get();

            auto it = endpoint_state_map.find(endpoint);
            if (it == endpoint_state_map.end()) {
                logger.warn("Endpoint {} disappeared while trying to assassinate, continuing anyway", endpoint);
            } else {
                auto& new_state = it->second;
                if (new_state.get_heart_beat_state().get_generation() != generation) {
                    throw std::runtime_error(sprint("Endpoint still alive: %s generation changed while trying to assassinate it", endpoint));
                } else if (new_state.get_heart_beat_state().get_heart_beat_version() != heartbeat) {
                    throw std::runtime_error(sprint("Endpoint still alive: %s heartbeat changed while trying to assassinate it", endpoint));
                }
            }
            ep_state.update_timestamp(); // make sure we don't evict it too soon
            ep_state.get_heart_beat_state().force_newer_generation_unsafe();
        }

        // do not pass go, do not collect 200 dollars, just gtfo
        std::unordered_set<dht::token> tokens_set(tokens.begin(), tokens.end());
        auto expire_time = compute_expire_time();
        ep_state.add_application_state(application_state::STATUS, storage_service_value_factory().left(tokens_set, expire_time.time_since_epoch().count()));
        handle_major_state_change(endpoint, ep_state);
        sleep(INTERVAL * 4).get();
        logger.warn("Finished assassinating {}", endpoint);
    });
}

bool gossiper::is_known_endpoint(inet_address endpoint) {
    return endpoint_state_map.count(endpoint);
}

int gossiper::get_current_generation_number(inet_address endpoint) {
    return endpoint_state_map.at(endpoint).get_heart_beat_state().get_generation();
}

future<bool> gossiper::do_gossip_to_live_member(gossip_digest_syn message) {
    size_t size = _live_endpoints.size();
    if (size == 0) {
        return make_ready_future<bool>(false);
    }
    logger.trace("do_gossip_to_live_member: live_endpoint nr={}", _live_endpoints.size());
    return send_gossip(message, _live_endpoints);
}

future<> gossiper::do_gossip_to_unreachable_member(gossip_digest_syn message) {
    double live_endpoint_count = _live_endpoints.size();
    double unreachable_endpoint_count = _unreachable_endpoints.size();
    if (unreachable_endpoint_count > 0) {
        /* based on some probability */
        double prob = unreachable_endpoint_count / (live_endpoint_count + 1);
        std::uniform_real_distribution<double> dist(0, 1);
        double rand_dbl = dist(_random);
        if (rand_dbl < prob) {
            std::set<inet_address> addrs;
            for (auto&& x : _unreachable_endpoints) {
                addrs.insert(x.first);
            }
            logger.trace("do_gossip_to_unreachable_member: live_endpoint nr={} unreachable_endpoints nr={}",
                live_endpoint_count, unreachable_endpoint_count);
            return send_gossip(message, addrs).discard_result();
        }
    }
    return make_ready_future<>();
}

future<> gossiper::do_gossip_to_seed(gossip_digest_syn prod) {
    size_t size = _seeds.size();
    if (size > 0) {
        if (size == 1 && _seeds.count(get_broadcast_address())) {
            return make_ready_future<>();
        }

        if (_live_endpoints.size() == 0) {
            logger.trace("do_gossip_to_seed: live_endpoints nr={}, seeds nr={}", 0, _seeds.size());
            return send_gossip(prod, _seeds).discard_result();
        } else {
            /* Gossip with the seed with some probability. */
            double probability = _seeds.size() / (double) (_live_endpoints.size() + _unreachable_endpoints.size());
            std::uniform_real_distribution<double> dist(0, 1);
            double rand_dbl = dist(_random);
            if (rand_dbl <= probability) {
                logger.trace("do_gossip_to_seed: live_endpoints nr={}, seeds nr={}", _live_endpoints.size(), _seeds.size());
                return send_gossip(prod, _seeds).discard_result();
            }
        }
    }
    return make_ready_future<>();
}

bool gossiper::is_gossip_only_member(inet_address endpoint) {
    auto it = endpoint_state_map.find(endpoint);
    if (it == endpoint_state_map.end()) {
        return false;
    }
    auto& eps = it->second;
    auto& ss = service::get_local_storage_service();
    return !is_dead_state(eps) && !ss.get_token_metadata().is_member(endpoint);
}

clk::time_point gossiper::get_expire_time_for_endpoint(inet_address endpoint) {
    /* default expire_time is A_VERY_LONG_TIME */
    auto it = _expire_time_endpoint_map.find(endpoint);
    if (it == _expire_time_endpoint_map.end()) {
        return compute_expire_time();
    } else {
        auto stored_time = it->second;
        return stored_time;
    }
}

std::experimental::optional<endpoint_state> gossiper::get_endpoint_state_for_endpoint(inet_address ep) {
    auto it = endpoint_state_map.find(ep);
    if (it == endpoint_state_map.end()) {
        return {};
    } else {
        return it->second;
    }
}

void gossiper::reset_endpoint_state_map() {
    endpoint_state_map.clear();
    _unreachable_endpoints.clear();
    _live_endpoints.clear();
}

std::unordered_map<inet_address, endpoint_state>& gms::gossiper::get_endpoint_states() {
    return endpoint_state_map;
}

bool gossiper::uses_host_id(inet_address endpoint) {
    if (net::get_local_messaging_service().knows_version(endpoint)) {
        return true;
    } else if (get_endpoint_state_for_endpoint(endpoint)->get_application_state(application_state::NET_VERSION)) {
        return true;
    }
    return false;
}

bool gossiper::uses_vnodes(inet_address endpoint) {
    return uses_host_id(endpoint) && get_endpoint_state_for_endpoint(endpoint)->get_application_state(application_state::TOKENS);
}

utils::UUID gossiper::get_host_id(inet_address endpoint) {
    if (!uses_host_id(endpoint)) {
        throw std::runtime_error(sprint("Host %s does not use new-style tokens!", endpoint));
    }
    sstring uuid = get_endpoint_state_for_endpoint(endpoint)->get_application_state(application_state::HOST_ID)->value;
    return utils::UUID(uuid);
}

std::experimental::optional<endpoint_state> gossiper::get_state_for_version_bigger_than(inet_address for_endpoint, int version) {
    std::experimental::optional<endpoint_state> reqd_endpoint_state;
    auto it = endpoint_state_map.find(for_endpoint);
    if (it != endpoint_state_map.end()) {
        auto& eps = it->second;
        /*
             * Here we try to include the Heart Beat state only if it is
             * greater than the version passed in. It might happen that
             * the heart beat version maybe lesser than the version passed
             * in and some application state has a version that is greater
             * than the version passed in. In this case we also send the old
             * heart beat and throw it away on the receiver if it is redundant.
            */
        int local_hb_version = eps.get_heart_beat_state().get_heart_beat_version();
        if (local_hb_version > version) {
            reqd_endpoint_state.emplace(eps.get_heart_beat_state());
            logger.trace("local heartbeat version {} greater than {} for {}", local_hb_version, version, for_endpoint);
        }
        /* Accumulate all application states whose versions are greater than "version" variable */
        for (auto& entry : eps.get_application_state_map()) {
            auto& value = entry.second;
            if (value.version > version) {
                if (!reqd_endpoint_state) {
                    reqd_endpoint_state.emplace(eps.get_heart_beat_state());
                }
                auto& key = entry.first;
                // FIXME: Add operator<< for application_state
                logger.trace("Adding state {}: {}" , int(key), value.value);
                reqd_endpoint_state->add_application_state(key, value);
            }
        }
    }
    return reqd_endpoint_state;
}

int gossiper::compare_endpoint_startup(inet_address addr1, inet_address addr2) {
    auto ep1 = get_endpoint_state_for_endpoint(addr1);
    auto ep2 = get_endpoint_state_for_endpoint(addr2);
    assert(ep1 && ep2);
    return ep1->get_heart_beat_state().get_generation() - ep2->get_heart_beat_state().get_generation();
}

void gossiper::notify_failure_detector(std::map<inet_address, endpoint_state> remoteEpStateMap) {
    for (auto& entry : remoteEpStateMap) {
        notify_failure_detector(entry.first, entry.second);
    }
}

// Runs inside seastar::async context
void gossiper::mark_alive(inet_address addr, endpoint_state local_state) {
    // if (MessagingService.instance().getVersion(addr) < MessagingService.VERSION_20) {
    //     real_mark_alive(addr, local_state);
    //     return;
    // }

    local_state.mark_dead();
    shard_id id = get_shard_id(addr);
    logger.trace("Sending a EchoMessage to {}", id);
    auto ok = make_shared<bool>(false);
    // FIXME: Add timeout
    ms().send_echo(id).then_wrapped([this, id, local_state = std::move(local_state), ok] (auto&& f) mutable {
        try {
            f.get();
            logger.trace("Got EchoMessage Reply");
            *ok = true;
        } catch (...) {
            logger.warn("Fail to send EchoMessage to {}: {}", id, std::current_exception());
        }
        return make_ready_future<>();
    }).get();

    if (*ok) {
        this->set_last_processed_message_at();
        this->real_mark_alive(id.addr, local_state);
    }
}

// Runs inside seastar::async context
void gossiper::real_mark_alive(inet_address addr, endpoint_state local_state) {
    logger.trace("marking as alive {}", addr);
    local_state.mark_alive();
    local_state.update_timestamp(); // prevents do_status_check from racing us and evicting if it was down > A_VERY_LONG_TIME
    _live_endpoints.insert(addr);
    _unreachable_endpoints.erase(addr);
    _expire_time_endpoint_map.erase(addr);
    logger.debug("removing expire time for endpoint : {}", addr);
    logger.info("inet_address {} is now UP", addr);
    for (auto& subscriber : _subscribers) {
        subscriber->on_alive(addr, local_state);
        logger.trace("Notified {}", subscriber);
    }
}

// Runs inside seastar::async context
void gossiper::mark_dead(inet_address addr, endpoint_state& local_state) {
    logger.trace("marking as down {}", addr);
    local_state.mark_dead();
    _live_endpoints.erase(addr);
    _unreachable_endpoints[addr] = now();
    logger.info("inet_address {} is now DOWN", addr);
    for (auto& subscriber : _subscribers) {
        subscriber->on_dead(addr, local_state);
        logger.trace("Notified {}", subscriber);
    }
}

// Runs inside seastar::async context
void gossiper::handle_major_state_change(inet_address ep, endpoint_state eps) {
    if (!is_dead_state(eps)) {
        if (endpoint_state_map.count(ep))  {
            logger.info("Node {} has restarted, now UP", ep);
        } else {
            logger.info("Node {} is now part of the cluster", ep);
        }
    }
    logger.trace("Adding endpoint state for {}", ep);
    endpoint_state_map[ep] = eps;

    // the node restarted: it is up to the subscriber to take whatever action is necessary
    for (auto& subscriber : _subscribers) {
        subscriber->on_restart(ep, eps);
    }

    if (!is_dead_state(eps)) {
        mark_alive(ep, eps);
    } else {
        logger.debug("Not marking {} alive due to dead state", ep);
        mark_dead(ep, eps);
    }
    for (auto& subscriber : _subscribers) {
        subscriber->on_join(ep, eps);
    }
}

bool gossiper::is_dead_state(endpoint_state eps) {
    if (!eps.get_application_state(application_state::STATUS)) {
        return false;
    }
    auto value = eps.get_application_state(application_state::STATUS)->value;
    std::vector<sstring> pieces;
    boost::split(pieces, value, boost::is_any_of(","));
    assert(pieces.size() > 0);
    sstring state = pieces[0];
    for (auto& deadstate : DEAD_STATES) {
        if (state == deadstate) {
            return true;
        }
    }
    return false;
}

// Runs inside seastar::async context
void gossiper::apply_new_states(inet_address addr, endpoint_state& local_state, endpoint_state& remote_state) {
    // don't assert here, since if the node restarts the version will go back to zero
    //int oldVersion = local_state.get_heart_beat_state().get_heart_beat_version();

    local_state.set_heart_beat_state(remote_state.get_heart_beat_state());
    // if (logger.isTraceEnabled()) {
    //     logger.trace("Updating heartbeat state version to {} from {} for {} ...",
    //     local_state.get_heart_beat_state().get_heart_beat_version(), oldVersion, addr);
    // }

    // we need to make two loops here, one to apply, then another to notify,
    // this way all states in an update are present and current when the notifications are received
    for (auto& remote_entry : remote_state.get_application_state_map()) {
        auto& remote_key = remote_entry.first;
        auto& remote_value = remote_entry.second;
        assert(remote_state.get_heart_beat_state().get_generation() == local_state.get_heart_beat_state().get_generation());
        local_state.add_application_state(remote_key, remote_value);
    }
    for (auto& entry : remote_state.get_application_state_map()) {
        do_on_change_notifications(addr, entry.first, entry.second);
    }
}

// Runs inside seastar::async context
void gossiper::do_before_change_notifications(inet_address addr, endpoint_state& ep_state, application_state& ap_state, versioned_value& new_value) {
    for (auto& subscriber : _subscribers) {
        subscriber->before_change(addr, ep_state, ap_state, new_value);
    }
}

// Runs inside seastar::async context
void gossiper::do_on_change_notifications(inet_address addr, const application_state& state, versioned_value& value) {
    for (auto& subscriber : _subscribers) {
        subscriber->on_change(addr, state, value);
    }
}

void gossiper::request_all(gossip_digest& g_digest,
    std::vector<gossip_digest>& delta_gossip_digest_list, int remote_generation) {
    /* We are here since we have no data for this endpoint locally so request everthing. */
    delta_gossip_digest_list.emplace_back(g_digest.get_endpoint(), remote_generation, 0);
    logger.trace("request_all for {}", g_digest.get_endpoint());
}

void gossiper::send_all(gossip_digest& g_digest,
    std::map<inet_address, endpoint_state>& delta_ep_state_map,
    int max_remote_version) {
    auto ep = g_digest.get_endpoint();
    auto local_ep_state_ptr = get_state_for_version_bigger_than(ep, max_remote_version);
    if (local_ep_state_ptr) {
        delta_ep_state_map[ep] = *local_ep_state_ptr;
    }
}

void gossiper::examine_gossiper(std::vector<gossip_digest>& g_digest_list,
    std::vector<gossip_digest>& delta_gossip_digest_list,
    std::map<inet_address, endpoint_state>& delta_ep_state_map) {
    if (g_digest_list.size() == 0) {
        /* we've been sent a *completely* empty syn, which should normally
             * never happen since an endpoint will at least send a syn with
             * itself.  If this is happening then the node is attempting shadow
             * gossip, and we should reply with everything we know.
             */
        logger.debug("Shadow request received, adding all states");
        for (auto& entry : endpoint_state_map) {
            g_digest_list.emplace_back(entry.first, 0, 0);
        }
    }
    for (gossip_digest& g_digest : g_digest_list) {
        int remote_generation = g_digest.get_generation();
        int max_remote_version = g_digest.get_max_version();
        /* Get state associated with the end point in digest */
        auto it = endpoint_state_map.find(g_digest.get_endpoint());
        /* Here we need to fire a GossipDigestAckMessage. If we have some
             * data associated with this endpoint locally then we follow the
             * "if" path of the logic. If we have absolutely nothing for this
             * endpoint we need to request all the data for this endpoint.
             */
        if (it != endpoint_state_map.end()) {
            endpoint_state& ep_state_ptr = it->second;
            int local_generation = ep_state_ptr.get_heart_beat_state().get_generation();
            /* get the max version of all keys in the state associated with this endpoint */
            int max_local_version = get_max_endpoint_state_version(ep_state_ptr);
            if (remote_generation == local_generation && max_remote_version == max_local_version) {
                continue;
            }

            if (remote_generation > local_generation) {
                /* we request everything from the gossiper */
                request_all(g_digest, delta_gossip_digest_list, remote_generation);
            } else if (remote_generation < local_generation) {
                /* send all data with generation = localgeneration and version > 0 */
                send_all(g_digest, delta_ep_state_map, 0);
            } else if (remote_generation == local_generation) {
                /*
                 * If the max remote version is greater then we request the
                 * remote endpoint send us all the data for this endpoint
                 * with version greater than the max version number we have
                 * locally for this endpoint.
                 *
                 * If the max remote version is lesser, then we send all
                 * the data we have locally for this endpoint with version
                 * greater than the max remote version.
                 */
                if (max_remote_version > max_local_version) {
                    delta_gossip_digest_list.emplace_back(g_digest.get_endpoint(), remote_generation, max_local_version);
                } else if (max_remote_version < max_local_version) {
                    /* send all data with generation = localgeneration and version > max_remote_version */
                    send_all(g_digest, delta_ep_state_map, max_remote_version);
                }
            }
        } else {
            /* We are here since we have no data for this endpoint locally so request everything. */
            request_all(g_digest, delta_gossip_digest_list, remote_generation);
        }
    }
}

future<> gossiper::start(int generation_number) {
    return start(generation_number, std::map<application_state, versioned_value>());
}

future<> gossiper::start(int generation_nbr, std::map<application_state, versioned_value> preload_local_states) {
    // Although gossiper runs on cpu0 only, we need to listen incoming gossip
    // message on all cpus and forard them to cpu0 to process.
    return _handlers.start().then([this] {
        return _handlers.invoke_on_all([this] (handler& h) {
            this->init_messaging_service_handler();
        });
    }).then([this, generation_nbr, preload_local_states] {
        build_seeds_list();
        /* initialize the heartbeat state for this localEndpoint */
        maybe_initialize_local_state(generation_nbr);
        endpoint_state& local_state = endpoint_state_map[get_broadcast_address()];
        for (auto& entry : preload_local_states) {
            local_state.add_application_state(entry.first, entry.second);
        }

        //notify snitches that Gossiper is about to start
#if 0
        DatabaseDescriptor.getEndpointSnitch().gossiperStarting();
#endif
        logger.trace("gossip started with generation {}", local_state.get_heart_beat_state().get_generation());
        _enabled = true;
        _scheduled_gossip_task.arm(INTERVAL);
        return make_ready_future<>();
    });
}

future<> gossiper::do_shadow_round() {
    return seastar::async([this, g = this->shared_from_this()] {
        build_seeds_list();
        _in_shadow_round = true;
        auto t = clk::now();
        while (this->_in_shadow_round) {
            // send a completely empty syn
            for (inet_address seed : _seeds) {
                std::vector<gossip_digest> digests;
                gossip_digest_syn message(get_cluster_name(), get_partitioner_name(), digests);
                auto id = get_shard_id(seed);
                logger.trace("Sending a GossipDigestSyn (ShadowRound) to {} ...", id);
                ms().send_gossip_digest_syn(id, std::move(message)).then_wrapped([this, id] (auto&& f) {
                    try {
                        auto ack_msg = f.get0();
                        logger.trace("Got GossipDigestSyn (ShadowRound) Reply");
                        return this->handle_ack_msg(id, std::move(ack_msg));
                    } catch (...) {
                        logger.trace("Fail to send GossipDigestSyn (ShadowRound) to {}: {}", id, std::current_exception());
                    }
                    return make_ready_future<>();
                }).get();
            }
            if (clk::now() > t + storage_service_ring_delay()) {
                throw std::runtime_error(sprint("Unable to gossip with any seeds (ShadowRound)"));
            }
            if (this->_in_shadow_round) {
                logger.trace("Sleep 1 second and retry ...");
                sleep(std::chrono::seconds(1)).get();
            }
        }
    });
}

void gossiper::build_seeds_list() {
    for (inet_address seed : get_seeds() ) {
        if (seed == get_broadcast_address()) {
            continue;
        }
        _seeds.emplace(seed);
    }
}

void gossiper::maybe_initialize_local_state(int generation_nbr) {
    heart_beat_state hb_state(generation_nbr);
    endpoint_state local_state(hb_state);
    local_state.mark_alive();
    inet_address ep = get_broadcast_address();
    auto it = endpoint_state_map.find(ep);
    if (it == endpoint_state_map.end()) {
        endpoint_state_map.emplace(ep, local_state);
    }
}

void gossiper::add_saved_endpoint(inet_address ep) {
    if (ep == get_broadcast_address()) {
        logger.debug("Attempt to add self as saved endpoint");
        return;
    }

    //preserve any previously known, in-memory data about the endpoint (such as DC, RACK, and so on)
    auto ep_state = endpoint_state(heart_beat_state(0));
    auto it = endpoint_state_map.find(ep);
    if (it != endpoint_state_map.end()) {
        ep_state = it->second;
        logger.debug("not replacing a previous ep_state for {}, but reusing it: {}", ep, ep_state);
        ep_state.set_heart_beat_state(heart_beat_state(0));
    }
    ep_state.mark_dead();
    endpoint_state_map[ep] = ep_state;
    _unreachable_endpoints[ep] = now();
    logger.trace("Adding saved endpoint {} {}", ep, ep_state.get_heart_beat_state().get_generation());
}

void gossiper::add_local_application_state(application_state state, versioned_value value) {
    seastar::async([this, g = this->shared_from_this(), state, value = std::move(value)] () mutable {
        inet_address ep_addr = get_broadcast_address();
        assert(endpoint_state_map.count(ep_addr));
        endpoint_state& ep_state = endpoint_state_map.at(ep_addr);
        // Fire "before change" notifications:
        do_before_change_notifications(ep_addr, ep_state, state, value);
        // Notifications may have taken some time, so preventively raise the version
        // of the new value, otherwise it could be ignored by the remote node
        // if another value with a newer version was received in the meantime:
        value = storage_service_value_factory().clone_with_higher_version(value);
        // Add to local application state and fire "on change" notifications:
        ep_state.add_application_state(state, value);
        do_on_change_notifications(ep_addr, state, value);
    }).then_wrapped([] (auto&& f) {
        try {
            f.get();
        } catch (...) {
            logger.warn("Fail to apply application_state: {}", std::current_exception());
        }
    });
}

void gossiper::add_lccal_application_states(std::list<std::pair<application_state, versioned_value> > states) {
    // Note: The taskLock in Origin code is removed, we can probaby use a
    // simple data structure here
    for (std::pair<application_state, versioned_value>& pair : states) {
        add_local_application_state(pair.first, pair.second);
    }
}

future<> gossiper::shutdown() {
    return seastar::async([this, g = this->shared_from_this()] {
        _enabled = false;
        _scheduled_gossip_task.cancel();
        logger.info("Announcing shutdown");
        sleep(INTERVAL * 2).get();
        for (inet_address addr : _live_endpoints) {
            shard_id id = get_shard_id(addr);
            logger.trace("Sending a GossipShutdown to {}", id);
            ms().send_gossip_shutdown(id, addr).then_wrapped([id] (auto&&f) {
                try {
                    f.get();
                    logger.trace("Got GossipShutdown Reply");
                } catch (...) {
                    logger.warn("Fail to send GossipShutdown to {}: {}", id, std::current_exception());
                }
            });
        }
    });
}

future<> gossiper::stop() {
    _enabled = false;
    _scheduled_gossip_task.cancel();
    return _handlers.stop().then( [this] () {
        if (engine().cpu_id() == 0) {
            get_local_failure_detector().unregister_failure_detection_event_listener(this);
        }
        return make_ready_future<>();
    });
}

bool gossiper::is_enabled() {
    return _enabled;
}

void gossiper::finish_shadow_round() {
    if (_in_shadow_round) {
        _in_shadow_round = false;
    }
}

bool gossiper::is_in_shadow_round() {
    return _in_shadow_round;
}

void gossiper::add_expire_time_for_endpoint(inet_address endpoint, clk::time_point expire_time) {
    logger.debug("adding expire time for endpoint : {} ({})", endpoint, expire_time.time_since_epoch().count());
    _expire_time_endpoint_map[endpoint] = expire_time;
}

clk::time_point gossiper::compute_expire_time() {
    return now() + A_VERY_LONG_TIME;
}

void gossiper::dump_endpoint_state_map() {
    logger.debug("----------- endpoint_state_map:  -----------");
    for (auto& x : endpoint_state_map) {
        logger.debug("ep={}, eps={}", x.first, x.second);
    }
}

void gossiper::debug_show() {
    auto reporter = std::make_shared<timer<clk>>();
    reporter->set_callback ([reporter] {
        auto& gossiper = gms::get_local_gossiper();
        gossiper.dump_endpoint_state_map();
    });
    reporter->arm_periodic(std::chrono::milliseconds(1000));
}

bool gossiper::is_alive(inet_address ep) {
    if (ep == get_broadcast_address()) {
        return true;
    }
    auto eps = get_endpoint_state_for_endpoint(ep);
    // we could assert not-null, but having isAlive fail screws a node over so badly that
    // it's worth being defensive here so minor bugs don't cause disproportionate
    // badness.  (See CASSANDRA-1463 for an example).
    if (eps) {
        return eps->is_alive();
    } else {
        logger.warn("unknown endpoint {}", ep);
        return false;
    }
}

future<std::set<inet_address>> get_unreachable_members() {
    return smp::submit_to(0, [] {
        return get_local_gossiper().get_unreachable_members();
    });
}

future<std::set<inet_address>> get_live_members() {
    return smp::submit_to(0, [] {
        return get_local_gossiper().get_live_members();
    });
}

future<int64_t> get_endpoint_downtime(inet_address ep) {
    return smp::submit_to(0, [ep] {
        return get_local_gossiper().get_endpoint_downtime(ep);
    });
}

future<int> get_current_generation_number(inet_address ep) {
    return smp::submit_to(0, [ep] {
        return get_local_gossiper().get_current_generation_number(ep);
    });
}

future<> unsafe_assassinate_endpoint(sstring ep) {
    return smp::submit_to(0, [ep] {
        return get_local_gossiper().unsafe_assassinate_endpoint(ep);
    });
}

future<> assassinate_endpoint(sstring ep) {
    return smp::submit_to(0, [ep] {
        return get_local_gossiper().assassinate_endpoint(ep);
    });
}

} // namespace gms
