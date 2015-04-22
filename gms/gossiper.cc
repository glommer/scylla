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

namespace gms {

gossiper::gossiper()
    : _scheduled_gossip_task([this] { run(); }) {
    // half of QUARATINE_DELAY, to ensure _just_removed_endpoints has enough leeway to prevent re-gossip
    fat_client_timeout = (int64_t) (QUARANTINE_DELAY / 2);
    /* register with the Failure Detector for receiving Failure detector events */
    get_local_failure_detector().register_failure_detection_event_listener(this->shared_from_this());
    // Register this instance with JMX
    init_messaging_service_handler();
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

void gossiper::init_messaging_service_handler() {
    ms().register_handler(messaging_verb::ECHO, [] (empty_msg msg) {
        return make_ready_future<empty_msg>();
    });
    ms().register_handler_oneway(messaging_verb::GOSSIP_SHUTDOWN, [] (inet_address from) {
        // TODO: Implement processing of incoming SHUTDOWN message
        get_local_failure_detector().force_conviction(from);
        return messaging_service::no_wait();
    });
    ms().register_handler(messaging_verb::GOSSIP_DIGEST_SYN, [this] (gossip_digest_syn syn_msg) {
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
    });
    ms().register_handler_oneway(messaging_verb::GOSSIP_DIGEST_ACK2, [this] (gossip_digest_ack2 msg) {
        auto& remote_ep_state_map = msg.get_endpoint_state_map();
        /* Notify the Failure Detector */
        this->notify_failure_detector(remote_ep_state_map);
        this->apply_state_locally(remote_ep_state_map);
        return messaging_service::no_wait();
    });
}

bool gossiper::send_gossip(gossip_digest_syn message, std::set<inet_address> epset) {
    std::vector<inet_address> __live_endpoints(epset.begin(), epset.end());
    size_t size = __live_endpoints.size();
    if (size < 1) {
        return false;
    }
    /* Generate a random number from 0 -> size */
    std::uniform_int_distribution<int> dist(0, size - 1);
    int index = dist(_random);
    inet_address to = __live_endpoints[index];
    // if (logger.isTraceEnabled())
    //     logger.trace("Sending a GossipDigestSyn to {} ...", to);
    using RetMsg = gossip_digest_ack;
    auto id = get_shard_id(to);
    ms().send_message<RetMsg>(messaging_verb::GOSSIP_DIGEST_SYN, std::move(id), std::move(message)).then([this, id] (RetMsg ack_msg) {
        if (!this->is_enabled() && !this->is_in_shadow_round()) {
            return make_ready_future<>();
        }

        auto g_digest_list = ack_msg.get_gossip_digest_list();
        auto ep_state_map = ack_msg.get_endpoint_state_map();

        if (ep_state_map.size() > 0) {
            /* Notify the Failure Detector */
            this->notify_failure_detector(ep_state_map);
            this->apply_state_locally(ep_state_map);
        }

        if (this->is_in_shadow_round()) {
            this->finish_shadow_round();
            return make_ready_future<>(); // don't bother doing anything else, we have what we came for
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
        return ms().send_message_oneway<void>(messaging_verb::GOSSIP_DIGEST_ACK2, std::move(id), std::move(ack2_msg)).then([] () {
            return make_ready_future<>();
        });
    });

    return _seeds.count(to);
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
                //logger.debug("Clearing interval times for {} due to generation change", endpoint);
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

void gossiper::apply_state_locally(std::map<inet_address, endpoint_state>& map) {
    for (auto& entry : map) {
        auto& ep = entry.first;
        if (ep == get_broadcast_address() && !is_in_shadow_round()) {
            continue;
        }
        if (_just_removed_endpoints.count(ep)) {
            // if (logger.isTraceEnabled())
            //     logger.trace("Ignoring gossip for {} because it is quarantined", ep);
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
            // if (logger.isTraceEnabled()) {
            //     logger.trace("{} local generation {}, remote generation {}", ep, local_generation, remote_generation);
            // }
            if (local_generation != 0 && remote_generation > local_generation + MAX_GENERATION_DIFFERENCE) {
                // assume some peer has corrupted memory and is broadcasting an unbelievable generation about another peer (or itself)
                // logger.warn("received an invalid gossip generation for peer {}; local generation = {}, received generation = {}",
                //         ep, local_generation, remote_generation);
            } else if (remote_generation > local_generation) {
                // if (logger.isTraceEnabled())
                //     logger.trace("Updating heartbeat state generation to {} from {} for {}", remote_generation, local_generation, ep);
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
                    // if (logger.isTraceEnabled()) {
                    //     logger.trace("Ignoring remote version {} <= {} for {}", remote_max_version, local_max_version, ep);
                }
                if (!local_ep_state_ptr.is_alive() && !is_dead_state(local_ep_state_ptr)) { // unless of course, it was dead
                    mark_alive(ep, local_ep_state_ptr);
                }
            } else {
                // if (logger.isTraceEnabled())
                //     logger.trace("Ignoring remote generation {} < {}", remote_generation, local_generation);
            }
        } else {
            // this is a new node, report it to the FD in case it is the first time we are seeing it AND it's not alive
            get_local_failure_detector().report(ep);
            handle_major_state_change(ep, remote_state);
        }
    }
}

void gossiper::remove_endpoint(inet_address endpoint) {
    // do subscribers first so anything in the subscriber that depends on gossiper state won't get confused
    for (auto& subscriber : _subscribers) {
        subscriber->on_remove(endpoint);
    }

    if(_seeds.count(endpoint)) {
        build_seeds_list();
        _seeds.erase(endpoint);
        //logger.info("removed {} from _seeds, updated _seeds list = {}", endpoint, _seeds);
    }

    _live_endpoints.erase(endpoint);
    _unreachable_endpoints.erase(endpoint);
    // do not remove endpointState until the quarantine expires
    get_local_failure_detector().remove(endpoint);
    // FIXME: MessagingService
    //MessagingService.instance().resetVersion(endpoint);
    warn(unimplemented::cause::GOSSIP);
    quarantine_endpoint(endpoint);
    // FIXME: MessagingService
    //MessagingService.instance().destroyConnectionPool(endpoint);
    // if (logger.isDebugEnabled())
    //     logger.debug("removing endpoint {}", endpoint);
}

void gossiper::do_status_check() {
    // if (logger.isTraceEnabled())
    //     logger.trace("Performing status check ...");

    int64_t now = now_millis();

    // FIXME:
    // int64_t pending = ((JMXEnabledThreadPoolExecutor) StageManager.getStage(Stage.GOSSIP)).getPendingTasks();
    int64_t pending = 1;
    if (pending > 0 && _last_processed_message_at < now - 1000) {
        // FIXME: SLEEP
        // if some new messages just arrived, give the executor some time to work on them
        //Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

        // still behind?  something's broke
        if (_last_processed_message_at < now - 1000) {
            // logger.warn("Gossip stage has {} pending tasks; skipping status check (no nodes will be marked down)", pending);
            return;
        }
    }

    for (auto& entry : endpoint_state_map) {
        const inet_address& endpoint = entry.first;
        if (endpoint == get_broadcast_address()) {
            continue;
        }

        get_local_failure_detector().interpret(endpoint);

        auto it = endpoint_state_map.find(endpoint);
        if (it != endpoint_state_map.end()) {
            endpoint_state& ep_state = it->second;
            // check if this is a fat client. fat clients are removed automatically from
            // gossip after FatClientTimeout.  Do not remove dead states here.
            if (is_gossip_only_member(endpoint)
                && !_just_removed_endpoints.count(endpoint)
                && ((now - ep_state.get_update_timestamp().time_since_epoch().count()) > fat_client_timeout)) {
                // logger.info("FatClient {} has been silent for {}ms, removing from gossip", endpoint, FatClientTimeout);
                remove_endpoint(endpoint); // will put it in _just_removed_endpoints to respect quarantine delay
                evict_from_membershipg(endpoint); // can get rid of the state immediately
            }

            // check for dead state removal
            int64_t expire_time = get_expire_time_for_endpoint(endpoint);
            if (!ep_state.is_alive() && (now > expire_time)) {
                /* && (!StorageService.instance.getTokenMetadata().isMember(endpoint))) */
                // if (logger.isDebugEnabled()) {
                //     logger.debug("time is expiring for endpoint : {} ({})", endpoint, expire_time);
                // }
                evict_from_membershipg(endpoint);
            }
        }
    }

    for (auto it = _just_removed_endpoints.begin(); it != _just_removed_endpoints.end();) {
        auto& t= it->second;
        if ((now - t) > QUARANTINE_DELAY) {
            // if (logger.isDebugEnabled())
            //     logger.debug("{} elapsed, {} gossip quarantine over", QUARANTINE_DELAY, entry.getKey());
            it = _just_removed_endpoints.erase(it);
        } else {
            it++;
        }
    }
}

void gossiper::run() {
    //wait on messaging service to start listening
    // MessagingService.instance().waitUntilListening();


    /* Update the local heartbeat counter. */
    //endpoint_state_map.get(FBUtilities.getBroadcastAddress()).get_heart_beat_state().updateHeartBeat();
    // if (logger.isTraceEnabled())
    //     logger.trace("My heartbeat is now {}", endpoint_state_map.get(FBUtilities.getBroadcastAddress()).get_heart_beat_state().get_heart_beat_version());
    std::vector<gossip_digest> g_digests;
    this->make_random_gossip_digest(g_digests);

    if (g_digests.size() > 0) {
        gossip_digest_syn message(get_cluster_name(), get_partitioner_name(), g_digests);

        /* Gossip to some random live member */
        bool gossiped_to_seed = do_gossip_to_live_member(message);

        /* Gossip to some unreachable member with some probability to check if he is back up */
        do_gossip_to_unreachable_member(message);

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
            do_gossip_to_seed(message);
        }

        do_status_check();
    }
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
        // FIXME: StorageService.instance.getTokenMetadata
        if (it != endpoint_state_map.end() && !is_dead_state(it->second) /* && StorageService.instance.getTokenMetadata().isMember(member) */) {
            token_owners.insert(member);
        }
        warn(unimplemented::cause::GOSSIP);
    }
    return token_owners;
}

std::set<inet_address> gossiper::get_unreachable_token_owners() {
    std::set<inet_address> token_owners;
    for (auto&& x : _unreachable_endpoints) {
        auto& endpoint = x.first;
        warn(unimplemented::cause::GOSSIP);
        if (true /* StorageService.instance.getTokenMetadata().isMember(endpoint) */) {
            token_owners.insert(endpoint);
        }
    }
    return token_owners;
}

int64_t gossiper::get_endpoint_downtime(inet_address ep) {
    auto it = _unreachable_endpoints.find(ep);
    if (it != _unreachable_endpoints.end()) {
        auto& downtime = it->second;
        return (now_nanos() - downtime) / 1000;
    } else {
        return 0L;
    }
}

void gossiper::convict(inet_address endpoint, double phi) {
    auto it = endpoint_state_map.find(endpoint);
    if (it == endpoint_state_map.end()) {
        return;
    }
    auto& state = it->second;
    if (state.is_alive() && is_dead_state(state)) {
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

void gossiper::evict_from_membershipg(inet_address endpoint) {
    _unreachable_endpoints.erase(endpoint);
    endpoint_state_map.erase(endpoint);
    _expire_time_endpoint_map.erase(endpoint);
    quarantine_endpoint(endpoint);
    // if (logger.isDebugEnabled())
    //     logger.debug("evicting {} from gossip", endpoint);
}

void gossiper::quarantine_endpoint(inet_address endpoint) {
    quarantine_endpoint(endpoint, now_millis());
}

void gossiper::quarantine_endpoint(inet_address endpoint, int64_t quarantine_expiration) {
    _just_removed_endpoints[endpoint] = quarantine_expiration;
}

void gossiper::replacement_quarantine(inet_address endpoint) {
    // remember, quarantine_endpoint will effectively already add QUARANTINE_DELAY, so this is 2x
    // logger.debug("");
    quarantine_endpoint(endpoint, now_millis() + QUARANTINE_DELAY);
}

void gossiper::replaced_endpoint(inet_address endpoint) {
    remove_endpoint(endpoint);
    evict_from_membershipg(endpoint);
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

void gossiper::advertise_removing(inet_address endpoint, utils::UUID host_id, utils::UUID local_host_id) {
    auto& state = endpoint_state_map.at(endpoint);
    // remember this node's generation
    int generation = state.get_heart_beat_state().get_generation();
    // logger.info("Removing host: {}", host_id);
    // logger.info("Sleeping for {}ms to ensure {} does not change", StorageService.RING_DELAY, endpoint);
    // FIXME: sleep
    warn(unimplemented::cause::GOSSIP);
    // Uninterruptibles.sleepUninterruptibly(StorageService.RING_DELAY, TimeUnit.MILLISECONDS);
    // make sure it did not change
    auto& eps = endpoint_state_map.at(endpoint);
    if (eps.get_heart_beat_state().get_generation() != generation) {
        throw std::runtime_error(sprint("Endpoint %s generation changed while trying to remove it", endpoint));
    }

    // update the other node's generation to mimic it as if it had changed it itself
    //logger.info("Advertising removal for {}", endpoint);
    eps.update_timestamp(); // make sure we don't evict it too soon
    eps.get_heart_beat_state().force_newer_generation_unsafe();
    // FIXME:  StorageService.instance.valueFactory
    // eps.add_application_state(application_state::STATUS, StorageService.instance.valueFactory.removingNonlocal(host_id));
    // eps.add_application_state(application_state::REMOVAL_COORDINATOR, StorageService.instance.valueFactory.removalCoordinator(local_host_id));
    endpoint_state_map[endpoint] = eps;
}

void gossiper::advertise_token_removed(inet_address endpoint, utils::UUID host_id) {
    auto& eps = endpoint_state_map.at(endpoint);
    eps.update_timestamp(); // make sure we don't evict it too soon
    eps.get_heart_beat_state().force_newer_generation_unsafe();
    int64_t expire_time = compute_expire_time();
    // FIXME: StorageService.instance.valueFactory.removedNonlocal
    // eps.add_application_state(application_state::STATUS, StorageService.instance.valueFactory.removedNonlocal(host_id, expire_time));
    //logger.info("Completing removal of {}", endpoint);
    add_expire_time_for_endpoint(endpoint, expire_time);
    endpoint_state_map[endpoint] = eps;
    // ensure at least one gossip round occurs before returning
    // FIXME: sleep
    //Uninterruptibles.sleepUninterruptibly(INTERVAL_IN_MILLIS * 2, TimeUnit.MILLISECONDS);
    warn(unimplemented::cause::GOSSIP);
}

void gossiper::unsafe_assassinate_endpoint(sstring address) {
    //logger.warn("Gossiper.unsafeAssassinateEndpoint is deprecated and will be removed in the next release; use assassinate_endpoint instead");
    assassinate_endpoint(address);
}

void gossiper::assassinate_endpoint(sstring address) {
    inet_address endpoint(address);
    auto is_exist = endpoint_state_map.count(endpoint);
    endpoint_state&& ep_state = is_exist ? endpoint_state_map.at(endpoint) :
                                           endpoint_state(heart_beat_state((int) ((now_millis() + 60000) / 1000), 9999));
    //Collection<Token> tokens = null;
    // logger.warn("Assassinating {} via gossip", endpoint);
    if (is_exist) {
        // FIXME:
        warn(unimplemented::cause::GOSSIP);
#if 0
        try {
            tokens = StorageService.instance.getTokenMetadata().getTokens(endpoint);
        } catch (Throwable th) {
            JVMStabilityInspector.inspectThrowable(th);
            // TODO this is broken
            logger.warn("Unable to calculate tokens for {}.  Will use a random one", address);
            tokens = Collections.singletonList(StorageService.getPartitioner().getRandomToken());
        }
#endif
        int generation = ep_state.get_heart_beat_state().get_generation();
        int heartbeat = ep_state.get_heart_beat_state().get_heart_beat_version();
        //logger.info("Sleeping for {}ms to ensure {} does not change", StorageService.RING_DELAY, endpoint);
        //Uninterruptibles.sleepUninterruptibly(StorageService.RING_DELAY, TimeUnit.MILLISECONDS);
        // make sure it did not change

        auto it = endpoint_state_map.find(endpoint);
        if (it == endpoint_state_map.end()) {
            // logger.warn("Endpoint {} disappeared while trying to assassinate, continuing anyway", endpoint);
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
    // FIXME: StorageService.instance and Sleep
    // ep_state.add_application_state(application_state::STATUS, StorageService.instance.valueFactory.left(tokens, compute_expire_time()));
    handle_major_state_change(endpoint, ep_state);
    // Uninterruptibles.sleepUninterruptibly(INTERVAL_IN_MILLIS * 4, TimeUnit.MILLISECONDS);
    //logger.warn("Finished assassinating {}", endpoint);
}

bool gossiper::is_known_endpoint(inet_address endpoint) {
    return endpoint_state_map.count(endpoint);
}

int gossiper::get_current_generation_number(inet_address endpoint) {
    return endpoint_state_map.at(endpoint).get_heart_beat_state().get_generation();
}

bool gossiper::do_gossip_to_live_member(gossip_digest_syn message) {
    size_t size = _live_endpoints.size();
    if (size == 0) {
        return false;
    }
    return send_gossip(message, _live_endpoints);
}

void gossiper::do_gossip_to_unreachable_member(gossip_digest_syn message) {
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
            send_gossip(message, addrs);
        }
    }
}

void gossiper::do_gossip_to_seed(gossip_digest_syn prod) {
    size_t size = _seeds.size();
    if (size > 0) {
        if (size == 1 && _seeds.count(get_broadcast_address())) {
            return;
        }

        if (_live_endpoints.size() == 0) {
            send_gossip(prod, _seeds);
        } else {
            /* Gossip with the seed with some probability. */
            double probability = _seeds.size() / (double) (_live_endpoints.size() + _unreachable_endpoints.size());
            std::uniform_real_distribution<double> dist(0, 1);
            double rand_dbl = dist(_random);
            if (rand_dbl <= probability) {
                send_gossip(prod, _seeds);
            }
        }
    }
}

bool gossiper::is_gossip_only_member(inet_address endpoint) {
    auto it = endpoint_state_map.find(endpoint);
    if (it == endpoint_state_map.end()) {
        return false;
    }
    auto& eps = it->second;
    // FIXME: StorageService.instance.getTokenMetadata
    return !is_dead_state(eps) /* && !StorageService.instance.getTokenMetadata().isMember(endpoint); */;
}

int64_t gossiper::get_expire_time_for_endpoint(inet_address endpoint) {
    /* default expire_time is A_VERY_LONG_TIME */
    auto it = _expire_time_endpoint_map.find(endpoint);
    if (it == _expire_time_endpoint_map.end()) {
        return compute_expire_time();
    } else {
        int64_t stored_time = it->second;
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

std::map<inet_address, endpoint_state>&gms::gossiper::get_endpoint_states() {
    return endpoint_state_map;
}

bool gossiper::uses_host_id(inet_address endpoint) {
    // FIXME
    warn(unimplemented::cause::GOSSIP);
    if (true /* MessagingService.instance().knowsVersion(endpoint) */) {
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
    // FIXME: Add UUID(const sstring& id) constructor
    warn(unimplemented::cause::GOSSIP);
    return utils::UUID(0, 0);
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
            // if (logger.isTraceEnabled())
            //     logger.trace("local heartbeat version {} greater than {} for {}", local_hb_version, version, for_endpoint);
        }
        /* Accumulate all application states whose versions are greater than "version" variable */
        for (auto& entry : eps.get_application_state_map()) {
            auto& value = entry.second;
            if (value.version > version) {
                if (!reqd_endpoint_state) {
                    reqd_endpoint_state.emplace(eps.get_heart_beat_state());
                }
                auto& key = entry.first;
                // if (logger.isTraceEnabled())
                //     logger.trace("Adding state {}: {}" , key, value.value);
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

void gossiper::mark_alive(inet_address addr, endpoint_state local_state) {
    // if (MessagingService.instance().getVersion(addr) < MessagingService.VERSION_20) {
    //     real_mark_alive(addr, local_state);
    //     return;
    // }

    local_state.mark_dead();
    //logger.trace("Sending a EchoMessage to {}", addr);
    shard_id id = get_shard_id(addr);
    ms().send_message<empty_msg>(messaging_verb::ECHO, id).then([this, addr, local_state = std::move(local_state)] (empty_msg msg) mutable {
        this->real_mark_alive(addr, local_state);
    });
}

void gossiper::real_mark_alive(inet_address addr, endpoint_state local_state) {
    // if (logger.isTraceEnabled())
    //         logger.trace("marking as alive {}", addr);
    local_state.mark_alive();
    local_state.update_timestamp(); // prevents do_status_check from racing us and evicting if it was down > A_VERY_LONG_TIME
    _live_endpoints.insert(addr);
    _unreachable_endpoints.erase(addr);
    _expire_time_endpoint_map.erase(addr);
    // logger.debug("removing expire time for endpoint : {}", addr);
    // logger.info("inet_address {} is now UP", addr);
    for (auto& subscriber : _subscribers)
        subscriber->on_alive(addr, local_state);
    // if (logger.isTraceEnabled())
    //     logger.trace("Notified {}", _subscribers);
}

void gossiper::mark_dead(inet_address addr, endpoint_state local_state) {
    // if (logger.isTraceEnabled())
    //     logger.trace("marking as down {}", addr);
    local_state.mark_dead();
    _live_endpoints.erase(addr);
    _unreachable_endpoints[addr] = now_nanos();
    // logger.info("inet_address {} is now DOWN", addr);
    for (auto& subscriber : _subscribers)
        subscriber->on_dead(addr, local_state);
    // if (logger.isTraceEnabled())
    //     logger.trace("Notified {}", _subscribers);
}

void gossiper::handle_major_state_change(inet_address ep, endpoint_state eps) {
    if (!is_dead_state(eps)) {
        if (endpoint_state_map.count(ep))  {
            //logger.info("Node {} has restarted, now UP", ep);
        } else {
            //logger.info("Node {} is now part of the cluster", ep);
        }
    }
    // if (logger.isTraceEnabled())
    //     logger.trace("Adding endpoint state for {}", ep);
    endpoint_state_map[ep] = eps;

    // the node restarted: it is up to the subscriber to take whatever action is necessary
    for (auto& subscriber : _subscribers) {
        subscriber->on_restart(ep, eps);
    }

    if (!is_dead_state(eps)) {
        mark_alive(ep, eps);
    } else {
        //logger.debug("Not marking {} alive due to dead state", ep);
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

void gossiper::apply_new_states(inet_address addr, endpoint_state local_state, endpoint_state remote_state) {
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

void gossiper::do_before_change_notifications(inet_address addr, endpoint_state& ep_state, application_state& ap_state, versioned_value& new_value) {
    for (auto& subscriber : _subscribers) {
        subscriber->before_change(addr, ep_state, ap_state, new_value);
    }
}

void gossiper::do_on_change_notifications(inet_address addr, const application_state& state, versioned_value& value) {
    for (auto& subscriber : _subscribers) {
        subscriber->on_change(addr, state, value);
    }
}

void gossiper::request_all(gossip_digest& g_digest,
    std::vector<gossip_digest>& delta_gossip_digest_list, int remote_generation) {
    /* We are here since we have no data for this endpoint locally so request everthing. */
    delta_gossip_digest_list.emplace_back(g_digest.get_endpoint(), remote_generation, 0);
    // if (logger.isTraceEnabled())
    //     logger.trace("request_all for {}", g_digest.get_endpoint());
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
        // logger.debug("Shadow request received, adding all states");
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

void gossiper::start(int generation_number) {
    start(generation_number, std::map<application_state, versioned_value>());
}

void gossiper::start(int generation_nbr, std::map<application_state, versioned_value> preload_local_states) {
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
    if (logger.isTraceEnabled())
        logger.trace("gossip started with generation {}", local_state.get_heart_beat_state().get_generation());
#endif
    std::chrono::milliseconds period(INTERVAL_IN_MILLIS);
    _scheduled_gossip_task.arm_periodic(period);
}

void gossiper::do_shadow_round() {
    build_seeds_list();
    // send a completely empty syn
    std::vector<gossip_digest> g_digests;
    gossip_digest_syn message(get_cluster_name(), get_partitioner_name(), g_digests);
    _in_shadow_round = true;
    for (inet_address seed : _seeds) {
        auto id = get_shard_id(seed);
        ms().send_message<gossip_digest_ack>(messaging_verb::GOSSIP_DIGEST_SYN,
                                             std::move(id), std::move(message)).then([this, id] (gossip_digest_ack ack_msg) {
            if (this->is_in_shadow_round()) {
                this->finish_shadow_round();
            }
        });
    }
    // FIXME: Implemnt the wait logic below
#if 0
    int slept = 0;
    try {
        while (true) {
            Thread.sleep(1000);
            if (!_in_shadow_round)
                break;
            slept += 1000;
            if (slept > StorageService.RING_DELAY)
                throw new RuntimeException("Unable to gossip with any _seeds");
        }
    } catch (InterruptedException wtf) {
        throw new RuntimeException(wtf);
    }
#endif
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
        // logger.debug("Attempt to add self as saved endpoint");
        return;
    }

    //preserve any previously known, in-memory data about the endpoint (such as DC, RACK, and so on)
    auto ep_state = endpoint_state(heart_beat_state(0));
    auto it = endpoint_state_map.find(ep);
    if (it != endpoint_state_map.end()) {
        ep_state = it->second;
        // logger.debug("not replacing a previous ep_state for {}, but reusing it: {}", ep, ep_state);
        ep_state.set_heart_beat_state(heart_beat_state(0));
    }
    ep_state.mark_dead();
    endpoint_state_map[ep] = ep_state;
    _unreachable_endpoints[ep] = now_nanos();
    // if (logger.isTraceEnabled())
    //     logger.trace("Adding saved endpoint {} {}", ep, ep_state.get_heart_beat_state().get_generation());
}

void gossiper::add_local_application_state(application_state state, versioned_value value) {
    inet_address ep_addr = get_broadcast_address();
    assert(endpoint_state_map.count(ep_addr));
    endpoint_state& ep_state = endpoint_state_map.at(ep_addr);
    // Fire "before change" notifications:
    do_before_change_notifications(ep_addr, ep_state, state, value);
    // Notifications may have taken some time, so preventively raise the version
    // of the new value, otherwise it could be ignored by the remote node
    // if another value with a newer version was received in the meantime:
    // FIXME:
    // value = StorageService.instance.valueFactory.cloneWithHigherVersion(value);
    // Add to local application state and fire "on change" notifications:
    ep_state.add_application_state(state, value);
    do_on_change_notifications(ep_addr, state, value);
}

void gossiper::add_lccal_application_states(std::list<std::pair<application_state, versioned_value> > states) {
    // Note: The taskLock in Origin code is removed, we can probaby use a
    // simple data structure here
    for (std::pair<application_state, versioned_value>& pair : states) {
        add_local_application_state(pair.first, pair.second);
    }
}

void gossiper::stop() {
    warn(unimplemented::cause::GOSSIP);
    // if (scheduledGossipTask != null)
    // 	scheduledGossipTask.cancel(false);
    // logger.info("Announcing shutdown");
    // Uninterruptibles.sleepUninterruptibly(INTERVAL_IN_MILLIS * 2, TimeUnit.MILLISECONDS);
    for (inet_address ep : _live_endpoints) {
        ms().send_message_oneway<void>(messaging_verb::GOSSIP_SHUTDOWN, get_shard_id(ep), ep).then([]{
        });
    }
}

bool gossiper::is_enabled() {
    //return (scheduledGossipTask != null) && (!scheduledGossipTask.isCancelled());
    warn(unimplemented::cause::GOSSIP);
    return true;
}

void gossiper::finish_shadow_round() {
    if (_in_shadow_round) {
        _in_shadow_round = false;
    }
}

bool gossiper::is_in_shadow_round() {
    return _in_shadow_round;
}

void gossiper::add_expire_time_for_endpoint(inet_address endpoint, int64_t expire_time) {
    // if (logger.isDebugEnabled()) {
    //     logger.debug("adding expire time for endpoint : {} ({})", endpoint, expire_time);
    // }
    _expire_time_endpoint_map[endpoint] = expire_time;
}

int64_t gossiper::compute_expire_time() {
    return now_millis() + A_VERY_LONG_TIME;
}

void gossiper::dump_endpoint_state_map() {
    print("----------- endpoint_state_map:  -----------\n");
    for (auto& x : endpoint_state_map) {
        print("ep=%s, eps=%s\n", x.first, x.second);
    }
}

} // namespace gms
