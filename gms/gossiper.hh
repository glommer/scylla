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

#pragma once

#include "unimplemented.hh"
#include "core/distributed.hh"
#include "core/shared_ptr.hh"
#include "core/print.hh"
#include "utils/UUID.hh"
#include "utils/fb_utilities.hh"
#include "gms/i_failure_detection_event_listener.hh"
#include "gms/versioned_value.hh"
#include "gms/application_state.hh"
#include "gms/endpoint_state.hh"
#include "message/messaging_service.hh"
#include <boost/algorithm/string.hpp>
#include <experimental/optional>
#include <algorithm>
#include <chrono>

namespace gms {

class gossip_digest_syn;
class gossip_digest_ack;
class gossip_digest_ack2;
class gossip_digest;
class inet_address;
class i_endpoint_state_change_subscriber;
class i_failure_detector;

/**
 * This module is responsible for Gossiping information for the local endpoint. This abstraction
 * maintains the list of live and dead endpoints. Periodically i.e. every 1 second this module
 * chooses a random node and initiates a round of Gossip with it. A round of Gossip involves 3
 * rounds of messaging. For instance if node A wants to initiate a round of Gossip with node B
 * it starts off by sending node B a GossipDigestSynMessage. Node B on receipt of this message
 * sends node A a GossipDigestAckMessage. On receipt of this message node A sends node B a
 * GossipDigestAck2Message which completes a round of Gossip. This module as and when it hears one
 * of the three above mentioned messages updates the Failure Detector with the liveness information.
 * Upon hearing a GossipShutdownMessage, this module will instantly mark the remote node as down in
 * the Failure Detector.
 */
class gossiper : public i_failure_detection_event_listener, public enable_shared_from_this<gossiper> {
public:
    using clk = std::chrono::high_resolution_clock;
private:
    using messaging_verb = net::messaging_verb;
    using messaging_service = net::messaging_service;
    using shard_id = net::messaging_service::shard_id;
    net::messaging_service& ms() {
        return net::get_local_messaging_service();
    }
    class handler {
    public:
        future<> stop() {
            return make_ready_future<>();
        }
    };
    distributed<handler> _handlers;
    void init_messaging_service_handler();
    future<gossip_digest_ack> handle_syn_msg(gossip_digest_syn syn_msg);
    future<> handle_ack_msg(shard_id id, gossip_digest_ack ack_msg);
    static constexpr uint32_t _default_cpuid = 0;
    shard_id get_shard_id(inet_address to) {
        return shard_id{to, _default_cpuid};
    }
    void do_sort(std::vector<gossip_digest>& g_digest_list);
    timer<clk> _scheduled_gossip_task;
    bool _enabled = false;
    sstring get_cluster_name() {
        // FIXME: DatabaseDescriptor.getClusterName()
        return "my_cluster_name";
    }
    sstring get_partitioner_name() {
        // FIXME: DatabaseDescriptor.getPartitionerName()
        return "my_partitioner_name";
    }
    std::set<inet_address> _seeds_from_config;
public:
    inet_address get_broadcast_address() {
        return utils::fb_utilities::get_broadcast_address();
    }
    std::set<inet_address> get_seeds() {
        // FIXME: DatabaseDescriptor.getSeeds()
        return _seeds_from_config;
    }
    void set_seeds(std::set<inet_address> _seeds) {
        _seeds_from_config = _seeds;
    }
public:
    static clk::time_point inline now() { return clk::now(); }
public:
    /* map where key is the endpoint and value is the state associated with the endpoint */
    std::unordered_map<inet_address, endpoint_state> endpoint_state_map;

    const std::vector<sstring> DEAD_STATES = { versioned_value::REMOVING_TOKEN, versioned_value::REMOVED_TOKEN,
                                               versioned_value::STATUS_LEFT, versioned_value::HIBERNATE };
    static constexpr std::chrono::milliseconds INTERVAL{1000};
    static constexpr std::chrono::hours A_VERY_LONG_TIME{24 * 3};

    /** Maximimum difference in generation and version values we are willing to accept about a peer */
    static constexpr int64_t MAX_GENERATION_DIFFERENCE = 86400 * 365;
    std::chrono::milliseconds fat_client_timeout;

    static std::chrono::milliseconds quarantine_delay();
private:

    std::random_device _random;
    /* subscribers for interest in EndpointState change */
    std::list<i_endpoint_state_change_subscriber*> _subscribers;

    /* live member set */
    std::set<inet_address> _live_endpoints;

    /* unreachable member set */
    std::map<inet_address, clk::time_point> _unreachable_endpoints;

    /* initial seeds for joining the cluster */
    std::set<inet_address> _seeds;

    /* map where key is endpoint and value is timestamp when this endpoint was removed from
     * gossip. We will ignore any gossip regarding these endpoints for QUARANTINE_DELAY time
     * after removal to prevent nodes from falsely reincarnating during the time when removal
     * gossip gets propagated to all nodes */
    std::map<inet_address, clk::time_point> _just_removed_endpoints;

    std::map<inet_address, clk::time_point> _expire_time_endpoint_map;

    bool _in_shadow_round = false;

    clk::time_point _last_processed_message_at = now();

    std::unordered_map<inet_address, endpoint_state> _shadow_endpoint_state_map;
    std::set<inet_address> _shadow_live_endpoints;

    void run();
public:
    gossiper();

    void set_last_processed_message_at();
    void set_last_processed_message_at(clk::time_point tp);

    bool seen_any_seed();

    /**
     * Register for interesting state changes.
     *
     * @param subscriber module which implements the IEndpointStateChangeSubscriber
     */
    void register_(i_endpoint_state_change_subscriber* subscriber);

    /**
     * Unregister interest for state changes.
     *
     * @param subscriber module which implements the IEndpointStateChangeSubscriber
     */
    void unregister_(i_endpoint_state_change_subscriber* subscriber);

    std::set<inet_address> get_live_members();

    std::set<inet_address> get_live_token_owners();

    /**
     * @return a list of unreachable gossip participants, including fat clients
     */
    std::set<inet_address> get_unreachable_members();

    /**
     * @return a list of unreachable token owners
     */
    std::set<inet_address> get_unreachable_token_owners();

    int64_t get_endpoint_downtime(inet_address ep);

    /**
     * This method is part of IFailureDetectionEventListener interface. This is invoked
     * by the Failure Detector when it convicts an end point.
     *
     * @param endpoint end point that is convicted.
     */
    virtual void convict(inet_address endpoint, double phi) override;

    /**
     * Return either: the greatest heartbeat or application state
     *
     * @param ep_state
     * @return
     */
    int get_max_endpoint_state_version(endpoint_state state);


private:
    /**
     * Removes the endpoint from gossip completely
     *
     * @param endpoint endpoint to be removed from the current membership.
     */
    void evict_from_membershipg(inet_address endpoint);
public:
    /**
     * Removes the endpoint from Gossip but retains endpoint state
     */
    void remove_endpoint(inet_address endpoint);
private:
    /**
     * Quarantines the endpoint for QUARANTINE_DELAY
     *
     * @param endpoint
     */
    void quarantine_endpoint(inet_address endpoint);

    /**
     * Quarantines the endpoint until quarantine_expiration + QUARANTINE_DELAY
     *
     * @param endpoint
     * @param quarantine_expiration
     */
    void quarantine_endpoint(inet_address endpoint, clk::time_point quarantine_expiration);

public:
    /**
     * Quarantine endpoint specifically for replacement purposes.
     * @param endpoint
     */
    void replacement_quarantine(inet_address endpoint);

    /**
     * Remove the Endpoint and evict immediately, to avoid gossiping about this node.
     * This should only be called when a token is taken over by a new IP address.
     *
     * @param endpoint The endpoint that has been replaced
     */
    void replaced_endpoint(inet_address endpoint);

private:
    /**
     * The gossip digest is built based on randomization
     * rather than just looping through the collection of live endpoints.
     *
     * @param g_digests list of Gossip Digests.
     */
    void make_random_gossip_digest(std::vector<gossip_digest>& g_digests);

public:
    /**
     * This method will begin removing an existing endpoint from the cluster by spoofing its state
     * This should never be called unless this coordinator has had 'removenode' invoked
     *
     * @param endpoint    - the endpoint being removed
     * @param host_id      - the ID of the host being removed
     * @param local_host_id - my own host ID for replication coordination
     */
    future<> advertise_removing(inet_address endpoint, utils::UUID host_id, utils::UUID local_host_id);

    /**
     * Handles switching the endpoint's state from REMOVING_TOKEN to REMOVED_TOKEN
     * This should only be called after advertise_removing
     *
     * @param endpoint
     * @param host_id
     */
    future<> advertise_token_removed(inet_address endpoint, utils::UUID host_id);

    future<> unsafe_assassinate_endpoint(sstring address);

    /**
     * Do not call this method unless you know what you are doing.
     * It will try extremely hard to obliterate any endpoint from the ring,
     * even if it does not know about it.
     *
     * @param address
     * @throws UnknownHostException
     */
    future<> assassinate_endpoint(sstring address);

public:
    bool is_known_endpoint(inet_address endpoint);

    int get_current_generation_number(inet_address endpoint);

    bool is_gossip_only_member(inet_address endpoint);
private:
    /**
     * Returns true if the chosen target was also a seed. False otherwise
     *
     * @param message
     * @param epSet   a set of endpoint from which a random endpoint is chosen.
     * @return true if the chosen endpoint is also a seed.
     */
    future<bool> send_gossip(gossip_digest_syn message, std::set<inet_address> epset);

    /* Sends a Gossip message to a live member and returns true if the recipient was a seed */
    future<bool> do_gossip_to_live_member(gossip_digest_syn message);

    /* Sends a Gossip message to an unreachable member */
    future<> do_gossip_to_unreachable_member(gossip_digest_syn message);

    /* Gossip to a seed for facilitating partition healing */
    future<> do_gossip_to_seed(gossip_digest_syn prod);

    void do_status_check();

public:
    clk::time_point get_expire_time_for_endpoint(inet_address endpoint);

    std::experimental::optional<endpoint_state> get_endpoint_state_for_endpoint(inet_address ep);

    // removes ALL endpoint states; should only be called after shadow gossip
    void reset_endpoint_state_map();

    std::unordered_map<inet_address, endpoint_state>& get_endpoint_states();

    bool uses_host_id(inet_address endpoint);

    bool uses_vnodes(inet_address endpoint);

    utils::UUID get_host_id(inet_address endpoint);

    std::experimental::optional<endpoint_state> get_state_for_version_bigger_than(inet_address for_endpoint, int version);

    /**
     * determine which endpoint started up earlier
     */
    int compare_endpoint_startup(inet_address addr1, inet_address addr2);

    void notify_failure_detector(std::map<inet_address, endpoint_state> remoteEpStateMap);


    void notify_failure_detector(inet_address endpoint, endpoint_state remote_endpoint_state);

private:
    void mark_alive(inet_address addr, endpoint_state local_state);

    void real_mark_alive(inet_address addr, endpoint_state local_state);

    void mark_dead(inet_address addr, endpoint_state& local_state);

    /**
     * This method is called whenever there is a "big" change in ep state (a generation change for a known node).
     *
     * @param ep      endpoint
     * @param ep_state EndpointState for the endpoint
     */
    void handle_major_state_change(inet_address ep, endpoint_state eps);

public:
    bool is_alive(inet_address ep);
    bool is_dead_state(endpoint_state eps);

    future<> apply_state_locally(std::map<inet_address, endpoint_state>& map);

private:
    void apply_new_states(inet_address addr, endpoint_state& local_state, endpoint_state& remote_state);

    // notify that a local application state is going to change (doesn't get triggered for remote changes)
    void do_before_change_notifications(inet_address addr, endpoint_state& ep_state, application_state& ap_state, versioned_value& new_value);

    // notify that an application state has changed
    void do_on_change_notifications(inet_address addr, const application_state& state, versioned_value& value);
    /* Request all the state for the endpoint in the g_digest */

    void request_all(gossip_digest& g_digest, std::vector<gossip_digest>& delta_gossip_digest_list, int remote_generation);

    /* Send all the data with version greater than max_remote_version */
    void send_all(gossip_digest& g_digest, std::map<inet_address, endpoint_state>& delta_ep_state_map, int max_remote_version);

public:
    /*
        This method is used to figure the state that the Gossiper has but Gossipee doesn't. The delta digests
        and the delta state are built up.
    */
    void examine_gossiper(std::vector<gossip_digest>& g_digest_list,
                         std::vector<gossip_digest>& delta_gossip_digest_list,
                         std::map<inet_address, endpoint_state>& delta_ep_state_map);

public:
    future<> start(int generation_number);

    /**
     * Start the gossiper with the generation number, preloading the map of application states before starting
     */
    future<> start(int generation_nbr, std::map<application_state, versioned_value> preload_local_states);

public:
    /**
     *  Do a single 'shadow' round of gossip, where we do not modify any state
     *  Only used when replacing a node, to get and assume its states
     */
    future<> do_shadow_round();

private:
    void build_seeds_list();

public:
    // initialize local HB state if needed, i.e., if gossiper has never been started before.
    void maybe_initialize_local_state(int generation_nbr);

    /**
     * Add an endpoint we knew about previously, but whose state is unknown
     */
    void add_saved_endpoint(inet_address ep);

    void add_local_application_state(application_state state, versioned_value value);

    void add_lccal_application_states(std::list<std::pair<application_state, versioned_value>> states);

    future<> shutdown();

    future<> stop();

public:
    bool is_enabled();

    void finish_shadow_round();

    bool is_in_shadow_round();

public:
    void add_expire_time_for_endpoint(inet_address endpoint, clk::time_point expire_time);

    static clk::time_point compute_expire_time();
public:
    void dump_endpoint_state_map();
    void debug_show();
};

extern distributed<gossiper> _the_gossiper;
inline gossiper& get_local_gossiper() {
    return _the_gossiper.local();
}
inline distributed<gossiper>& get_gossiper() {
    return _the_gossiper;
}

future<std::set<inet_address>> get_unreachable_members();
future<std::set<inet_address>> get_live_members();
future<int64_t> get_endpoint_downtime(inet_address ep);
future<int> get_current_generation_number(inet_address ep);
future<> unsafe_assassinate_endpoint(sstring ep);
future<> assassinate_endpoint(sstring ep);

} // namespace gms
