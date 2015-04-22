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

#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"
#include "gms/i_failure_detector.hh"
#include "gms/i_failure_detection_event_listener.hh"
#include "gms/endpoint_state.hh"
#include "gms/application_state.hh"
#include "gms/inet_address.hh"
#include <iostream>

namespace gms {

long failure_detector_helper::get_initial_value() {
#if 0
    String newvalue = System.getProperty("cassandra.fd_initial_value_ms");
    if (newvalue == null)
    {
        return Gossiper.intervalInMillis * 2;
    }
    else
    {
        logger.info("Overriding FD INITIAL_VALUE to {}ms", newvalue);
        return Integer.parseInt(newvalue);
    }
#endif
    warn(unimplemented::cause::GOSSIP);
    return 1000 * 2;
}

long failure_detector_helper::INITIAL_VALUE_NANOS() {
    // Convert from milliseconds to nanoseconds
    return get_initial_value() * 1000;
}

long arrival_window::get_max_interval() {
#if 0
    sstring newvalue = System.getProperty("cassandra.fd_max_interval_ms");
    if (newvalue == null)
    {
        return failure_detector.INITIAL_VALUE_NANOS;
    }
    else
    {
        logger.info("Overriding FD MAX_INTERVAL to {}ms", newvalue);
        return TimeUnit.NANOSECONDS.convert(Integer.parseInt(newvalue), TimeUnit.MILLISECONDS);
    }
#endif
    warn(unimplemented::cause::GOSSIP);
    return failure_detector_helper::INITIAL_VALUE_NANOS();
}

void arrival_window::add(long value) {
    printf("arrival_window::add value=%ld,inter_arrival_time=%ld,_tlast=%ld\n", value, value - _tlast, _tlast);
    assert(_tlast >= 0);
    if (_tlast > 0L) {
        long inter_arrival_time = value - _tlast;
        if (inter_arrival_time <= MAX_INTERVAL_IN_NANO) {
            _arrival_intervals.add(inter_arrival_time);
        } else  {
            //logger.debug("Ignoring interval time of {}", interArrivalTime);
        }
    } else {
        // We use a very large initial interval since the "right" average depends on the cluster size
        // and it's better to err high (false negatives, which will be corrected by waiting a bit longer)
        // than low (false positives, which cause "flapping").
        _arrival_intervals.add(failure_detector_helper::INITIAL_VALUE_NANOS());
    }
    _tlast = value;
}

double arrival_window::mean() {
    return _arrival_intervals.mean();
}

double arrival_window::phi(long tnow) {
    print("arrival_window::phi: size=%d, _tlast=%ld, aw=%s\n", _arrival_intervals.size(), _tlast, *this);
    assert(_arrival_intervals.size() > 0 && _tlast > 0); // should not be called before any samples arrive
    long t = tnow - _tlast;
    return t / mean();
}

std::ostream& operator<<(std::ostream& os, const arrival_window& w) {
    os << "size = " << w._arrival_intervals.deque().size();
    for (auto& x : w._arrival_intervals.deque()) {
        os << x << " ";
    }
    return os;
}

sstring failure_detector::get_all_endpoint_states() {
    std::stringstream ss;
    for (auto& entry : get_local_gossiper().endpoint_state_map) {
        auto& ep = entry.first;
        auto& state = entry.second;
        ss << ep << "\n";
        append_endpoint_state(ss, state);
    }
    return sstring(ss.str());
}

std::map<sstring, sstring> failure_detector::get_simple_states() {
    std::map<sstring, sstring> nodes_status;
    for (auto& entry : get_local_gossiper().endpoint_state_map) {
        auto& ep = entry.first;
        auto& state = entry.second;
        std::stringstream ss;
        ss << ep;
        if (state.is_alive())
            nodes_status.emplace(sstring(ss.str()), "UP");
        else
            nodes_status.emplace(sstring(ss.str()), "DOWN");
    }
    return nodes_status;
}

int failure_detector::get_down_endpoint_count() {
    int count = 0;
    for (auto& entry : get_local_gossiper().endpoint_state_map) {
        auto& state = entry.second;
        if (!state.is_alive()) {
            count++;
        }
    }
    return count;
}

int failure_detector::get_up_endpoint_count() {
    int count = 0;
    for (auto& entry : get_local_gossiper().endpoint_state_map) {
        auto& state = entry.second;
        if (state.is_alive()) {
            count++;
        }
    }
    return count;
}

sstring failure_detector::get_endpoint_state(sstring address) {
    std::stringstream ss;
    auto eps = get_local_gossiper().get_endpoint_state_for_endpoint(inet_address(address));
    if (eps) {
        append_endpoint_state(ss, *eps);
        return sstring(ss.str());
    } else {
        return sstring("unknown endpoint ") + address;
    }
}

void failure_detector::append_endpoint_state(std::stringstream& ss, endpoint_state& state) {
    ss << "  generation:" << state.get_heart_beat_state().get_generation() << "\n";
    ss << "  heartbeat:" << state.get_heart_beat_state().get_heart_beat_version() << "\n";
    for (auto& entry : state.get_application_state_map()) {
        auto& app_state = entry.first;
        auto& value = entry.second;
        if (app_state == application_state::TOKENS) {
            continue;
        }
        // FIXME: Add operator<< for application_state
        ss << "  " << int32_t(app_state) << ":" << value.value << "\n";
    }
}

void failure_detector::set_phi_convict_threshold(double phi) {
    // FIXME
    // DatabaseDescriptor.setPhiConvictThreshold(phi);
}

double failure_detector::get_phi_convict_threshold() {
    // FIXME: phi_convict_threshold must be between 5 and 16"
    // return DatabaseDescriptor.getPhiConvictThreshold();
    warn(unimplemented::cause::GOSSIP);
    return 8;
}

bool failure_detector::is_alive(inet_address ep) {
    if (ep.is_broadcast_address()) {
        return true;
    }

    auto eps = get_local_gossiper().get_endpoint_state_for_endpoint(ep);
    // we could assert not-null, but having isAlive fail screws a node over so badly that
    // it's worth being defensive here so minor bugs don't cause disproportionate
    // badness.  (See CASSANDRA-1463 for an example).
    if (eps) {
        return eps->is_alive();
    } else {
        // logger.error("unknown endpoint {}", ep);
        return false;
    }
}

void failure_detector::report(inet_address ep) {
    // if (logger.isTraceEnabled())
    //     logger.trace("reporting {}", ep);
    long now = db_clock::now().time_since_epoch().count();
    auto it = _arrival_samples.find(ep);
    if (it == _arrival_samples.end()) {
        // avoid adding an empty ArrivalWindow to the Map
        auto heartbeat_window = arrival_window(SAMPLE_SIZE);
        heartbeat_window.add(now);
        _arrival_samples.emplace(ep, heartbeat_window);
        print("report A: ep=%s, now=%d, aw=%s\n", ep, now, heartbeat_window);
    } else {
        print("report B: ep=%s, now=%d\n", ep, now);
        it->second.add(now);
    }
}

void failure_detector::interpret(inet_address ep) {
    auto it = _arrival_samples.find(ep);
    if (it == _arrival_samples.end()) {
        print("interpret A: ep=%s\n", ep);
        return;
    }
    arrival_window& hb_wnd = it->second;
    long now = db_clock::now().time_since_epoch().count();
    double phi = hb_wnd.phi(now);
    // if (logger.isTraceEnabled())
    //     logger.trace("PHI for {} : {}", ep, phi);

    if (PHI_FACTOR * phi > get_phi_convict_threshold()) {
        print("interpret B: ep=%s, phi=%f\n", ep, phi);
        // logger.trace("notifying listeners that {} is down", ep);
        // logger.trace("intervals: {} mean: {}", hb_wnd, hb_wnd.mean());
        for (auto& listener : _fd_evnt_listeners) {
            listener->convict(ep, phi);
        }
    } else {
        print("interpret C: ep=%s, phi=%f\n", ep, phi);
    }
}

void failure_detector::force_conviction(inet_address ep) {
    //logger.debug("Forcing conviction of {}", ep);
    for (auto& listener : _fd_evnt_listeners) {
        listener->convict(ep, get_phi_convict_threshold());
    }
}

void failure_detector::remove(inet_address ep) {
    _arrival_samples.erase(ep);
}

void failure_detector::register_failure_detection_event_listener(shared_ptr<i_failure_detection_event_listener> listener) {
    _fd_evnt_listeners.push_back(std::move(listener));
}

void failure_detector::unregister_failure_detection_event_listener(shared_ptr<i_failure_detection_event_listener> listener) {
    _fd_evnt_listeners.remove(listener);
}

std::ostream& operator<<(std::ostream& os, const failure_detector& x) {
    os << "----------- failure_detector:    -----------\n";
    for (auto& entry : x._arrival_samples) {
        const inet_address& ep = entry.first;
        const arrival_window& win = entry.second;
        os << ep << " : "  << win << "\n";
    }
    return os;
}

} // namespace gms
