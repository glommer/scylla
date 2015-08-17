/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "core/reactor.hh"
#include "core/iostream.hh"
#include "core/distributed.hh"
#include "core/print.hh"
#include "core/sstring.hh"
#include "core/do_with.hh"
#include "net/api.hh"
#include "utils/serialization.hh"
#include "gms/inet_address.hh"
#include "rpc/rpc_types.hh"
#include <unordered_map>
#include "frozen_mutation.hh"
#include "query-request.hh"
#include "db/serializer.hh"
#include "mutation_query.hh"

// forward declarations
namespace streaming { namespace messages {
    class stream_init_message;
    class prepare_message;
}}

namespace gms {
    class gossip_digest_syn;
    class gossip_digest_ack;
    class gossip_digest_ack2;
}

class frozen_mutation;

namespace utils {
    class UUID;
}

namespace db {
class seed_provider_type;
}

namespace net {

/* All verb handler identifiers */
enum class messaging_verb : int32_t {
    MUTATION,
    MUTATION_DONE,
    BINARY, // Deprecated
    READ_REPAIR,
    READ,
    READ_DATA,
    READ_MUTATION_DATA, // urchin-only
    READ_DIGEST,
    REQUEST_RESPONSE, // client-initiated reads and writes
    STREAM_INITIATE, // Deprecated
    STREAM_INITIATE_DONE, // Deprecated
    STREAM_REPLY, // Deprecated
    STREAM_REQUEST, // Deprecated
    RANGE_SLICE,
    BOOTSTRAP_TOKEN, // Deprecated
    TREE_REQUEST, // Deprecated
    TREE_RESPONSE, // Deprecated
    JOIN, // Deprecated
    GOSSIP_DIGEST_SYN,
    GOSSIP_DIGEST_ACK,
    GOSSIP_DIGEST_ACK2,
    DEFINITIONS_ANNOUNCE, // Deprecated
    DEFINITIONS_UPDATE,
    TRUNCATE,
    SCHEMA_CHECK,
    INDEX_SCAN, // Deprecated
    REPLICATION_FINISHED,
    INTERNAL_RESPONSE, // responses to internal calls
    COUNTER_MUTATION,
    STREAMING_REPAIR_REQUEST, // Deprecated
    STREAMING_REPAIR_RESPONSE, // Deprecated
    SNAPSHOT, // Similar to nt snapshot
    MIGRATION_REQUEST,
    GOSSIP_SHUTDOWN,
    _TRACE,
    ECHO,
    REPAIR_MESSAGE,
    PAXOS_PREPARE,
    PAXOS_PROPOSE,
    PAXOS_COMMIT,
    PAGED_RANGE,
    UNUSED_1,
    UNUSED_2,
    UNUSED_3,
    // Used by streaming
    STREAM_INIT_MESSAGE,
    PREPARE_MESSAGE,
    PREPARE_DONE_MESSAGE,
    STREAM_MUTATION,
    STREAM_MUTATION_DONE,
    INCOMING_FILE_MESSAGE,
    OUTGOING_FILE_MESSAGE,
    RECEIVED_MESSAGE,
    RETRY_MESSAGE,
    COMPLETE_MESSAGE,
    SESSION_FAILED_MESSAGE,
    LAST,
};

} // namespace net

namespace std {
template <>
class hash<net::messaging_verb> {
public:
    size_t operator()(const net::messaging_verb& x) const {
        return hash<int32_t>()(int32_t(x));
    }
};
} // namespace std

namespace net {

// NOTE: operator(input_stream<char>&, T&) takes a reference to uninitialized
//       T object and should use placement new in case T is non POD
struct serializer {
    template <typename T, typename Input>
    inline T read_integral(Input& input) const {
        static_assert(std::is_integral<T>::value, "T should be integral");
        T data;
        input.read(reinterpret_cast<char*>(&data), sizeof(T));
        return net::ntoh(data);
    }

    template <typename T, typename Output>
    inline void write_integral(Output& output, T data) const {
        static_assert(std::is_integral<T>::value, "T should be integral");
        data = net::hton(data);
        output.write(reinterpret_cast<const char*>(&data), sizeof(T));
    }

    // Adaptor for writing objects having db::serializer<>
    template <typename Serializable, typename Output>
    inline void write_serializable(Output& out, const Serializable& v) const {
        db::serializer<Serializable> ser(v);
        bytes b(bytes::initialized_later(), ser.size() + data_output::serialized_size<uint32_t>());
        data_output d_out(b);
        d_out.write<uint32_t>(ser.size());
        ser.write(d_out);
        return out.write(reinterpret_cast<const char*>(b.c_str()), b.size());
    }

    // Adaptor for reading objects having db::serializer<>
    template <typename Serializable, typename Input>
    inline Serializable read_serializable(Input& in) const {
        auto sz = read_integral<uint32_t>(in);
        bytes data(bytes::initialized_later(), sz);
        in.read(reinterpret_cast<char*>(data.begin()), sz);
        data_input din(data);
        return db::serializer<Serializable>::read(din);
    }

    // For integer type
    template <typename Input>
    bool read(Input& input, rpc::type<bool>) const { return read(input, rpc::type<uint8_t>()); }
    template <typename Input>
    int8_t read(Input& input, rpc::type<int8_t>) const { return read_integral<int8_t>(input); }
    template <typename Input>
    uint8_t read(Input& input, rpc::type<uint8_t>) const { return read_integral<uint8_t>(input); }
    template <typename Input>
    int16_t read(Input& input, rpc::type<int16_t>) const { return read_integral<int16_t>(input); }
    template <typename Input>
    uint16_t read(Input& input, rpc::type<uint16_t>) const { return read_integral<uint16_t>(input); }
    template <typename Input>
    int32_t read(Input& input, rpc::type<int32_t>) const { return read_integral<int32_t>(input); }
    template <typename Input>
    uint32_t read(Input& input, rpc::type<uint32_t>) const { return read_integral<uint32_t>(input); }
    template <typename Input>
    int64_t read(Input& input, rpc::type<int64_t>) const { return read_integral<int64_t>(input); }
    template <typename Input>
    uint64_t read(Input& input, rpc::type<uint64_t>) const { return read_integral<uint64_t>(input); }
    template <typename Output>
    void write(Output& output, bool data) const { write(output, uint8_t(data)); }
    template <typename Output>
    void write(Output& output, int8_t data) const { write_integral(output, data); }
    template <typename Output>
    void write(Output& output, uint8_t data) const { write_integral(output, data); }
    template <typename Output>
    void write(Output& output, int16_t data) const { write_integral(output, data); }
    template <typename Output>
    void write(Output& output, uint16_t data) const { write_integral(output, data); }
    template <typename Output>
    void write(Output& output, int32_t data) const { write_integral(output, data); }
    template <typename Output>
    void write(Output& output, uint32_t data) const { write_integral(output, data); }
    template <typename Output>
    void write(Output& output, int64_t data) const { write_integral(output, data); }
    template <typename Output>
    void write(Output& output, uint64_t data) const { write_integral(output, data); }

    // For vectors
    template <typename T, typename Output>
    inline void write(Output& out, const std::vector<T>& v) const {
        write(out, uint32_t(v.size()));
        for (auto&& e : v) {
            write(out, e);
        }
    }
    template <typename T, typename Input>
    inline std::vector<T> read(Input& in, rpc::type<std::vector<T>>) const {
        auto sz = read(in, rpc::type<uint32_t>());
        std::vector<T> v;
        v.reserve(sz);
        while (sz--) {
            v.push_back(read(in, rpc::type<T>()));
        }
        return v;
    }

    // For messaging_verb
    template <typename Output>
    void write(Output& out, messaging_verb v) const {
        return write(out, std::underlying_type_t<messaging_verb>(v));
    }
    template <typename Input>
    messaging_verb operator()(Input& in, rpc::type<messaging_verb>) const {
        return messaging_verb(read(in, rpc::type<std::underlying_type_t<messaging_verb>>()));
    }

    // For sstring
    template <typename Output>
    void write(Output& out, const sstring& v) const {
        write(out, uint32_t(v.size()));
        out.write(v.begin(), v.size());
    }
    template <typename Input>
    sstring read(Input& in, rpc::type<sstring>) const {
        auto sz = read(in, rpc::type<uint32_t>());
        sstring v(sstring::initialized_later(), sz);
        in.read(v.begin(), sz);
        return v;
    }

    // For frozen_mutation
    template <typename Output>
    void write(Output& out, const frozen_mutation& v) const{
        return write_serializable(out, v);
    }
    template <typename Input>
    frozen_mutation read(Input& in, rpc::type<frozen_mutation>) const {
        return read_serializable<frozen_mutation>(in);
    }

    // For reconcilable_result
    template <typename Output>
    void write(Output& out, const reconcilable_result& v) const{
        return write_serializable(out, v);
    }
    template <typename Input>
    reconcilable_result read(Input& in, rpc::type<reconcilable_result>) const {
        return read_serializable<reconcilable_result>(in);
    }

    // For complex types which have serialize()/deserialize(),  e.g. gms::gossip_digest_syn, gms::gossip_digest_ack2
    template <typename T, typename Output>
    void write_gms(Output& out, const T& v) const {
        uint32_t sz = v.serialized_size();
        write(out, sz);
        bytes b(bytes::initialized_later(), sz);
        auto _out = b.begin();
        v.serialize(_out);
        out.write(reinterpret_cast<const char*>(b.c_str()), sz);
    }
    template <typename T, typename Input>
    T read_gms(Input& in) const {
        auto sz = read(in, rpc::type<uint32_t>());
        bytes b(bytes::initialized_later(), sz);
        in.read(reinterpret_cast<char*>(b.begin()), sz);
        bytes_view bv(b);
        return T::deserialize(bv);
    }

    template <typename Output>
    void write(Output& out, const gms::gossip_digest_syn& v) const;
    template <typename Input>
    gms::gossip_digest_syn read(Input& in, rpc::type<gms::gossip_digest_syn>) const;

    template <typename Output>
    void write(Output& out, const gms::gossip_digest_ack2& v) const;
    template <typename Input>
    gms::gossip_digest_ack2 read(Input& in, rpc::type<gms::gossip_digest_ack2>) const;

    template <typename Output>
    void write(Output& out, const streaming::messages::stream_init_message& v) const;
    template <typename Input>
    streaming::messages::stream_init_message read(Input& in, rpc::type<streaming::messages::stream_init_message>) const;

    template <typename Output>
    void write(Output& out, const streaming::messages::prepare_message& v) const;
    template <typename Input>
    streaming::messages::prepare_message read(Input& in, rpc::type<streaming::messages::prepare_message>) const;

    template <typename Output>
    void write(Output& out, const gms::inet_address& v) const;
    template <typename Input>
    gms::inet_address read(Input& in, rpc::type<gms::inet_address>) const;

    template <typename Output>
    void write(Output& out, const gms::gossip_digest_ack& v) const;
    template <typename Input>
    gms::gossip_digest_ack read(Input& in, rpc::type<gms::gossip_digest_ack>) const;

    template <typename Output>
    void write(Output& out, const query::read_command& v) const;
    template <typename Input>
    query::read_command read(Input& in, rpc::type<query::read_command>) const;

    template <typename Output>
    void write(Output& out, const query::result& v) const;
    template <typename Input>
    query::result read(Input& in, rpc::type<query::result>) const;

    template <typename Output>
    void write(Output& out, const query::result_digest& v) const;
    template <typename Input>
    query::result_digest read(Input& in, rpc::type<query::result_digest>) const;

    template <typename Output>
    void write(Output& out, const utils::UUID& v) const;
    template <typename Input>
    utils::UUID read(Input& in, rpc::type<utils::UUID>) const;

    // for query::range<T>
    template <typename Output, typename T>
    void write(Output& out, const query::range<T>& v) const;
    template <typename Input, typename T>
    query::range<T> read(Input& input, rpc::type<query::range<T>>) const;

    template <typename Output, typename T>
    void write(Output& out, const foreign_ptr<T>& v) const {
        return write(out, *v);
    }
    template <typename Input, typename T>
    foreign_ptr<T> read(Input& in, rpc::type<foreign_ptr<T>>) const {
        return make_foreign(read(in, rpc::type<T>()));
    }

    template <typename Output, typename T>
    void write(Output& out, const lw_shared_ptr<T>& v) const {
        return write(out, *v);
    }
    template <typename Input, typename T>
    lw_shared_ptr<T> read(Input& in, rpc::type<lw_shared_ptr<T>>) const {
        return make_lw_shared(read(in, rpc::type<T>()));
    }
};

// thunk from new-style free function serialization to old-style member function
template <typename Output, typename T>
inline
void
write(serializer s, Output& out, const T& data) {
    s.write(out, data);
}

template <typename Input, typename T>
inline
T
read(serializer s, Input& in, rpc::type<T> type) {
    return s.read(in, type);
}

struct shard_id {
    gms::inet_address addr;
    uint32_t cpu_id;
    friend bool operator==(const shard_id& x, const shard_id& y);
    friend bool operator<(const shard_id& x, const shard_id& y);
    friend std::ostream& operator<<(std::ostream& os, const shard_id& x);
    struct hash {
        size_t operator()(const shard_id& id) const;
    };
};

class messaging_service {
public:
    using shard_id = net::shard_id;
    using inet_address = gms::inet_address;
    using UUID = utils::UUID;

    struct rpc_protocol_wrapper;
    struct rpc_protocol_client_wrapper;
    struct rpc_protocol_server_wrapper;

    // FIXME: messaging service versioning
    static constexpr int32_t current_version = 0;

    struct shard_info {
        shard_info(std::unique_ptr<rpc_protocol_client_wrapper>&& client);
        std::unique_ptr<rpc_protocol_client_wrapper> rpc_client;
        rpc::stats get_stats() const;
    };

    void foreach_client(std::function<void(const shard_id& id, const shard_info& info)> f) const;

    void increment_dropped_messages(messaging_verb verb);

    uint64_t get_dropped_messages(messaging_verb verb) const;

    const uint64_t* get_dropped_messages() const;

    int32_t get_raw_version(const gms::inet_address& endpoint) const;

    bool knows_version(const gms::inet_address& endpoint) const;

private:
    static constexpr uint16_t _default_port = 7000;
    gms::inet_address _listen_address;
    uint16_t _port;
    std::unique_ptr<rpc_protocol_wrapper> _rpc;
    std::unique_ptr<rpc_protocol_server_wrapper> _server;
    std::unordered_map<shard_id, shard_info, shard_id::hash> _clients;
    uint64_t _dropped_messages[static_cast<int32_t>(messaging_verb::LAST)] = {};
public:
    messaging_service(gms::inet_address ip = gms::inet_address("0.0.0.0"));
    ~messaging_service();
public:
    uint16_t port();
    gms::inet_address listen_address();
    future<> stop();
    static rpc::no_wait_type no_wait();
public:
    // Wrapper for STREAM_INIT_MESSAGE verb
    void register_stream_init_message(std::function<future<unsigned> (streaming::messages::stream_init_message msg, unsigned src_cpu_id)>&& func);
    future<unsigned> send_stream_init_message(shard_id id, streaming::messages::stream_init_message msg, unsigned src_cpu_id);

    // Wrapper for PREPARE_MESSAGE verb
    void register_prepare_message(std::function<future<streaming::messages::prepare_message> (streaming::messages::prepare_message msg, UUID plan_id,
        inet_address from, inet_address connecting, unsigned src_cpu_id, unsigned dst_cpu_id)>&& func);
    future<streaming::messages::prepare_message> send_prepare_message(shard_id id, streaming::messages::prepare_message msg, UUID plan_id,
        inet_address from, inet_address connecting, unsigned src_cpu_id, unsigned dst_cpu_id);

    // Wrapper for PREPARE_DONE_MESSAGE verb
    void register_prepare_done_message(std::function<future<> (UUID plan_id, inet_address from, inet_address connecting, unsigned dst_cpu_id)>&& func);
    future<> send_prepare_done_message(shard_id id, UUID plan_id, inet_address from, inet_address connecting, unsigned dst_cpu_id);

    // Wrapper for STREAM_MUTATION verb
    void register_stream_mutation(std::function<future<> (UUID plan_id, frozen_mutation fm, unsigned dst_cpu_id)>&& func);
    future<> send_stream_mutation(shard_id id, UUID plan_id, frozen_mutation fm, unsigned dst_cpu_id);

    void register_stream_mutation_done(std::function<future<> (UUID plan_id, UUID cf_id, inet_address from, inet_address connecting, unsigned dst_cpu_id)>&& func);
    future<> send_stream_mutation_done(shard_id id, UUID plan_id, UUID cf_id, inet_address from, inet_address connecting, unsigned dst_cpu_id);

    void register_complete_message(std::function<rpc::no_wait_type (UUID plan_id, inet_address from, inet_address connecting, unsigned dst_cpu_id)>&& func);
    future<> send_complete_message(shard_id id, UUID plan_id, inet_address from, inet_address connecting, unsigned dst_cpu_id);

    // Wrapper for ECHO verb
    void register_echo(std::function<future<> ()>&& func);
    future<> send_echo(shard_id id);

    // Wrapper for GOSSIP_SHUTDOWN
    void register_gossip_shutdown(std::function<rpc::no_wait_type (inet_address from)>&& func);
    future<> send_gossip_shutdown(shard_id id, inet_address from);

    // Wrapper for GOSSIP_DIGEST_SYN
    void register_gossip_digest_syn(std::function<future<gms::gossip_digest_ack> (gms::gossip_digest_syn)>&& func);
    future<gms::gossip_digest_ack> send_gossip_digest_syn(shard_id id, gms::gossip_digest_syn msg);

    // Wrapper for GOSSIP_DIGEST_ACK2
    void register_gossip_digest_ack2(std::function<rpc::no_wait_type (gms::gossip_digest_ack2)>&& func);
    future<> send_gossip_digest_ack2(shard_id id, gms::gossip_digest_ack2 msg);

    // Wrapper for DEFINITIONS_UPDATE
    void register_definitions_update(std::function<rpc::no_wait_type (std::vector<frozen_mutation> fm)>&& func);
    future<> send_definitions_update(shard_id id, std::vector<frozen_mutation> fm);

    // Wrapper for MIGRATION_REQUEST
    void register_migration_request(std::function<future<std::vector<frozen_mutation>> (gms::inet_address reply_to, unsigned shard)>&& func);
    future<std::vector<frozen_mutation>> send_migration_request(shard_id id, gms::inet_address reply_to, unsigned shard);

    // FIXME: response_id_type is an alias in service::storage_proxy::response_id_type
    using response_id_type = uint64_t;
    // Wrapper for MUTATION
    void register_mutation(std::function<rpc::no_wait_type (frozen_mutation fm, std::vector<inet_address> forward,
        inet_address reply_to, unsigned shard, response_id_type response_id)>&& func);
    future<> send_mutation(shard_id id, const frozen_mutation& fm, std::vector<inet_address> forward,
        inet_address reply_to, unsigned shard, response_id_type response_id);

    // Wrapper for MUTATION_DONE
    void register_mutation_done(std::function<rpc::no_wait_type (const rpc::client_info& cinfo, unsigned shard, response_id_type response_id)>&& func);
    future<> send_mutation_done(shard_id id, unsigned shard, response_id_type response_id);

    // Wrapper for READ_DATA
    // Note: WTH is future<foreign_ptr<lw_shared_ptr<query::result>>
    void register_read_data(std::function<future<foreign_ptr<lw_shared_ptr<query::result>>> (query::read_command cmd, query::partition_range pr)>&& func);
    future<query::result> send_read_data(shard_id id, query::read_command& cmd, query::partition_range& pr);

    // Wrapper for READ_MUTATION_DATA
    void register_read_mutation_data(std::function<future<foreign_ptr<lw_shared_ptr<reconcilable_result>>> (query::read_command cmd, query::partition_range pr)>&& func);
    future<reconcilable_result> send_read_mutation_data(shard_id id, query::read_command& cmd, query::partition_range& pr);

    // Wrapper for READ_DIGEST
    void register_read_digest(std::function<future<query::result_digest> (query::read_command cmd, query::partition_range pr)>&& func);
    future<query::result_digest> send_read_digest(shard_id id, query::read_command& cmd, query::partition_range& pr);

public:
    // Return rpc::protocol::client for a shard which is a ip + cpuid pair.
    rpc_protocol_client_wrapper& get_rpc_client(shard_id id);
    void remove_rpc_client(shard_id id);
    std::unique_ptr<rpc_protocol_wrapper>& rpc();
};

extern distributed<messaging_service> _the_messaging_service;

inline distributed<messaging_service>& get_messaging_service() {
    return _the_messaging_service;
}

inline messaging_service& get_local_messaging_service() {
    return _the_messaging_service.local();
}

future<> init_messaging_service(sstring listen_address, db::seed_provider_type seed_provider);
} // namespace net
