/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include <iostream>
#include "core/function_traits.hh"
#include "core/apply.hh"
#include "core/shared_ptr.hh"
#include "core/sstring.hh"
#include "core/future-util.hh"

namespace rpc {

template<std::size_t N, typename Serializer, typename... T>
inline std::enable_if_t<N == sizeof...(T), future<>> marshall(Serializer&, output_stream<char>&, std::tuple<T...>&&) {
    return make_ready_future<>();
}

template<std::size_t N = 0, typename Serializer, typename... T>
inline std::enable_if_t<N != sizeof...(T), future<>> marshall(Serializer& serialize, output_stream<char>& out, std::tuple<T...>&& args) {
    using tuple_type = std::tuple<T...>;
    return serialize(out, std::forward<typename std::tuple_element<N, tuple_type>::type>(std::get<N>(args))).then([&serialize, &out, args = std::move(args)] () mutable {
        return marshall<N + 1>(serialize, out, std::move(args));
    }).then([&out] {
        return out.flush();
    });
}

template<std::size_t N, typename Serializer, typename... T>
inline std::enable_if_t<N == sizeof...(T), future<>> unmarshall(Serializer&, input_stream<char>&, std::tuple<T&...>&&) {
    return make_ready_future<>();
}

template<std::size_t N = 0, typename Serializer, typename... T>
inline std::enable_if_t<N != sizeof...(T), future<>> unmarshall(Serializer& deserialize, input_stream<char>& in, std::tuple<T&...>&& args) {
    return deserialize(in, std::get<N>(args)).then([&deserialize, &in, args = std::move(args)] () mutable {
        return unmarshall<N + 1>(deserialize, in, std::move(args));
    });
}

// ref_tuple gets tuple and returns another tuple with references to members of received tuple
template<typename... T, std::size_t... I>
inline std::tuple<T&...> ref_tuple_impl(std::tuple<T...>& t, std::index_sequence<I...>) {
    return std::tie(std::get<I>(t)...);
}

template<typename... T>
inline std::tuple<T&...> ref_tuple(std::tuple<T...>& t) {
    return ref_tuple_impl(t, std::make_index_sequence<sizeof...(T)>());
}

struct reply_payload_base {
    sstring ex;
};

template <typename T>
struct reply_payload : reply_payload_base {
    T v;
};

template<>
struct reply_payload<void> : reply_payload_base {
};

template<typename Payload, typename... T>
struct rcv_reply_base : reply_payload<Payload> {
    bool done = false;
    promise<T...> p;
    template<typename... V>
    void set_value(V&&... v) {
        done = true;
        p.set_value(std::forward<V>(v)...);
    }
    ~rcv_reply_base() {
        if (!done) {
            p.set_exception(closed_error());
        }
    }
};

template<typename Serializer, typename MsgType, typename T>
struct rcv_reply : rcv_reply_base<T, T> {
    inline future<> get_reply(typename protocol<Serializer, MsgType>::client& dst) {
        return unmarshall(dst.serializer(), dst.in(), std::tie(this->v)).then([this] {
            this->set_value(this->v);
        });
    }
};

template<typename Serializer, typename MsgType, typename... T>
struct rcv_reply<Serializer, MsgType, future<T...>> : rcv_reply_base<std::tuple<T...>, T...> {
    inline future<> get_reply(typename protocol<Serializer, MsgType>::client& dst) {
        return unmarshall(dst.serializer(), dst.in(), ref_tuple(this->v)).then([this] {
            this->set_value(this->v);
        });
    }
};

template<typename Serializer, typename MsgType>
struct rcv_reply<Serializer, MsgType, void> : rcv_reply_base<void, void> {
    inline future<> get_reply(typename protocol<Serializer, MsgType>::client& dst) {
        this->set_value();
        return make_ready_future<>();
    }
};

template<typename MsgType, typename... T>
struct message {
    MsgType t;
    id_type id = 0;
    std::tuple<T...> args;
    message() = default;
    message(MsgType xt, id_type xid, T&&... xargs) : t(xt), id(xid), args(std::forward<T>(xargs)...) {}
};

template<typename T1, typename T2>
inline void assert_type() {
    static_assert(std::is_convertible<T1, T2>::value, "wrong type");
}

// helpers to calculate types of message::args tuple for sending
// At is a type provided to rpc call
// Ft is a type that rpc handler expects
// If At is a lvalue reference the message type is Ft&, not need to copy an object into a message
// If At is an rvalue reference the type is Ft since value will be copied into it
// assert_type() will check that values are convertible before assigning
template<typename Ft, typename At>
struct build_msg_type;

template<typename Ft, typename At>
struct build_msg_type<Ft, At&> {
    typedef Ft& type;
};

template<typename Ft, typename At>
struct build_msg_type<Ft, At&&> {
    typedef Ft type;
};

template<typename Ret, typename Serializer, typename MsgType>
inline auto wait_for_reply(typename protocol<Serializer, MsgType>::client& dst, id_type msg_id, future<> sent, std::enable_if_t<!std::is_same<Ret, no_wait_type>::value, void*> = nullptr) {
    sent.finally([]{}); // discard result or exception, this path does not need to wait for message to be send
    using reply_type = rcv_reply<Serializer, MsgType, Ret>;
    auto lambda = [] (reply_type& r, typename protocol<Serializer, MsgType>::client& dst, id_type msg_id) mutable {
        if (msg_id >= 0) {
            return r.get_reply(dst);
        } else {
            return unmarshall(dst.serializer(), dst.in(), std::tie(r.ex)).then([&r] {
                r.done = true;
                r.p.set_exception(std::runtime_error(r.ex.c_str()));
            });
        }
    };
    using handler_type = typename protocol<Serializer, MsgType>::client::template reply_handler<reply_type, decltype(lambda)>;
    auto r = std::make_unique<handler_type>(std::move(lambda));
    auto fut = r->reply.p.get_future();
    dst.wait_for_reply(msg_id, std::move(r));
    return fut;
}

template<typename Ret, typename Serializer, typename MsgType>
inline auto wait_for_reply(typename protocol<Serializer, MsgType>::client& dst, id_type msg_id, future<>&& sent, std::enable_if_t<std::is_same<Ret, no_wait_type>::value, void*> = nullptr) {
    return std::move(sent);
}

// Returns lambda that can be used to send rpc messages.
// The lambda gets client connection and rpc parameters as arguments, marshalls them sends
// to a server and waits for a reply. After receiving reply it unmarshalls it and signal completion
// to a caller.
template<typename F, typename Serializer, typename MsgType, std::size_t... I>
auto send_helper(MsgType t, std::index_sequence<I...>) {
    return [t](typename protocol<Serializer, MsgType>::client& dst, auto&&... args) {
        // check that number and type of parameters match registered remote function
        static_assert(sizeof...(I) == sizeof...(args), "wrong number of parameters");
        using types = std::tuple<typename build_msg_type<typename F::template arg<I>::type, decltype(args)>::type...>;
        int _[] = { 0, (assert_type<decltype(args), typename std::tuple_element<I, types>::type>(), 0)... }; (void)_;

        if (dst.error()) {
            throw closed_error();
        }

        // send message
        auto msg_id = dst.next_message_id();
        auto m = std::make_unique<message<MsgType, typename std::tuple_element<I, types>::type...>>(t, msg_id, std::forward<decltype(args)>(args)...);
        auto xargs = std::tie(m->t, m->id, std::get<I>(m->args)...); // holds references to all message elements
        promise<> sent; // will be fulfilled when data is sent
        auto fsent = sent.get_future();
        dst.out_ready() = dst.out_ready().then([&dst, xargs = std::move(xargs), m = std::move(m), sent = std::move(sent)] () mutable {
            return marshall(dst.serializer(), dst.out(), std::move(xargs)).then([m = std::move(m)] {});
        }).finally([sent = std::move(sent)] () mutable {
            sent.set_value();
        });

        // prepare reply handler, if return type is now_wait_type this does nothing, since no reply will be sent
        return wait_for_reply<typename F::return_type, Serializer, MsgType>(dst, msg_id, std::move(fsent));
    };
}

template<typename Serializer, typename MsgType, typename Payload>
struct snd_reply_base : reply_payload<Payload> {
    id_type id;

    snd_reply_base(id_type xid) : id(xid) {}
    inline future<> send_ex(typename protocol<Serializer, MsgType>::server::connection& client) {
        return marshall(client.serializer(), client.out(), std::tie(id, this->ex));
    }
};

template<typename Serializer, typename MsgType, typename T>
struct snd_reply : snd_reply_base<Serializer, MsgType, T> {
    snd_reply(id_type xid) : snd_reply_base<Serializer, MsgType, T>(xid) {}
    inline void set_val(std::tuple<T>&& val) {
        this->v = std::move(std::get<0>(val));
    }
    inline future<> reply(typename protocol<Serializer, MsgType>::server::connection& client) {
        return marshall(client.serializer(), client.out(), std::tie(this->id, this->v));
    }
};

template<typename Serializer, typename MsgType, typename... T>
struct snd_reply<Serializer, MsgType, future<T...>> : snd_reply_base<Serializer, MsgType, std::tuple<T...>> {
    snd_reply(id_type xid) : snd_reply_base<Serializer, MsgType, std::tuple<T...>>(xid) {}
    inline void set_val(std::tuple<T...>&& val) {
        this->v = std::move(val);
    }
    inline future<> reply(typename protocol<Serializer, MsgType>::server::connection& client) {
        return marshall(client.serializer(), client.out(), std::tuple_cat(std::tie(this->id), ref_tuple(this->v)));
    }
};

template<typename Serializer, typename MsgType>
struct snd_reply<Serializer, MsgType, void> : snd_reply_base<Serializer, MsgType, void> {
    snd_reply(id_type xid) : snd_reply_base<Serializer, MsgType, void>(xid) {}
    inline void set_val(std::tuple<>&& val) {
    }
    inline future<> reply(typename protocol<Serializer, MsgType>::server::connection& client) {
        return marshall(client.serializer(), client.out(), std::tie(this->id));
    }
};

// specialization for no_wait_type which does not send a reply
template<typename Serializer, typename MsgType>
struct snd_reply<Serializer, MsgType, no_wait_type> : snd_reply_base<Serializer, MsgType, no_wait_type> {
    snd_reply(id_type xid) : snd_reply_base<Serializer, MsgType, no_wait_type>(xid) {}
    inline void set_val(std::tuple<no_wait_type>&& val) {
    }
    inline future<> reply(typename protocol<Serializer, MsgType>::server::connection& client) {
        return make_ready_future<>();
    }
    inline future<> send_ex(typename protocol<Serializer, MsgType>::server::connection& client) {
        client.get_protocol().log(client.info(), -this->id, to_sstring("exception \"") + this->ex + "\" in no_wait handler ignored");
        return make_ready_future<>();
    }
};


template<typename Serializer, typename MsgType, typename Ret>
inline future<> reply(std::unique_ptr<snd_reply<Serializer, MsgType, Ret>>& r, typename protocol<Serializer, MsgType>::server::connection& client) {
    if (r->id < 0) {
        return r->send_ex(client);
    } else {
        return r->reply(client);
    }
}

// build callback arguments tuple depending on whether it gets client_info as a first parameter
template<bool Info, typename MsgType, typename... M>
inline auto make_apply_args(client_info& info, std::unique_ptr<message<MsgType, M...>>& m, std::enable_if_t<!Info, void*> = nullptr) {
    return std::move(m->args);
}

template<bool Info, typename MsgType, typename... M>
inline auto make_apply_args(client_info& info, std::unique_ptr<message<MsgType, M...>>& m, std::enable_if_t<Info, void*> = nullptr) {
    return std::tuple_cat(std::make_tuple(std::cref(info)), std::move(m->args));
}

template<typename Ret, bool Info, typename Serializer, typename MsgType, typename Func, typename... M>
inline future<std::unique_ptr<snd_reply<Serializer, MsgType, Ret>>> apply(Func& func, client_info& info, std::unique_ptr<message<MsgType, M...>>&& m) {
    using futurator = futurize<Ret>;
    auto r = std::make_unique<snd_reply<Serializer, MsgType, Ret>>(m->id);
    try {
        auto f = futurator::apply(func, make_apply_args<Info>(info, m));
        return f.then_wrapped([r = std::move(r)] (typename futurator::type ret) mutable {
            try {
                r->set_val(std::move(ret.get()));
            } catch(std::runtime_error& ex) {
                r->id = -r->id;
                r->ex = ex.what();
            }
            return make_ready_future<std::unique_ptr<snd_reply<Serializer, MsgType, Ret>>>(std::move(r));
        });
    } catch (std::runtime_error& ex) {
        r->id = -r->id;
        r->ex = ex.what();
        return make_ready_future<std::unique_ptr<snd_reply<Serializer, MsgType, Ret>>>(std::move(r));
    }
}

// lref_to_cref is a helper that encapsulates lvalue reference in std::ref() or does nothing otherwise
template<typename T>
auto lref_to_cref(T&& x) {
    return std::move(x);
}

template<typename T>
auto lref_to_cref(T& x) {
    return std::ref(x);
}

// Creates lambda to handle RPC message on a server.
// The lambda unmarshalls all parameters, calls a handler, marshall return values and sends them back to a client
template<typename F, typename Serializer, typename MsgType, bool Info, typename Func, std::size_t... I>
auto recv_helper(std::index_sequence<I...>, Func&& func) {
    return [func = lref_to_cref(std::forward<Func>(func))](lw_shared_ptr<typename protocol<Serializer, MsgType>::server::connection> client) mutable {
        // create message to hold all received values
        auto m = std::make_unique<message<MsgType, typename F::template arg<I>::type...>>();
        auto xargs = std::tie(m->id, std::get<I>(m->args)...); // holds reference to all message elements
        return unmarshall(client->serializer(), client->in(), std::move(xargs)).then([client, m = std::move(m), &func] () mutable {
            // note: apply is executed asynchronously with regards to networking so we cannot chain futures here by doing "return apply()"
            apply<typename F::return_type, Info, Serializer>(func, client->info(), std::move(m)).then([client] (std::unique_ptr<snd_reply<Serializer, MsgType, typename F::return_type>>&& r) {
                client->out_ready() = client->out_ready().then([client, r = std::move(r)] () mutable {
                    auto f = reply(r, *client);
                    // hold on r and client while reply is sent
                    return f.then([client, r = std::move(r)] {});
                });
            });
        });
    };
}

// helper to create copy constructible lambda from non copy constructible one. std::function<> works only with former kind.
template<typename Func>
auto make_copyable_function(Func&& func, std::enable_if_t<!std::is_copy_constructible<std::decay_t<Func>>::value, void*> = nullptr) {
  auto p = make_lw_shared<typename std::decay_t<Func>>(std::forward<Func>(func));
  return [p] (auto&&... args) { return (*p)( std::forward<decltype(args)>(args)... ); };
}

template<typename Func>
auto make_copyable_function(Func&& func, std::enable_if_t<std::is_copy_constructible<std::decay_t<Func>>::value, void*> = nullptr) {
    return std::forward<Func>(func);
}

template<typename Ret, typename... Args>
struct client_type_helper {
    using type = Ret(Args...);
    static constexpr bool info = false;
};

template<typename Ret, typename First, typename... Args>
struct client_type_helper<Ret, First, Args...> {
    using type = Ret(First, Args...);
    static constexpr bool info = false;
    static_assert(!std::is_same<client_info&, First>::value, "reference to client_info has to be const");
};

template<typename Ret, typename... Args>
struct client_type_helper<Ret, const client_info&, Args...> {
    using type = Ret(Args...);
    static constexpr bool info = true;
};

template<typename Ret, typename... Args>
struct client_type_helper<Ret, client_info, Args...> {
    using type = Ret(Args...);
    static constexpr bool info = true;
};

template<typename F, typename I>
struct client_type_impl;

template<typename F, std::size_t... I>
struct client_type_impl<F, std::integer_sequence<std::size_t, I...>> {
    using type = client_type_helper<typename F::return_type, typename F::template arg<I>::type...>;
};

// this class is used to calculate client side rpc function signature
// if rpc callback receives client_info as a first parameter it is dropped
// from an argument list, otherwise signature is identical to what was passed to
// make_client()
template<typename Func>
class client_type {
    using trait = function_traits<Func>;
    using ctype = typename client_type_impl<trait, std::make_index_sequence<trait::arity>>::type;
public:
    using type = typename ctype::type; // client function signature
    static constexpr bool info = ctype::info; // true if client_info is a first parameter of rpc handler
};

template<typename Serializer, typename MsgType>
template<typename Func>
auto protocol<Serializer, MsgType>::make_client(MsgType t) {
    using trait = function_traits<typename client_type<Func>::type>;
    return send_helper<trait, Serializer>(t, std::make_index_sequence<trait::arity>());
}

template<typename Serializer, typename MsgType>
template<typename Func>
auto protocol<Serializer, MsgType>::register_handler(MsgType t, Func&& func) {
    constexpr auto info = client_type<Func>::info;
    using trait = function_traits<typename client_type<Func>::type>;
    auto recv = recv_helper<trait, Serializer, MsgType, info>(std::make_index_sequence<trait::arity>(), std::forward<Func>(func));
    register_receiver(t, make_copyable_function(std::move(recv)));
    return make_client<Func>(t);
}

template<typename Serializer, typename MsgType>
protocol<Serializer, MsgType>::server::server(protocol<Serializer, MsgType>& proto, ipv4_addr addr) : _proto(proto) {
    listen_options lo;
    lo.reuse_address = true;
    accept(engine().listen(make_ipv4_address(addr), lo));
}

template<typename Serializer, typename MsgType>
void protocol<Serializer, MsgType>::server::accept(server_socket&& ss) {
    keep_doing([this, ss = std::move(ss)] () mutable {
        return ss.accept().then([this] (connected_socket fd, socket_address addr) mutable {
            auto conn = make_lw_shared<connection>(*this, std::move(fd), std::move(addr), _proto);
            conn->process();
        });
    });
}

template<typename Serializer, typename MsgType>
protocol<Serializer, MsgType>::server::connection::connection(protocol<Serializer, MsgType>::server& s, connected_socket&& fd, socket_address&& addr, protocol<Serializer, MsgType>& proto)
    : protocol<Serializer, MsgType>::connection(std::move(fd), proto), _server(s) {
    _info.addr = std::move(addr);
}

template<typename Serializer, typename MsgType>
future<> protocol<Serializer, MsgType>::server::connection::process() {
    return do_until([this] { return this->_read_buf.eof() || this->_error; }, [this] () mutable {
        return unmarshall(this->serializer(), this->_read_buf, std::tie(_type)).then([this] {
            auto it = _server._proto._handlers.find(_type);
            if (it != _server._proto._handlers.end()) {
                return it->second(this->shared_from_this());
            } else {
                this->_error = true;
                return make_ready_future<>();
            }
        });
    }).finally([conn_ptr = this->shared_from_this()] () {
        // hold onto connection pointer until do_until() exists
    });
}

template<typename Serializer, typename MsgType>
protocol<Serializer, MsgType>::client::client(protocol<Serializer, MsgType>& proto, ipv4_addr addr) : protocol<Serializer, MsgType>::connection(proto) {
    this->_output_ready = _connected.get_future();
    engine().net().connect(make_ipv4_address(addr)).then([this] (connected_socket fd) {
        this->_fd = std::move(fd);
        this->_read_buf = this->_fd.input();
        this->_write_buf = this->_fd.output();
        this->_connected.set_value();
        do_until([this] { return this->_read_buf.eof() || this->_error; }, [this] () mutable {
            return unmarshall(this->serializer(), this->_read_buf, std::tie(_rcv_msg_id)).then([this] {
                auto it = _outstanding.find(::abs(_rcv_msg_id));
                if (it != _outstanding.end()) {
                    auto handler = std::move(it->second);
                    _outstanding.erase(it);
                    auto f = (*handler)(*this, _rcv_msg_id);
                    // hold on to handler so it will not be deleted before reply is received
                    return f.then([handler = std::move(handler)] {});
                } else {
                    this->_error = true;
                    return make_ready_future<>();
                }
            });
        }).finally([this] () {
            this->_write_buf.close();
            _outstanding.clear();
        });
    });
}

}
