/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "cql3_type.hh"

namespace cql3 {

class cql3_type::raw_type : public raw {
private:
    shared_ptr<cql3_type> _type;
public:
    raw_type(shared_ptr<cql3_type> type)
        : _type{type}
    { }
public:
    virtual shared_ptr<cql3_type> prepare(database& db, const sstring& keyspace) {
        return _type;
    }

    virtual bool supports_freezing() const {
        return false;
    }

    virtual bool is_counter() const {
        throw std::runtime_error("not implemented");
#if 0
        return type == Native.COUNTER;
#endif
    }

    virtual sstring to_string() const {
        return _type->to_string();
    }
};

class cql3_type::raw_collection : public raw {
    const collection_type_impl::kind* _kind;
    shared_ptr<raw> _keys;
    shared_ptr<raw> _values;
public:
    raw_collection(const collection_type_impl::kind* kind, shared_ptr<raw> keys, shared_ptr<raw> values)
            : _kind(kind), _keys(std::move(keys)), _values(std::move(values)) {
    }

    virtual void freeze() override {
        if (_keys && _keys->supports_freezing()) {
            _keys->freeze();
        }
        if (_values && _values->supports_freezing()) {
            _values->freeze();
        }
        _frozen = true;
    }

    virtual bool supports_freezing() const override {
        return true;
    }

    virtual bool is_collection() const override {
        return true;
    }

    virtual shared_ptr<cql3_type> prepare(database& db, const sstring& keyspace) override {
        assert(_values); // "Got null values type for a collection";

        if (!_frozen && _values->supports_freezing() && !_values->_frozen) {
            throw exceptions::invalid_request_exception(sprint("Non-frozen collections are not allowed inside collections: %s", *this));
        }
        if (_values->is_counter()) {
            throw exceptions::invalid_request_exception(sprint("Counters are not allowed inside collections: %s", *this));
        }

        if (_keys) {
            if (!_frozen && _keys->supports_freezing() && !_keys->_frozen) {
                throw exceptions::invalid_request_exception(sprint("Non-frozen collections are not allowed inside collections: %s", *this));
            }
        }

        if (_kind == &collection_type_impl::kind::list) {
            return make_shared(cql3_type(to_string(), list_type_impl::get_instance(_values->prepare(db, keyspace)->get_type(), !_frozen), false));
        } else if (_kind == &collection_type_impl::kind::set) {
            return make_shared(cql3_type(to_string(), set_type_impl::get_instance(_values->prepare(db, keyspace)->get_type(), !_frozen), false));
        } else if (_kind == &collection_type_impl::kind::map) {
            assert(_keys); // "Got null keys type for a collection";
            return make_shared(cql3_type(to_string(), map_type_impl::get_instance(_keys->prepare(db, keyspace)->get_type(), _values->prepare(db, keyspace)->get_type(), !_frozen), false));
        }
        abort();
    }

    virtual sstring to_string() const override {
        sstring start = _frozen ? "frozen<" : "";
        sstring end = _frozen ? ">" : "";
        if (_kind == &collection_type_impl::kind::list) {
            return sprint("%slist<%s>%s", start, _values, end);
        } else if (_kind == &collection_type_impl::kind::set) {
            return sprint("%sset<%s>%s", start, _values, end);
        } else if (_kind == &collection_type_impl::kind::map) {
            return sprint("%smap<%s, %s>%s", start, _keys, _values, end);
        }
        abort();
    }
};

class cql3_type::raw_tuple : public raw {
    std::vector<shared_ptr<raw>> _types;
public:
    raw_tuple(std::vector<shared_ptr<raw>> types)
            : _types(std::move(types)) {
    }
    virtual bool supports_freezing() const override {
        return true;
    }
    virtual bool is_collection() const override {
        return false;
    }
    virtual void freeze() override {
        for (auto&& t : _types) {
            if (t->supports_freezing()) {
                t->freeze();
            }
        }
        _frozen = true;
    }
    virtual shared_ptr<cql3_type> prepare(database& db, const sstring& keyspace) override {
        if (!_frozen) {
            freeze();
        }
        std::vector<data_type> ts;
        for (auto&& t : _types) {
            if (t->is_counter()) {
                throw exceptions::invalid_request_exception("Counters are not allowed inside tuples");
            }
            ts.push_back(t->prepare(db, keyspace)->get_type());
        }
        return make_cql3_tuple_type(tuple_type_impl::get_instance(std::move(ts)));
    }
    virtual sstring to_string() const override {
        return sprint("tuple<%s>", join(", ", _types));
    }
};

bool
cql3_type::raw::is_collection() const {
    return false;
}

bool
cql3_type::raw::is_counter() const {
    return false;
}

std::experimental::optional<sstring>
cql3_type::raw::keyspace() const {
    return std::experimental::optional<sstring>{};
}

void
cql3_type::raw::freeze() {
    sstring message = sprint("frozen<> is only allowed on collections, tuples, and user-defined types (got %s)", to_string());
    throw exceptions::invalid_request_exception(message);
}

shared_ptr<cql3_type::raw>
cql3_type::raw::from(shared_ptr<cql3_type> type) {
    return ::make_shared<raw_type>(type);
}

shared_ptr<cql3_type::raw>
cql3_type::raw::map(shared_ptr<raw> t1, shared_ptr<raw> t2) {
    return make_shared(raw_collection(&collection_type_impl::kind::map, std::move(t1), std::move(t2)));
}

shared_ptr<cql3_type::raw>
cql3_type::raw::list(shared_ptr<raw> t) {
    return make_shared(raw_collection(&collection_type_impl::kind::list, {}, std::move(t)));
}

shared_ptr<cql3_type::raw>
cql3_type::raw::set(shared_ptr<raw> t) {
    return make_shared(raw_collection(&collection_type_impl::kind::set, {}, std::move(t)));
}

shared_ptr<cql3_type::raw>
cql3_type::raw::tuple(std::vector<shared_ptr<raw>> ts) {
    return make_shared(raw_tuple(std::move(ts)));
}

thread_local shared_ptr<cql3_type> cql3_type::ascii = make("ascii", ascii_type, cql3_type::kind::ASCII);
thread_local shared_ptr<cql3_type> cql3_type::bigint = make("bigint", long_type, cql3_type::kind::BIGINT);
thread_local shared_ptr<cql3_type> cql3_type::blob = make("blob", bytes_type, cql3_type::kind::BLOB);
thread_local shared_ptr<cql3_type> cql3_type::boolean = make("boolean", boolean_type, cql3_type::kind::BOOLEAN);
thread_local shared_ptr<cql3_type> cql3_type::double_ = make("double", double_type, cql3_type::kind::DOUBLE);
thread_local shared_ptr<cql3_type> cql3_type::float_ = make("float", float_type, cql3_type::kind::FLOAT);
thread_local shared_ptr<cql3_type> cql3_type::int_ = make("int", int32_type, cql3_type::kind::INT);
thread_local shared_ptr<cql3_type> cql3_type::text = make("text", utf8_type, cql3_type::kind::TEXT);
thread_local shared_ptr<cql3_type> cql3_type::timestamp = make("timestamp", timestamp_type, cql3_type::kind::TIMESTAMP);
thread_local shared_ptr<cql3_type> cql3_type::uuid = make("uuid", uuid_type, cql3_type::kind::UUID);
thread_local shared_ptr<cql3_type> cql3_type::varchar = make("varchar", utf8_type, cql3_type::kind::TEXT);
thread_local shared_ptr<cql3_type> cql3_type::timeuuid = make("timeuuid", timeuuid_type, cql3_type::kind::TIMEUUID);
thread_local shared_ptr<cql3_type> cql3_type::inet = make("inet", inet_addr_type, cql3_type::kind::INET);

const std::vector<shared_ptr<cql3_type>>&
cql3_type::values() {
    static thread_local std::vector<shared_ptr<cql3_type>> v = {
        cql3_type::ascii,
        cql3_type::bigint,
        cql3_type::blob,
        cql3_type::boolean,
#if 0
        cql3_type::counter,
        cql3_type::decimal,
#endif
        cql3_type::double_,
        cql3_type::float_,
        cql3_type:inet,
        cql3_type::int_,
        cql3_type::text,
        cql3_type::timestamp,
        cql3_type::uuid,
        cql3_type::varchar,
        cql3_type::timeuuid,
    };
    return v;
}

shared_ptr<cql3_type>
make_cql3_tuple_type(shared_ptr<tuple_type_impl> t) {
    auto name = sprint("tuple<%s>",
                       join(", ",
                            t->all_types()
                            | boost::adaptors::transformed(std::mem_fn(&abstract_type::as_cql3_type))));
    return ::make_shared<cql3_type>(std::move(name), std::move(t), false);
}


std::ostream&
operator<<(std::ostream& os, const cql3_type::raw& r) {
    return os << r.to_string();
}

}

