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
 */

/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "operation.hh"
#include "operation_impl.hh"
#include "maps.hh"
#include "sets.hh"

namespace cql3 {


shared_ptr<operation>
operation::set_element::prepare(const sstring& keyspace, column_definition& receiver) {
    using exceptions::invalid_request_exception;
    auto rtype = dynamic_pointer_cast<collection_type_impl>(receiver.type);
    if (!rtype) {
        throw invalid_request_exception(sprint("Invalid operation (%s) for non collection column %s", receiver, receiver.name()));
    } else if (!rtype->is_multi_cell()) {
        throw invalid_request_exception(sprint("Invalid operation (%s) for frozen collection column %s", receiver, receiver.name()));
    }

    if (&rtype->_kind == &collection_type_impl::kind::list) {
        abort();
        // FIXME:
#if 0
            Term idx = selector.prepare(keyspace, Lists.indexSpecOf(receiver));
            Term lval = value.prepare(keyspace, Lists.valueSpecOf(receiver));
            return new Lists.SetterByIndex(receiver, idx, lval);
#endif
    } else if (&rtype->_kind == &collection_type_impl::kind::set) {
        throw invalid_request_exception(sprint("Invalid operation (%s) for set column %s", receiver, receiver.name()));
    } else if (&rtype->_kind == &collection_type_impl::kind::map) {
        auto key = _selector->prepare(keyspace, maps::key_spec_of(*receiver.column_specification));
        auto mval = _value->prepare(keyspace, maps::value_spec_of(*receiver.column_specification));
        return make_shared<maps::setter_by_key>(receiver, key, mval);
    }
    abort();
}

bool
operation::set_element::is_compatible_with(shared_ptr<raw_update> other) {
    // TODO: we could check that the other operation is not setting the same element
    // too (but since the index/key set may be a bind variables we can't always do it at this point)
    return !dynamic_pointer_cast<set_value>(std::move(other));
}

shared_ptr<operation>
operation::addition::prepare(const sstring& keyspace, column_definition& receiver) {
    auto v = _value->prepare(keyspace, receiver.column_specification);

    auto ctype = dynamic_pointer_cast<collection_type_impl>(receiver.type);
    if (!ctype) {
        warn(unimplemented::cause::COUNTERS);
        throw exceptions::invalid_request_exception("not implemented");
        // FIXME: implelement
#if 0
        if (!(receiver.type instanceof CounterColumnType))
            throw new InvalidRequestException(String.format("Invalid operation (%s) for non counter column %s", toString(receiver), receiver.name));
        return new Constants.Adder(receiver, v);
#endif
    } else if (!ctype->is_multi_cell()) {
        throw exceptions::invalid_request_exception(sprint("Invalid operation (%s) for frozen collection column %s", receiver, receiver.name()));
    }

    if (&ctype->_kind == &collection_type_impl::kind::list) {
        warn(unimplemented::cause::COLLECTIONS);
        // FIXME:
        // return new Lists.Appender(receiver, v);
        throw exceptions::invalid_request_exception("unimplemented, bye");
    } else if (&ctype->_kind == &collection_type_impl::kind::set) {
        return make_shared<sets::adder>(receiver, v);
    } else if (&ctype->_kind == &collection_type_impl::kind::map) {
        return make_shared<maps::putter>(receiver, v);
    } else {
        abort();
    }
}

bool
operation::addition::is_compatible_with(shared_ptr<raw_update> other) {
    return !dynamic_pointer_cast<set_value>(other);
}

shared_ptr<operation>
operation::subtraction::prepare(const sstring& keyspace, column_definition& receiver) {
    warn(unimplemented::cause::COLLECTIONS);
    throw exceptions::invalid_request_exception("unimplemented, go away");
    // FIXME:
#if 0
    if (!(receiver.type instanceof CollectionType))
    {
        if (!(receiver.type instanceof CounterColumnType))
            throw new InvalidRequestException(String.format("Invalid operation (%s) for non counter column %s", toString(receiver), receiver.name));
        return new Constants.Substracter(receiver, value.prepare(keyspace, receiver));
    }
    else if (!(receiver.type.isMultiCell()))
        throw new InvalidRequestException(String.format("Invalid operation (%s) for frozen collection column %s", toString(receiver), receiver.name));

    switch (((CollectionType)receiver.type).kind)
    {
        case LIST:
            return new Lists.Discarder(receiver, value.prepare(keyspace, receiver));
        case SET:
            return new Sets.Discarder(receiver, value.prepare(keyspace, receiver));
        case MAP:
            // The value for a map subtraction is actually a set
            ColumnSpecification vr = new ColumnSpecification(receiver.ksName,
                                                             receiver.cfName,
                                                             receiver.name,
                                                             SetType.getInstance(((MapType)receiver.type).getKeysType(), false));
            return new Sets.Discarder(receiver, value.prepare(keyspace, vr));
    }
    throw new AssertionError();
#endif
}

bool
operation::subtraction::is_compatible_with(shared_ptr<raw_update> other) {
    return !dynamic_pointer_cast<set_value>(other);
}

shared_ptr<operation>
operation::prepend::prepare(const sstring& keyspace, column_definition& receiver) {
    warn(unimplemented::cause::COLLECTIONS);
    throw exceptions::invalid_request_exception("unimplemented, go away");
    // FIXME:
#if 0
    auto v = _value->prepare(keyspace, receiver.column_specification);

    if (!(receiver.type instanceof ListType))
        throw new InvalidRequestException(String.format("Invalid operation (%s) for non list column %s", toString(receiver), receiver.name));
    else if (!(receiver.type.isMultiCell()))
        throw new InvalidRequestException(String.format("Invalid operation (%s) for frozen list column %s", toString(receiver), receiver.name));

    return new Lists.Prepender(receiver, v);
#endif
}

bool
operation::prepend::is_compatible_with(shared_ptr<raw_update> other) {
    return !dynamic_pointer_cast<set_value>(other);
}

}
