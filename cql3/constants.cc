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
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#include "cql3/constants.hh"
#include "cql3/cql3_type.hh"

namespace cql3 {

const ::shared_ptr<term::raw> constants::NULL_LITERAL = ::make_shared<constants::null_literal>();
const ::shared_ptr<terminal> constants::null_literal::NULL_VALUE = ::make_shared<constants::null_literal::null_value>();

std::ostream&
operator<<(std::ostream&out, constants::type t)
{
    switch (t) {
        case constants::type::STRING:  return out << "STRING";
        case constants::type::INTEGER: return out << "INTEGER";
        case constants::type::UUID:    return out << "UUID";
        case constants::type::FLOAT:   return out << "FLOAT";
        case constants::type::BOOLEAN: return out << "BOOLEAN";
        case constants::type::HEX:     return out << "HEX";
    };
    assert(0);
}

bytes
constants::literal::parsed_value(::shared_ptr<abstract_type> validator)
{
    try {
        if (_type == type::HEX && validator == bytes_type) {
            auto v = static_cast<sstring_view>(_text);
            v.remove_prefix(2);
            return validator->from_string(v);
        }
        if (validator->is_counter()) {
            return long_type->from_string(_text);
        }
        return validator->from_string(_text);
    } catch (const exceptions::marshal_exception& e) {
        throw exceptions::invalid_request_exception(e.what());
    }
}

assignment_testable::test_result
constants::literal::test_assignment(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver)
{
    auto receiver_type = receiver->type->as_cql3_type();
    if (receiver_type->is_collection()) {
        return test_result::NOT_ASSIGNABLE;
    }
    if (!receiver_type->is_native()) {
        return test_result::WEAKLY_ASSIGNABLE;
    }
    auto kind = receiver_type.get()->get_kind();
    switch (_type) {
        case type::STRING:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::ASCII,
                    cql3_type::kind::TEXT,
                    cql3_type::kind::INET,
                    cql3_type::kind::VARCHAR,
                    cql3_type::kind::TIMESTAMP>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::INTEGER:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::BIGINT,
                    cql3_type::kind::COUNTER,
                    cql3_type::kind::DECIMAL,
                    cql3_type::kind::DOUBLE,
                    cql3_type::kind::FLOAT,
                    cql3_type::kind::INT,
                    cql3_type::kind::TIMESTAMP,
                    cql3_type::kind::VARINT>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::UUID:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::UUID,
                    cql3_type::kind::TIMEUUID>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::FLOAT:
            if (cql3_type::kind_enum_set::frozen<
                    cql3_type::kind::DECIMAL,
                    cql3_type::kind::DOUBLE,
                    cql3_type::kind::FLOAT>::contains(kind)) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::BOOLEAN:
            if (kind == cql3_type::kind_enum_set::prepare<cql3_type::kind::BOOLEAN>()) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
        case type::HEX:
            if (kind == cql3_type::kind_enum_set::prepare<cql3_type::kind::BLOB>()) {
                return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
            }
            break;
    }
    return assignment_testable::test_result::NOT_ASSIGNABLE;
}

::shared_ptr<term>
constants::literal::prepare(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver)
{
    if (!is_assignable(test_assignment(db, keyspace, receiver))) {
        throw exceptions::invalid_request_exception(sprint("Invalid %s constant (%s) for \"%s\" of type %s",
            _type, _text, *receiver->name, receiver->type->as_cql3_type()->to_string()));
    }
    return ::make_shared<value>(std::experimental::make_optional(parsed_value(receiver->type)));
}

void constants::deleter::execute(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) {
    if (column.type->is_multi_cell()) {
        collection_type_impl::mutation coll_m;
        coll_m.tomb = params.make_tombstone();
        auto ctype = static_pointer_cast<collection_type_impl>(column.type);
        m.set_cell(prefix, column, atomic_cell_or_collection::from_collection_mutation(ctype->serialize_mutation_form(coll_m)));
    } else {
        m.set_cell(prefix, column, params.make_dead_cell());
    }
}

}
