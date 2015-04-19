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

#ifndef CQL3_ABSTRACT_MARKER_HH
#define CQL3_ABSTRACT_MARKER_HH

#include "cql3/variable_specifications.hh"
#include "cql3/column_specification.hh"
#include "cql3/term.hh"

#include "db/marshal/collection_type.hh"
#include "db/marshal/list_type.hh"

namespace cql3 {

/**
 * A single bind marker.
 */
class abstract_marker : public non_terminal {
protected:
    const int32_t _bind_index;
    const ::shared_ptr<column_specification> _receiver;

public:
    abstract_marker(int32_t bind_index, ::shared_ptr<column_specification>&& receiver)
        : _bind_index{bind_index}
        , _receiver{std::move(receiver)}
    { }

    virtual void collect_marker_specification(::shared_ptr<variable_specifications> bound_names) override {
        bound_names->add(_bind_index, _receiver);
    }

    virtual bool contains_bind_marker() const override {
        return true;
    }

    /**
     * A parsed, but non prepared, bind marker.
     */
    class raw : public term::raw {
    protected:
        const int32_t _bind_index;

    public:
        raw(int32_t bind_index)
            : _bind_index{bind_index}
        { }

        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver) override;

        virtual assignment_testable::test_result test_assignment(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver) override {
            return assignment_testable::test_result::WEAKLY_ASSIGNABLE;
        }

        virtual sstring to_string() const override {
            return "?";
        }
    };

    /**
     * A raw placeholder for multiple values of the same type for a single column.
     * For example, "SELECT ... WHERE user_id IN ?'.
     *
     * Because a single type is used, a List is used to represent the values.
     */
    class in_raw : public raw {
    public:
        in_raw(int32_t bind_index)
            : raw{bind_index}
        { }

    private:
        static ::shared_ptr<column_specification> make_in_receiver(::shared_ptr<column_specification> receiver) {
            auto in_name = ::make_shared<column_identifier>(sstring("in(") + receiver->name->to_string() + sstring(")"), true);
            return ::make_shared<column_specification>(receiver->ks_name, receiver->cf_name, in_name, db::marshal::list_type::get_instance(receiver->type, false));
        }

    public:
        virtual ::shared_ptr<term> prepare(database& db, const sstring& keyspace, ::shared_ptr<column_specification> receiver) override;
    };
};

}

#endif
