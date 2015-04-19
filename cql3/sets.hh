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

#ifndef CQL3_SETS_HH
#define CQL3_SETS_HH

#include "cql3/abstract_marker.hh"
#include "maps.hh"
#include "column_specification.hh"
#include "column_identifier.hh"
#include "to_string.hh"
#include <unordered_set>

namespace cql3 {

#if 0
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Joiner;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
#endif

/**
 * Static helper methods and classes for sets.
 */
class sets {
    sets() = delete;
public:
    static shared_ptr<column_specification> value_spec_of(shared_ptr<column_specification> column);

    class literal : public term::raw {
        std::vector<shared_ptr<term::raw>> _elements;
    public:
        explicit literal(std::vector<shared_ptr<term::raw>> elements)
                : _elements(std::move(elements)) {
        }
        shared_ptr<term> prepare(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver);
        void validate_assignable_to(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver);
        assignment_testable::test_result
        test_assignment(database& db, const sstring& keyspace, shared_ptr<column_specification> receiver);
        virtual sstring to_string() const override;
    };

    class value : public terminal, collection_terminal {
    public:
        std::set<bytes, serialized_compare> _elements;
    public:
        value(std::set<bytes, serialized_compare> elements)
                : _elements(std::move(elements)) {
        }
        static value from_serialized(bytes_view v, set_type type, serialization_format sf);
        virtual bytes_opt get(const query_options& options) override;
        virtual bytes get_with_protocol_version(serialization_format sf) override;
        bool equals(set_type st, const value& v);
        virtual sstring to_string() const override;
    };

    // See Lists.DelayedValue
    class delayed_value : public non_terminal {
        serialized_compare _comparator;
        std::vector<shared_ptr<term>> _elements;
    public:
        delayed_value(serialized_compare comparator, std::vector<shared_ptr<term>> elements)
            : _comparator(std::move(comparator)), _elements(std::move(elements)) {
        }
        virtual bool contains_bind_marker() const override;
        virtual void collect_marker_specification(shared_ptr<variable_specifications> bound_names) override;
        virtual shared_ptr<terminal> bind(const query_options& options);
    };

    class marker : public abstract_marker {
    public:
        marker(int32_t bind_index, ::shared_ptr<column_specification> receiver)
            : abstract_marker{bind_index, std::move(receiver)}
        { }
#if 0
        protected Marker(int bindIndex, ColumnSpecification receiver)
        {
            super(bindIndex, receiver);
            assert receiver.type instanceof SetType;
        }
#endif

        virtual ::shared_ptr<terminal> bind(const query_options& options) override;
#if 0
        public Value bind(QueryOptions options) throws InvalidRequestException
        {
            ByteBuffer value = options.getValues().get(bindIndex);
            return value == null ? null : Value.fromSerialized(value, (SetType)receiver.type, options.getProtocolVersion());
        }
#endif
    };

    class setter : public operation {
    public:
        setter(const column_definition& column, shared_ptr<term> t)
                : operation(column, std::move(t)) {
        }
        virtual void execute(mutation& m, const exploded_clustering_prefix& row_key, const update_parameters& params) override;
    };

    class adder : public operation {
    public:
        adder(const column_definition& column, shared_ptr<term> t)
            : operation(column, std::move(t)) {
        }
        virtual void execute(mutation& m, const exploded_clustering_prefix& row_key, const update_parameters& params) override;
        static void do_add(mutation& m, const exploded_clustering_prefix& row_key, const update_parameters& params,
                shared_ptr<term> t, const column_definition& column, tombstone ts = {});
    };

    // Note that this is reused for Map subtraction too (we subtract a set from a map)
    class discarder : public operation {
    public:
        discarder(const column_definition& column, shared_ptr<term> t)
            : operation(column, std::move(t)) {
        }
        virtual void execute(mutation& m, const exploded_clustering_prefix& row_key, const update_parameters& params) override;
    };
};

}

#endif
