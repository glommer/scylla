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

#ifndef CQL_STATEMENTS_UPDATE_STATEMENT_HH
#define CQL_STATEMENTS_UPDATE_STATEMENT_HH

#include "cql3/statements/modification_statement.hh"
#include "cql3/column_identifier.hh"
#include "cql3/term.hh"

#include "database.hh"

#include <vector>
#include "unimplemented.hh"

#if 0
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
#endif

namespace cql3 {

namespace statements {

/**
 * An <code>UPDATE</code> statement parsed from a CQL query statement.
 *
 */
class update_statement : public modification_statement {
public:
#if 0
    private static final Constants.Value EMPTY = new Constants.Value(ByteBufferUtil.EMPTY_BYTE_BUFFER);
#endif

    update_statement(statement_type type, uint32_t bound_terms, schema_ptr s, std::unique_ptr<attributes> attrs)
        : modification_statement{type, bound_terms, std::move(s), std::move(attrs)}
    { }

private:
    virtual bool require_full_clustering_key() const override {
        return true;
    }

    virtual void add_update_for_key(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) override;
public:
    class parsed_insert : public modification_statement::parsed {
    private:
        const std::vector<::shared_ptr<column_identifier::raw>> _column_names;
        const std::vector<::shared_ptr<term::raw>> _column_values;
    public:
        /**
         * A parsed <code>INSERT</code> statement.
         *
         * @param name column family being operated on
         * @param columnNames list of column names
         * @param columnValues list of column values (corresponds to names)
         * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
         */
        parsed_insert(::shared_ptr<cf_name> name,
                      ::shared_ptr<attributes::raw> attrs,
                      std::vector<::shared_ptr<column_identifier::raw>> column_names,
                      std::vector<::shared_ptr<term::raw>> column_values,
                      bool if_not_exists)
            : modification_statement::parsed{std::move(name), std::move(attrs), conditions_vector{}, if_not_exists, false}
            , _column_names{std::move(column_names)}
            , _column_values{std::move(column_values)}
        { }

        virtual ::shared_ptr<modification_statement> prepare_internal(database& db, schema_ptr schema,
                    ::shared_ptr<variable_specifications> bound_names, std::unique_ptr<attributes> attrs) override;

    };

    class parsed_update : public modification_statement::parsed {
    private:
        // Provided for an UPDATE
        std::vector<std::pair<::shared_ptr<column_identifier::raw>, ::shared_ptr<operation::raw_update>>> _updates;
        std::vector<relation_ptr> _where_clause;
    public:
        /**
         * Creates a new UpdateStatement from a column family name, columns map, consistency
         * level, and key term.
         *
         * @param name column family being operated on
         * @param attrs additional attributes for statement (timestamp, timeToLive)
         * @param updates a map of column operations to perform
         * @param whereClause the where clause
         */
        parsed_update(::shared_ptr<cf_name> name,
            ::shared_ptr<attributes::raw> attrs,
            std::vector<std::pair<::shared_ptr<column_identifier::raw>, ::shared_ptr<operation::raw_update>>> updates,
            std::vector<relation_ptr> where_clause,
            conditions_vector conditions)
                : modification_statement::parsed(std::move(name), std::move(attrs), std::move(conditions), false, false)
                , _updates(std::move(updates))
                , _where_clause(std::move(where_clause))
        { }
    protected:
        virtual ::shared_ptr<modification_statement> prepare_internal(database& db, schema_ptr schema,
                    ::shared_ptr<variable_specifications> bound_names, std::unique_ptr<attributes> attrs);
    };
};

}

}

#endif
