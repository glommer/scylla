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
 * Copyright 2014 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#pragma once

namespace transport {

namespace messages {

class result_message;

}

}

#include "transport/event.hh"

#include "cql3/statements/cf_statement.hh"
#include "cql3/cql_statement.hh"

#include "core/shared_ptr.hh"

#include <experimental/optional>

namespace cql3 {

namespace statements {

namespace messages = transport::messages;

/**
 * Abstract class for statements that alter the schema.
 */
class schema_altering_statement : public cf_statement, public cql_statement, public ::enable_shared_from_this<schema_altering_statement> {
private:
    const bool _is_column_family_level;

protected:
    schema_altering_statement()
        : cf_statement{::shared_ptr<cf_name>{}}
        , _is_column_family_level{false}
    { }

    schema_altering_statement(::shared_ptr<cf_name> name)
        : cf_statement{std::move(name)}
        , _is_column_family_level{true}
    { }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return cf_statement::uses_function(ks_name, function_name);
    }

    virtual uint32_t get_bound_terms() override {
        return 0;
    }

    virtual void prepare_keyspace(service::client_state& state) override {
        if (_is_column_family_level) {
            cf_statement::prepare_keyspace(state);
        }
    }

    virtual ::shared_ptr<prepared> prepare(database& db) override {
        return ::make_shared<parsed_statement::prepared>(this->shared_from_this());
    }

    virtual shared_ptr<transport::event::schema_change> change_event() = 0;

    /**
     * Announces the migration to other nodes in the cluster.
     * @return true if the execution of this statement resulted in a schema change, false otherwise (when IF NOT EXISTS
     * is used, for example)
     * @throws RequestValidationException
     */
    virtual future<bool> announce_migration(distributed<service::storage_proxy>& proxy, bool is_local_only) = 0;

    virtual future<::shared_ptr<messages::result_message>>
    execute(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) override;

    virtual future<::shared_ptr<messages::result_message>>
    execute_internal(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) override;
};

}

}
