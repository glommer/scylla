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

#include "cql3/statements/schema_altering_statement.hh"

#include "transport/messages/result_message.hh"

namespace cql3 {

namespace statements {

future<::shared_ptr<messages::result_message>>
schema_altering_statement::execute(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    // If an IF [NOT] EXISTS clause was used, this may not result in an actual schema change.  To avoid doing
    // extra work in the drivers to handle schema changes, we return an empty message in this case. (CASSANDRA-7600)
    return announce_migration(proxy, false).then([this] (bool did_change_schema) {
        if (!did_change_schema) {
            auto result = ::make_shared<messages::result_message::void_message>();
            return make_ready_future<::shared_ptr<messages::result_message>>(result);
        }
        auto ce = this->change_event();
        ::shared_ptr<messages::result_message> result;
        if (!ce) {
            result = ::make_shared<messages::result_message::void_message>();
        } else {
            result = ::make_shared<messages::result_message::schema_change>(ce);
        }
        return make_ready_future<::shared_ptr<messages::result_message>>(result);
    });
}

future<::shared_ptr<messages::result_message>>
schema_altering_statement::execute_internal(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    throw std::runtime_error("unsupported operation");
#if 0
    try
    {
        boolean didChangeSchema = announceMigration(true);
        if (!didChangeSchema)
            return new ResultMessage.Void();

        Event.SchemaChange ce = changeEvent();
        return ce == null ? new ResultMessage.Void() : new ResultMessage.SchemaChange(ce);
    }
    catch (RequestValidationException e)
    {
        throw new RuntimeException(e);
    }
#endif
}

}

}
