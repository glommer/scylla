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

#include "query_options.hh"
#include "version.hh"

namespace cql3 {

thread_local const query_options::specific_options query_options::specific_options::DEFAULT{-1, {}, {}, api::missing_timestamp};

thread_local query_options query_options::DEFAULT{db::consistency_level::ONE, std::experimental::nullopt,
    {}, false, query_options::specific_options::DEFAULT, version::native_protocol(), serialization_format::use_32_bit()};


query_options::query_options(std::vector<bytes_opt> values)
        : query_options(db::consistency_level::ONE, { }, std::move(values),
        false, query_options::specific_options::DEFAULT, version::native_protocol(),
                serialization_format::use_32_bit()) {
}

}

