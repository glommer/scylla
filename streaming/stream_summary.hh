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
 *
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
 */

#pragma once

#include "utils/UUID.hh"
#include <ostream>

namespace streaming {

/**
 * Summary of streaming.
 */
class stream_summary {
public:
    using UUID = utils::UUID;
    UUID cf_id;

    /**
     * Number of files to transfer. Can be 0 if nothing to transfer for some streaming request.
     */
    int files;
    long total_size;

    stream_summary() = default;
    stream_summary(UUID _cf_id, int _files, long _total_size)
        : cf_id (_cf_id)
        , files(_files)
        , total_size(_total_size) {
    }
    friend std::ostream& operator<<(std::ostream& os, const stream_summary& r);
#if 0
    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamSummary summary = (StreamSummary) o;
        return files == summary.files && totalSize == summary.totalSize && cfId.equals(summary.cfId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(cfId, files, totalSize);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("StreamSummary{");
        sb.append("path=").append(cfId);
        sb.append(", files=").append(files);
        sb.append(", totalSize=").append(totalSize);
        sb.append('}');
        return sb.toString();
    }

#endif
public:
    void serialize(bytes::iterator& out) const;
    static stream_summary deserialize(bytes_view& v);
    size_t serialized_size() const;
};

} // namespace streaming
