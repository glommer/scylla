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
#include "streaming/messages/stream_message.hh"
#include "streaming/messages/file_message_header.hh"
#include "streaming/compress/compression_info.hh"
#include "streaming/stream_detail.hh"
#include "sstables/sstables.hh"
#include <seastar/core/semaphore.hh>

namespace streaming {
namespace messages {

/**
 * OutgoingFileMessage is used to transfer the part(or whole) of a SSTable data file.
 */
class outgoing_file_message : public stream_message {
    using UUID = utils::UUID;
    using compression_info = compress::compression_info;
    using format_types = sstables::sstable::format_types;
public:

    file_message_header header;
    stream_detail detail;

    size_t mutations_nr{0};
    semaphore mutations_done{0};

    outgoing_file_message() = default;
    outgoing_file_message(int32_t sequence_number, stream_detail d, bool keep_ss_table_level)
        : stream_message(stream_message::Type::FILE) {
#if 0
        CompressionInfo compressionInfo = null;
        if (sstable.compression)
        {
            CompressionMetadata meta = sstable.getCompressionMetadata();
            compressionInfo = new CompressionInfo(meta.getChunksForSections(sections), meta.parameters);
        }
#endif
        // FIXME:
        sstring version; // sstable.descriptor.version.toString()
        format_types format = format_types::big; // sstable.descriptor.formatType
        int32_t level = 0; // keepSSTableLevel ? sstable.getSSTableLevel() : 0
        compression_info comp_info;
        header = file_message_header(d.cf_id, sequence_number, version, format, d.estimated_keys,
                                     {}, comp_info, d.repaired_at, level);
        detail = std::move(d);
    }

#if 0
    @Override
    public String toString()
    {
        return "File (" + header + ", file: " + sstable.getFilename() + ")";
    }
#endif
};

} // namespace messages
} // namespace streaming

