/*
 * Copyright 2015 Cloudius Systems
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "core/iostream.hh"
#include "core/fstream.hh"
#include "types.hh"
#include "compress.hh"

namespace sstables {

class file_writer {
public:
    // This helper class allows one to track the progress of data being written through the
    // file_writer. It is common for data to have natural markers, such as a partition for the
    // data_file. A user may want to know how many of those markers were written, but only the
    // sink impls can truly know that: writing to the file_writer writes to buffered storage,
    // and only after put() there is any expectation that data will be in the disk.
    //
    // The user of the interface will let the file_writer know that we have reached a milestone
    // in the file by calling add_progress_marker(). The interface is not concerned with what
    // this marker really is (if bytes, partitions, etc).
    //
    // When the impl class' put() method is called and the write succeeds, the outstanding progress
    // is pushed to a stable state, that can later be queried by the current_progress method
    class progress_tracker {
        uint64_t non_pushed = 0;
        uint64_t pushed = 0;
        void push() {
            pushed += non_pushed;
            non_pushed = 0;
        }
        friend class file_writer;
        friend class checksummed_file_data_sink_impl;
        friend class compressed_file_data_sink_impl;
    };
private:
    output_stream<char> _out;
    size_t _offset = 0;
protected:
    progress_tracker _progress_tracker;
public:
    file_writer(file f, file_output_stream_options options)
        : _out(make_file_output_stream(std::move(f), std::move(options))) {}

    file_writer(output_stream<char>&& out)
        : _out(std::move(out)) {}

    virtual ~file_writer() = default;
    file_writer(file_writer&&) = default;

    virtual future<> write(const char* buf, size_t n) {
        _offset += n;
        return _out.write(buf, n);
    }
    virtual future<> write(const bytes& s) {
        _offset += s.size();
        return _out.write(s);
    }
    future<> flush() {
        return _out.flush();
    }
    future<> close() {
        return _out.close();
    }
    void add_progress_marker(uint64_t progress) {
        _progress_tracker.non_pushed += progress;
    };

    uint64_t current_progress() const {
        return _progress_tracker.pushed;
    };

    size_t offset() {
        return _offset;
    }
};

output_stream<char> make_checksummed_file_output_stream(file f, struct checksum& cinfo, uint32_t& full_file_checksum, bool checksum_file,
                                                        file_output_stream_options options, file_writer::progress_tracker& t);

class checksummed_file_writer : public file_writer {
    checksum _c;
    uint32_t _full_checksum;
public:
    checksummed_file_writer(file f, file_output_stream_options options, bool checksum_file = false)
            : file_writer(make_checksummed_file_output_stream(std::move(f), _c, _full_checksum, checksum_file, options, _progress_tracker))
            , _c({uint32_t(std::min(size_t(DEFAULT_CHUNK_SIZE), size_t(options.buffer_size)))})
            , _full_checksum(init_checksum_adler32()) {}

    // Since we are exposing a reference to _full_checksum, we delete the move
    // constructor.  If it is moved, the reference will refer to the old
    // location.
    checksummed_file_writer(checksummed_file_writer&&) = delete;
    checksummed_file_writer(const checksummed_file_writer&) = default;

    checksum& finalize_checksum() {
        return _c;
    }
    uint32_t full_checksum() {
        return _full_checksum;
    }
};

class checksummed_file_data_sink_impl : public data_sink_impl {
    output_stream<char> _out;
    struct checksum& _c;
    uint32_t& _full_checksum;
    bool _checksum_file;
    file_writer::progress_tracker& _progress_tracker;
public:
    checksummed_file_data_sink_impl(file f, struct checksum& c, uint32_t& full_file_checksum, bool checksum_file, file_output_stream_options options, file_writer::progress_tracker& t)
            : _out(make_file_output_stream(std::move(f), std::move(options)))
            , _c(c)
            , _full_checksum(full_file_checksum)
            , _checksum_file(checksum_file)
            , _progress_tracker(t)
            {}

    future<> put(net::packet data) { abort(); }
    virtual future<> put(temporary_buffer<char> buf) override {
        // bufs will usually be a multiple of chunk size, but this won't be the case for
        // the last buffer being flushed.
        if (!_checksum_file) {
            _full_checksum = checksum_adler32(_full_checksum, buf.begin(), buf.size());
        } else {
            for (size_t offset = 0; offset < buf.size(); offset += _c.chunk_size) {
                size_t size = std::min(size_t(_c.chunk_size), buf.size() - offset);
                uint32_t per_chunk_checksum = init_checksum_adler32();

                per_chunk_checksum = checksum_adler32(per_chunk_checksum, buf.begin() + offset, size);
                _full_checksum = checksum_adler32_combine(_full_checksum, per_chunk_checksum, size);
                _c.checksums.push_back(per_chunk_checksum);
            }
        }
        auto f = _out.write(buf.begin(), buf.size());
        return f.then([buf = std::move(buf), this] {
            _progress_tracker.push();
        });
    }

    virtual future<> close() {
        // Nothing to do, because close at the file_stream level will call flush on us.
        return _out.close();
    }
};

class checksummed_file_data_sink : public data_sink {
public:
    checksummed_file_data_sink(file f, struct checksum& cinfo, uint32_t& full_file_checksum, bool checksum_file, file_output_stream_options options, file_writer::progress_tracker& t)
        : data_sink(std::make_unique<checksummed_file_data_sink_impl>(std::move(f), cinfo, full_file_checksum, checksum_file, std::move(options), t)) {}
};

inline
output_stream<char> make_checksummed_file_output_stream(file f, struct checksum& cinfo, uint32_t& full_file_checksum,
                                                        bool checksum_file, file_output_stream_options options, file_writer::progress_tracker& t) {
    auto buffer_size = options.buffer_size;
    return output_stream<char>(checksummed_file_data_sink(std::move(f), cinfo, full_file_checksum, checksum_file, std::move(options), t), buffer_size, true);
}

// compressed_file_data_sink_impl works as a filter for a file output stream,
// where the buffer flushed will be compressed and its checksum computed, then
// the result passed to a regular output stream.
class compressed_file_data_sink_impl : public data_sink_impl {
    output_stream<char> _out;
    sstables::compression* _compression_metadata;
    size_t _pos = 0;
    file_writer::progress_tracker& _progress_tracker;
public:
    compressed_file_data_sink_impl(file f, sstables::compression* cm, file_output_stream_options options, file_writer::progress_tracker& t)
            : _out(make_file_output_stream(std::move(f), options))
            , _compression_metadata(cm)
            , _progress_tracker(t) {}

    future<> put(net::packet data) { abort(); }
    virtual future<> put(temporary_buffer<char> buf) override {
        auto output_len = _compression_metadata->compress_max_size(buf.size());
        // account space for checksum that goes after compressed data.
        temporary_buffer<char> compressed(output_len + 4);

        // compress flushed data.
        auto len = _compression_metadata->compress(buf.get(), buf.size(), compressed.get_write(), output_len);
        if (len > output_len) {
            throw std::runtime_error("possible overflow during compression");
        }

        _compression_metadata->offsets.elements.push_back(_pos);
        // account compressed data + 32-bit checksum.
        _pos += len + 4;
        _compression_metadata->set_compressed_file_length(_pos);
        // total length of the uncompressed data.
        _compression_metadata->data_len += buf.size();

        // compute 32-bit checksum for compressed data.
        uint32_t per_chunk_checksum = checksum_adler32(compressed.get(), len);
        _compression_metadata->update_full_checksum(per_chunk_checksum, len);

        // write checksum into buffer after compressed data.
        *unaligned_cast<uint32_t*>(compressed.get_write() + len) = htonl(per_chunk_checksum);

        compressed.trim(len + 4);

        auto f = _out.write(compressed.get(), compressed.size());
        return f.then([compressed = std::move(compressed), this] {
            _progress_tracker.push();
        });
    }
    virtual future<> close() {
        return _out.close();
    }
};

class compressed_file_data_sink : public data_sink {
public:
    compressed_file_data_sink(file f, sstables::compression* cm, file_output_stream_options options, file_writer::progress_tracker& t)
        : data_sink(std::make_unique<compressed_file_data_sink_impl>(
                std::move(f), cm, options, t)) {}
};

static inline output_stream<char> make_compressed_file_output_stream(file f, file_output_stream_options options, file_writer::progress_tracker& t, sstables::compression* cm) {
    // buffer of output stream is set to chunk length, because flush must
    // happen every time a chunk was filled up.
    options.buffer_size = cm->uncompressed_chunk_length();
    return output_stream<char>(compressed_file_data_sink(std::move(f), cm, options, t), options.buffer_size, true);
}

class compressed_file_writer : public file_writer {
public:
    compressed_file_writer(file f, file_output_stream_options options, sstables::compression *cm)
            : file_writer(make_compressed_file_output_stream(std::move(f), options, _progress_tracker, cm))
            {}
    compressed_file_writer(compressed_file_writer&&) = delete;
    compressed_file_writer(const compressed_file_writer&) = default;
};
}
