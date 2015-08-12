/*
* Copyright (C) 2015 Cloudius Systems, Ltd.
*
*/

#pragma once
#include <net/byteorder.hh>
#include "exceptions/exceptions.hh"
#include "core/future.hh"
#include "core/iostream.hh"

template<typename T>
static inline T consume_be(temporary_buffer<char>& p) {
    T i = net::ntoh(*unaligned_cast<const T*>(p.get()));
    p.trim_front(sizeof(T));
    return i;
}

class continuous_data_consumer {
public:
    enum class proceed { yes, no };
protected:
    input_stream<char> _input;
    // remaining length of input to read (if <0, continue until end of file).
    int64_t _remain;

    // some states do not consume input (its only exists to perform some
    // action when finishing to read a primitive type via a prestate, in
    // the rare case that a primitive type crossed a buffer). Such
    // non-consuming states need to run even if the data buffer is empty.
    virtual bool non_consuming() const {
        return false;
    }

    // state for non-NONE prestates
    uint32_t _pos;
    // state for READING_U16, READING_U32, READING_U64 prestate
    uint16_t _u16;
    uint32_t _u32;
    uint64_t _u64;

    union {
        char bytes[sizeof(uint64_t)];
        uint64_t uint64;
        uint32_t uint32;
        uint16_t uint16;
    } _read_int;
    // state for READING_BYTES prestate
    temporary_buffer<char> _read_bytes;
    temporary_buffer<char>* _read_bytes_where; // which temporary_buffer to set, _key or _val?

protected:
    virtual void verify_end_state() {
    }

    // state machine progress:
    enum class prestate {
        NONE,
        READING_U16,
        READING_U32,
        READING_U64,
        READING_BYTES,
    } _prestate = prestate::NONE;


    uint16_t u16() { return _u16; }
    uint32_t u32() { return _u32; }
    uint64_t u64() { return _u64; }

    enum class read_status { ready, waiting };
    // Read a 16-bit integer into _u16. If the whole thing is in the buffer
    // (this is the common case), do this immediately. Otherwise, remember
    // what we have in the buffer, and remember to continue later by using
    // a "prestate":
    inline read_status read_16(temporary_buffer<char>& data) {
        if (data.size() >= sizeof(uint16_t)) {
            _u16 = consume_be<uint16_t>(data);
            return read_status::ready;
        } else {
            std::copy(data.begin(), data.end(), _read_int.bytes);
            _pos = data.size();
            data.trim(0);
            _prestate = prestate::READING_U16;
            return read_status::waiting;
        }
    }
    inline read_status read_32(temporary_buffer<char>& data) {
        if (data.size() >= sizeof(uint32_t)) {
            _u32 = consume_be<uint32_t>(data);
            return read_status::ready;
        } else {
            std::copy(data.begin(), data.end(), _read_int.bytes);
            _pos = data.size();
            data.trim(0);
            _prestate = prestate::READING_U32;
            return read_status::waiting;
        }
    }
    inline read_status read_64(temporary_buffer<char>& data) {
        if (data.size() >= sizeof(uint64_t)) {
            _u64 = consume_be<uint64_t>(data);
            return read_status::ready;
        } else {
            std::copy(data.begin(), data.end(), _read_int.bytes);
            _pos = data.size();
            data.trim(0);
            _prestate = prestate::READING_U64;
            return read_status::waiting;
        }
    }
    inline read_status read_bytes(temporary_buffer<char>& data, uint32_t len, temporary_buffer<char>& where) {
        if (data.size() >=  len) {
            where = data.share(0, len);
            data.trim_front(len);
            return read_status::ready;
        } else {
            // copy what we have so far, read the rest later
            _read_bytes = temporary_buffer<char>(len);
            std::copy(data.begin(), data.end(),_read_bytes.get_write());
            _read_bytes_where = &where;
            _pos = data.size();
            data.trim(0);
            _prestate = prestate::READING_BYTES;
            return read_status::waiting;
        }
    }

    virtual continuous_data_consumer::proceed process_state(temporary_buffer<char>& data) = 0;
public:
    template<typename Consumer>
    future<> consume_input(Consumer& c) {
        return _input.consume(c);
    }

    continuous_data_consumer(input_stream<char>&& input, uint64_t maxlen)
        : _input(std::move(input)), _remain(maxlen) {}

    using unconsumed_remainder = input_stream<char>::unconsumed_remainder;
    // called by input_stream::consume():
    future<unconsumed_remainder>
    operator()(temporary_buffer<char> data) {
        if (_remain >= 0 && data.size() >= (uint64_t)_remain) {
            // We received more data than we actually care about, so process
            // the beginning of the buffer, and return the rest to the stream
            auto segment = data.share(0, _remain);
            process(segment);
            data.trim_front(_remain - segment.size());
            _remain -= (_remain - segment.size());
            if (_remain == 0) {
                verify_end_state();
            }
            return make_ready_future<unconsumed_remainder>(std::move(data));
        } else if (data.empty()) {
            // End of file
            verify_end_state();
            return make_ready_future<unconsumed_remainder>(std::move(data));
        } else {
            // We can process the entire buffer (if the consumer wants to).
            auto orig_data_size = data.size();
            if (process(data) == continuous_data_consumer::proceed::yes) {
                assert(data.size() == 0);
                if (_remain >= 0) {
                    _remain -= orig_data_size;
                }
                return make_ready_future<unconsumed_remainder>();
            } else {
                if (_remain >= 0) {
                    _remain -= orig_data_size - data.size();
                }
                return make_ready_future<unconsumed_remainder>(std::move(data));
            }
        }
    }

    // process() feeds the given data into the state machine.
    // The consumer may request at any point (e.g., after reading a whole
    // row) to stop the processing, in which case we trim the buffer to
    // leave only the unprocessed part. The caller must handle calling
    // process() again, and/or refilling the buffer, as needed.
    continuous_data_consumer::proceed process(temporary_buffer<char>& data) {
#if 0
        // Testing hack: call process() for tiny chunks separately, to verify
        // that primitive types crossing input buffer are handled correctly.
        constexpr size_t tiny_chunk = 1; // try various tiny sizes
        if (data.size() > tiny_chunk) {
            for (unsigned i = 0; i < data.size(); i += tiny_chunk) {
                auto chunk_size = std::min(tiny_chunk, data.size() - i);
                auto chunk = data.share(i, chunk_size);
                if (process(chunk) == continuous_data_consumer::proceed::no) {
                    data.trim_front(i + chunk_size - chunk.size());
                    return continuous_data_consumer::proceed::no;
                }
            }
            data.trim(0);
            return continuous_data_consumer::proceed::yes;
        }
#endif
        while (data || non_consuming()) {
            if (_prestate != prestate::NONE) {
                // We're in the middle of reading a basic type, which crossed
                // an input buffer. Resume that read before continuing to
                // handle the current state:
                if (_prestate == prestate::READING_BYTES) {
                    auto n = std::min(_read_bytes.size() - _pos, data.size());
                    std::copy(data.begin(), data.begin() + n,
                            _read_bytes.get_write() + _pos);
                    data.trim_front(n);
                    _pos += n;
                    if (_pos == _read_bytes.size()) {
                        *_read_bytes_where = std::move(_read_bytes);
                        _prestate = prestate::NONE;
                    }
                } else {
                    // in the middle of reading an integer
                    unsigned len;
                    switch (_prestate) {
                    case prestate::READING_U16:
                        len = sizeof(uint16_t);
                        break;
                    case prestate::READING_U32:
                        len = sizeof(uint32_t);
                        break;
                    case prestate::READING_U64:
                        len = sizeof(uint64_t);
                        break;
                    default:
                        throw sstables::malformed_sstable_exception("unknown prestate");
                    }
                    assert(_pos < len);
                    auto n = std::min((size_t)(len - _pos), data.size());
                    std::copy(data.begin(), data.begin() + n, _read_int.bytes + _pos);
                    data.trim_front(n);
                    _pos += n;
                    if (_pos == len) {
                        // done reading the integer, store it in _u16, _u32 or _u64:
                        switch (_prestate) {
                        case prestate::READING_U16:
                            _u16 = net::ntoh(_read_int.uint16);
                            break;
                        case prestate::READING_U32:
                            _u32 = net::ntoh(_read_int.uint32);
                            break;
                        case prestate::READING_U64:
                            _u64 = net::ntoh(_read_int.uint64);
                            break;
                        default:
                            throw sstables::malformed_sstable_exception(
                                    "unknown prestate");
                        }
                        _prestate = prestate::NONE;
                    }
                }
                continue;
            }
            if (process_state(data) == continuous_data_consumer::proceed::no) {
                return continuous_data_consumer::proceed::no;
            }
        }
        return continuous_data_consumer::proceed::yes;
    }
};
