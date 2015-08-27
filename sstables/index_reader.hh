/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once
#include "sstables.hh"
#include "consumer.hh"

namespace sstables {

class index_consumer {
    uint64_t max_quantity;
public:
    index_list indexes;

    index_consumer(uint64_t q) : max_quantity(q) {
        indexes.reserve(q);
    }

    bool should_continue() {
        return indexes.size() < max_quantity;
    }
    void consume_entry(index_entry&& ie) {
        indexes.push_back(std::move(ie));
    }
};

class index_consume_entry_context: public continuous_data_consumer {
private:
    index_consumer& _consumer;

    enum class state {
        START,
        KEY_SIZE,
        KEY_BYTES,
        POSITION,
        PROMOTED_SIZE,
        PROMOTED_BYTES,
        CONSUME_ENTRY,
    } _state = state::START;

    temporary_buffer<char> _key;
    temporary_buffer<char> _promoted;

    static inline bytes to_bytes(temporary_buffer<char>& b) {
        using byte = bytes_view::value_type;
        auto s = bytes(reinterpret_cast<const byte*>(b.get()), b.size());
        b.release();
        return s;
    }

    bool non_consuming() const {
        return ((_state == state::CONSUME_ENTRY) || (_state == state::START) ||
                ((_state == state::PROMOTED_BYTES) && (_prestate == prestate::NONE)));
    }

    proceed do_process(temporary_buffer<char>& data) {
        while (data || non_consuming()) {
            process_buffer(data);

            switch (_state) {
            // START comes first, to make the handling of the 0-quantity case simpler
            case state::START:
                if (!_consumer.should_continue()) {
                    return continuous_data_consumer::proceed::no;
                }
                _state = state::KEY_SIZE;
                break;
            case state::KEY_SIZE:
                if (read_16(data) != read_status::ready) {
                    _state = state::KEY_BYTES;
                    break;
                }
            case state::KEY_BYTES:
                if (read_bytes(data, _u16, _key) != read_status::ready) {
                    _state = state::POSITION;
                    break;
                }
            case state::POSITION:
                if (read_64(data) != read_status::ready) {
                    _state = state::PROMOTED_SIZE;
                    break;
                }
            case state::PROMOTED_SIZE:
                if (read_32(data) != read_status::ready) {
                    _state = state::PROMOTED_BYTES;
                    break;
                }
            case state::PROMOTED_BYTES:
                if (read_bytes(data, _u32, _promoted) != read_status::ready) {
                    _state = state::CONSUME_ENTRY;
                    break;
                }
            case state::CONSUME_ENTRY: {
                index_entry ie;
                ie.key.value = to_bytes(_key);
                ie.position = _u64;
                ie.promoted_index.value = to_bytes(_promoted);
                _consumer.consume_entry(std::move(ie));
                _state = state::START;
                break;
            }
            default:
                throw malformed_sstable_exception("unknown state");
            }
        }
        return continuous_data_consumer::proceed::yes;
    }

public:
    index_consume_entry_context(index_consumer& consumer,
            input_stream<char>&& input, uint64_t maxlen)
        : continuous_data_consumer(std::move(input), maxlen, [this] (temporary_buffer<char>& b) { return do_process(b); })
        , _consumer(consumer)
    {}

};
}
