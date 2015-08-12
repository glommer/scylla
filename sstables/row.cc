/*
 * Copyright 2015 Cloudius Systems
 */

#include "sstables.hh"
#include "consumer.hh"

namespace sstables {

// data_consume_rows_context remembers the context that an ongoing
// data_consume_rows() future is in.
class data_consume_rows_context : public continuous_data_consumer {
private:
    row_consumer& _consumer;
    enum class state {
        ROW_START,
        ROW_KEY_BYTES,
        DELETION_TIME,
        DELETION_TIME_2,
        DELETION_TIME_3,
        ATOM_START,
        ATOM_START_2,
        ATOM_NAME_BYTES,
        ATOM_MASK,
        EXPIRING_CELL,
        EXPIRING_CELL_2,
        EXPIRING_CELL_3,
        CELL,
        CELL_2,
        CELL_VALUE_BYTES,
        CELL_VALUE_BYTES_2,
        RANGE_TOMBSTONE,
        RANGE_TOMBSTONE_2,
        RANGE_TOMBSTONE_3,
        RANGE_TOMBSTONE_4,
        RANGE_TOMBSTONE_5,
    } _state = state::ROW_START;

    virtual bool non_consuming() const override {
        return (((_state == state::DELETION_TIME_3)
                || (_state == state::CELL_VALUE_BYTES_2)
                || (_state == state::ATOM_START_2)
                || (_state == state::EXPIRING_CELL_3)) && (_prestate == prestate::NONE));
    }
    temporary_buffer<char> _key;
    temporary_buffer<char> _val;

    // state for reading a cell
    bool _deleted;
    uint32_t _ttl, _expiration;

    static inline bytes_view to_bytes_view(temporary_buffer<char>& b) {
        // The sstable code works with char, our "bytes_view" works with
        // byte_t. Rather than change all the code, let's do a cast...
        using byte = bytes_view::value_type;
        return bytes_view(reinterpret_cast<const byte*>(b.get()), b.size());
    }

public:
    data_consume_rows_context(row_consumer& consumer,
            input_stream<char> && input, uint64_t maxlen) :
            continuous_data_consumer(std::move(input), maxlen), _consumer(consumer) {
    }
    virtual void verify_end_state() override {
        if (_state != state::ROW_START || _prestate != prestate::NONE) {
            throw malformed_sstable_exception("end of input, but not end of row");
        }
    }

private:
    virtual continuous_data_consumer::proceed process_state(temporary_buffer<char>& data) override {
        switch (_state) {
        case state::ROW_START:
            // read 2-byte key length into _u16
            read_16(data);
            _state = state::ROW_KEY_BYTES;
            break;
        case state::ROW_KEY_BYTES:
            // After previously reading 16-bit length, read key's bytes.
            read_bytes(data, u16(), _key);
            _state = state::DELETION_TIME;
            break;
        case state::DELETION_TIME: {
            auto u32_status = read_32(data);
            _state = state::DELETION_TIME_2;
            if (u32_status == read_status::ready) {
                auto u64_status = read_64(data);
                _state = state::DELETION_TIME_3;
                if (u64_status == read_status::ready) {
                    // If we can read the entire deletion time at once, we can
                    // skip the DELETION_TIME_2 and DELETION_TIME_3 states.
                    deletion_time del;
                    del.local_deletion_time = u32();
                    del.marked_for_delete_at = u64();
                    _consumer.consume_row_start(to_bytes_view(_key), del);
                    // after calling the consume function, we can release the
                    // buffers we held for it.
                    _key.release();
                    _state = state::ATOM_START;
                }
            }
            break;
        }
        case state::DELETION_TIME_2:
            read_64(data);
            _state = state::DELETION_TIME_3;
            break;
        case state::DELETION_TIME_3: {
            deletion_time del;
            del.local_deletion_time = u32();
            del.marked_for_delete_at = u64();
            _consumer.consume_row_start(to_bytes_view(_key), del);
            // after calling the consume function, we can release the
            // buffers we held for it.
            _key.release();
            _state = state::ATOM_START;
            break;
        }
        case state::ATOM_START:
            if (read_16(data) == read_status::ready) {
                if (u16() == 0) {
                    // end of row marker
                    _state = state::ROW_START;
                    if (_consumer.consume_row_end() ==
                            continuous_data_consumer::proceed::no) {
                        return continuous_data_consumer::proceed::no;
                    }
                } else {
                    _state = state::ATOM_NAME_BYTES;
                }
            } else {
                _state = state::ATOM_START_2;
            }
            break;
        case state::ATOM_START_2:
            if (u16() == 0) {
                // end of row marker
                _state = state::ROW_START;
                if (_consumer.consume_row_end() ==
                        continuous_data_consumer::proceed::no) {
                    return continuous_data_consumer::proceed::no;
                }
            } else {
                _state = state::ATOM_NAME_BYTES;
            }
            break;
        case state::ATOM_NAME_BYTES:
            read_bytes(data, u16(), _key);
            _state = state::ATOM_MASK;
            break;
        case state::ATOM_MASK: {
            auto mask = consume_be<uint8_t>(data);
            enum mask_type {
                DELETION_MASK = 0x01,
                EXPIRATION_MASK = 0x02,
                COUNTER_MASK = 0x04,
                COUNTER_UPDATE_MASK = 0x08,
                RANGE_TOMBSTONE_MASK = 0x10,
            };
            if (mask & RANGE_TOMBSTONE_MASK) {
                _state = state::RANGE_TOMBSTONE;
            } else if (mask & COUNTER_MASK) {
                // FIXME: see ColumnSerializer.java:deserializeColumnBody
                throw malformed_sstable_exception("FIXME COUNTER_MASK");
            } else if (mask & EXPIRATION_MASK) {
                _deleted = false;
                _state = state::EXPIRING_CELL;
            } else {
                // FIXME: see ColumnSerializer.java:deserializeColumnBody
                if (mask & COUNTER_UPDATE_MASK) {
                    throw malformed_sstable_exception("FIXME COUNTER_UPDATE_MASK");
                }
                _ttl = _expiration = 0;
                _deleted = mask & DELETION_MASK;
                _state = state::CELL;
            }
            break;
        }
        case state::EXPIRING_CELL: {
            auto first_status = read_32(data);
            _state = state::EXPIRING_CELL_2;
            if (first_status == read_status::ready) {
                _ttl = u32();
                auto second_status = read_32(data);
                _state = state::EXPIRING_CELL_3;
                if (second_status == read_status::ready) {
                    _expiration = u32();
                    _state = state::CELL;
                }
            }
            break;
        }
        case state::EXPIRING_CELL_2:
            _ttl = u32();
            read_32(data);
            _state = state::EXPIRING_CELL_3;
            break;
        case state::EXPIRING_CELL_3:
            _expiration = u32();
            _state = state::CELL;
            break;
        case state::CELL: {
            auto status = read_64(data);
            _state = state::CELL_2;
            // Try to read both values in the same loop if possible
            if (status == read_status::ready) {
                read_32(data);
                _state = state::CELL_VALUE_BYTES;
            }
            break;
        }
        case state::CELL_2:
            read_32(data);
            _state = state::CELL_VALUE_BYTES;
            break;
        case state::CELL_VALUE_BYTES:
            if (read_bytes(data, u32(), _val) == read_status::ready) {
                // If the whole string is in our buffer, great, we don't
                // need to copy, and can skip the CELL_VALUE_BYTES_2 state.
                //
                // finally pass it to the consumer:
                if (_deleted) {
                    if (_val.size() != 4) {
                        throw malformed_sstable_exception("deleted cell expects local_deletion_time value");
                    }
                    deletion_time del;
                    del.local_deletion_time = consume_be<uint32_t>(_val);
                    del.marked_for_delete_at = u64();
                    _consumer.consume_deleted_cell(to_bytes_view(_key), del);
                } else {
                    _consumer.consume_cell(to_bytes_view(_key),
                            to_bytes_view(_val), u64(), _ttl, _expiration);
                }
                // after calling the consume function, we can release the
                // buffers we held for it.
                _key.release();
                _val.release();
                _state = state::ATOM_START;
            } else {
                _state = state::CELL_VALUE_BYTES_2;
            }
            break;
        case state::CELL_VALUE_BYTES_2:
            if (_deleted) {
                if (_val.size() != 4) {
                    throw malformed_sstable_exception("deleted cell expects local_deletion_time value");
                }
                deletion_time del;
                del.local_deletion_time = consume_be<uint32_t>(_val);
                del.marked_for_delete_at = u64();
                _consumer.consume_deleted_cell(to_bytes_view(_key), del);
            } else {
                _consumer.consume_cell(to_bytes_view(_key),
                        to_bytes_view(_val), u64(), _ttl, _expiration);
            }
            // after calling the consume function, we can release the
            // buffers we held for it.
            _key.release();
            _val.release();
            _state = state::ATOM_START;
            break;
        case state::RANGE_TOMBSTONE:
            read_16(data);
            _state = state::RANGE_TOMBSTONE_2;
            break;
        case state::RANGE_TOMBSTONE_2:
            // read the end column into _val.
            read_bytes(data, u16(), _val);
            _state = state::RANGE_TOMBSTONE_3;
            break;
        case state::RANGE_TOMBSTONE_3:
            read_32(data);
            _state = state::RANGE_TOMBSTONE_4;
            break;
        case state::RANGE_TOMBSTONE_4:
            read_64(data);
            _state = state::RANGE_TOMBSTONE_5;
            break;
        case state::RANGE_TOMBSTONE_5:
        {
            deletion_time del;
            del.local_deletion_time = u32();
            del.marked_for_delete_at = u64();
            _consumer.consume_range_tombstone(to_bytes_view(_key),
                    to_bytes_view(_val), del);
            _key.release();
            _val.release();
            _state = state::ATOM_START;
            break;
        }

        default:
            throw malformed_sstable_exception("unknown state");
        }
        return continuous_data_consumer::proceed::yes;
    }
};

// data_consume_rows() and data_consume_rows_at_once() both can read just a
// single row or many rows. The difference is that data_consume_rows_at_once()
// is optimized to reading one or few rows (reading it all into memory), while
// data_consume_rows() uses a read buffer, so not all the rows need to fit
// memory in the same time (they are delivered to the consumer one by one).
class data_consume_context::impl {
private:
    std::unique_ptr<data_consume_rows_context> _ctx;
public:
    impl(row_consumer& consumer,
            input_stream<char>&& input, uint64_t maxlen) :
                _ctx(new data_consume_rows_context(consumer, std::move(input), maxlen)) { }
    future<> read() {
        return _ctx->consume_input(*_ctx);
    }
};

data_consume_context::~data_consume_context() = default;
data_consume_context::data_consume_context(data_consume_context&&) = default;
data_consume_context& data_consume_context::operator=(data_consume_context&&) = default;
data_consume_context::data_consume_context(std::unique_ptr<impl> p) : _pimpl(std::move(p)) { }
future<> data_consume_context::read() {
    return _pimpl->read();
}

data_consume_context sstable::data_consume_rows(
        row_consumer& consumer, uint64_t start, uint64_t end) {
    return std::make_unique<data_consume_context::impl>(
            consumer, data_stream_at(start), end - start);
}

data_consume_context sstable::data_consume_rows(row_consumer& consumer) {
    return data_consume_rows(consumer, 0, data_size());
}

future<> sstable::data_consume_rows_at_once(row_consumer& consumer,
        uint64_t start, uint64_t end) {
    return data_read(start, end - start).then([&consumer]
                                               (temporary_buffer<char> buf) {
        data_consume_rows_context ctx(consumer, input_stream<char>(), -1);
        ctx.process(buf);
        ctx.verify_end_state();
    });
}

}
