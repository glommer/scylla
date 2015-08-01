#pragma once

/*
 * Copyright 2015 Cloudius Systems.
 */

// This class is the parts of java.util.UUID that we need

#include <stdint.h>
#include <cassert>
#include <array>
#include <iostream>

#include "core/sstring.hh"
#include "core/print.hh"
#include "net/byteorder.hh"
#include "bytes.hh"
#include "utils/serialization.hh"

namespace utils {

class UUID {
private:
    int64_t most_sig_bits;
    int64_t least_sig_bits;
public:
    UUID() : most_sig_bits(0), least_sig_bits(0) {}
    UUID(int64_t most_sig_bits, int64_t least_sig_bits)
        : most_sig_bits(most_sig_bits), least_sig_bits(least_sig_bits) {}
    explicit UUID(const sstring& uuid_string) : UUID(sstring_view(uuid_string)) { }
    explicit UUID(sstring_view uuid_string);

    int64_t get_most_significant_bits() const {
        return most_sig_bits;
    }
    int64_t get_least_significant_bits() const {
        return least_sig_bits;
    }
    int version() const {
        return (most_sig_bits >> 12) & 0xf;
    }

    int64_t timestamp() const {
        //if (version() != 1) {
        //     throw new UnsupportedOperationException("Not a time-based UUID");
        //}
        assert(version() == 1);

        return ((most_sig_bits & 0xFFF) << 48) |
               (((most_sig_bits >> 16) & 0xFFFF) << 32) |
               (((uint64_t)most_sig_bits) >> 32);

    }

    // This matches Java's UUID.toString() actual implementation. Note that
    // that method's documentation suggest something completely different!
    sstring to_sstring() const {
        return sprint("%08x%04x%04x%04x%012x",
                ((uint64_t)most_sig_bits >> 32),
                ((uint64_t)most_sig_bits >> 16 & 0xffff),
                ((uint64_t)most_sig_bits & 0xffff),
                ((uint64_t)least_sig_bits >> 48 & 0xffff),
                ((uint64_t)least_sig_bits & 0xffffffffffffLL));
    }

    friend std::ostream& operator<<(std::ostream& out, const UUID& uuid);

    bool operator==(const UUID& v) const {
        return most_sig_bits == v.most_sig_bits
                && least_sig_bits == v.least_sig_bits
                ;
    }

    bool operator<(const UUID& v) const {
         if (most_sig_bits != v.most_sig_bits) {
             return most_sig_bits < v.most_sig_bits;
         } else {
             return least_sig_bits < v.least_sig_bits;
         }
    }

    bytes to_bytes() const {
        bytes b(bytes::initialized_later(),16);
        auto i = b.begin();
        serialize_int64(i, most_sig_bits);
        serialize_int64(i, least_sig_bits);
        return b;
    }

    void serialize(bytes::iterator& out) const;
    static UUID deserialize(bytes_view& v);
    size_t serialized_size() const;
};

UUID make_random_uuid();

}

namespace std {
template<>
struct hash<utils::UUID> {
    size_t operator()(const utils::UUID& id) const {
        auto hilo = id.get_most_significant_bits()
                ^ id.get_least_significant_bits();
        return size_t((hilo >> 32) ^ hilo);
    }
};
}
