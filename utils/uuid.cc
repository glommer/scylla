/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */


#include "UUID.hh"
#include "net/byteorder.hh"
#include <random>
#include <boost/iterator/function_input_iterator.hpp>
#include <boost/algorithm/string.hpp>
#include <string>
#include "core/sstring.hh"
#include "util/serialization.hh"
#include "types.hh"

namespace utils {

UUID
make_random_uuid() {
    // FIXME: keep in userspace
    static thread_local std::random_device urandom;
    static thread_local std::uniform_int_distribution<uint8_t> dist(0, 255);
    union {
        uint8_t b[16];
        struct {
            uint64_t msb, lsb;
        } w;
    } v;
    for (auto& b : v.b) {
        b = dist(urandom);
    }
    v.b[6] &= 0x0f;
    v.b[6] |= 0x40; // version 4
    v.b[8] &= 0x3f;
    v.b[8] |= 0x80; // IETF variant
    return UUID(net::hton(v.w.msb), net::hton(v.w.lsb));
}

std::ostream& operator<<(std::ostream& out, const UUID& uuid) {
    return out << uuid.to_sstring();
}

UUID::UUID(sstring_view uuid) {
    auto uuid_string = sstring(uuid.begin(), uuid.size());
    boost::erase_all(uuid_string, "-");
    auto size = uuid_string.size() / 2;
    assert(size == 16);
    sstring most = sstring(uuid_string.begin(), uuid_string.begin() + size);
    sstring least = sstring(uuid_string.begin() + size, uuid_string.end());
    int base = 16;
    this->most_sig_bits = std::stoull(most, nullptr, base);
    this->least_sig_bits = std::stoull(least, nullptr, base);
}

void UUID::serialize(bytes::iterator& out) const {
   serialize_int64(out, most_sig_bits);
   serialize_int64(out, least_sig_bits);
}

UUID UUID::deserialize(bytes_view& v) {
    auto most = read_simple<int64_t>(v);
    auto least = read_simple<int64_t>(v);
    return UUID(most, least);
}

size_t UUID::serialized_size() const {
    return serialize_int64_size + serialize_int64_size;
}

}
