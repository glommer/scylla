/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include <boost/lexical_cast.hpp>
#include <algorithm>
#include "cql3/cql3_type.hh"
#include "types.hh"
#include "core/print.hh"
#include "net/ip.hh"
#include "database.hh"
#include "util/serialization.hh"
#include "combine.hh"
#include <cmath>
#include <sstream>
#include <regex>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/numeric.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/c_local_time_adjustor.hpp>
#include <boost/locale/encoding_utf.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

template<typename T>
struct simple_type_traits {
    static T read_nonempty(bytes_view v) {
        return read_simple_exactly<T>(v);
    }
};

template<>
struct simple_type_traits<bool> {
    static bool read_nonempty(bytes_view v) {
        return read_simple_exactly<int8_t>(v) != 0;
    }
};

template<>
struct simple_type_traits<db_clock::time_point> {
    static db_clock::time_point read_nonempty(bytes_view v) {
        return db_clock::time_point(db_clock::duration(read_simple_exactly<int64_t>(v)));
    }
};

template <typename T>
struct simple_type_impl : abstract_type {
    simple_type_impl(sstring name) : abstract_type(std::move(name)) {}
    virtual int32_t compare(bytes_view v1, bytes_view v2) const override {
        if (v1.empty()) {
            return v2.empty() ? 0 : -1;
        }
        if (v2.empty()) {
            return 1;
        }
        T a = simple_type_traits<T>::read_nonempty(v1);
        T b = simple_type_traits<T>::read_nonempty(v2);
        return a == b ? 0 : a < b ? -1 : 1;
    }
    virtual bool less(bytes_view v1, bytes_view v2) const override {
        return compare(v1, v2) < 0;
    }
    virtual bool is_byte_order_equal() const override {
        return true;
    }
    virtual size_t hash(bytes_view v) const override {
        return std::hash<bytes_view>()(v);
    }
};

template<typename T>
struct integer_type_impl : simple_type_impl<T> {
    integer_type_impl(sstring name) : simple_type_impl<T>(name) {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) const override {
        if (value.empty()) {
            return;
        }
        auto v = boost::any_cast<const T&>(value);
        auto u = net::hton(v);
        out = std::copy_n(reinterpret_cast<const char*>(&u), sizeof(u), out);
    }
    virtual size_t serialized_size(const boost::any& value) const override {
        if (value.empty()) {
            return 0;
        }
        auto v = boost::any_cast<const T&>(value);
        return sizeof(v);
    }
    virtual boost::any deserialize(bytes_view v) const override {
        if (v.empty()) {
            return {};
        }
        return read_simple_opt<T>(v);
    }
    T compose_value(const bytes& b) const {
        if (b.size() != sizeof(T)) {
            throw marshal_exception();
        }
        return (T)net::ntoh(*reinterpret_cast<const T*>(b.begin()));
    }
    bytes decompose_value(T v) const {
        bytes b(bytes::initialized_later(), sizeof(v));
        *reinterpret_cast<T*>(b.begin()) = (T)net::hton(v);
        return b;
    }
    virtual void validate(bytes_view v) const override {
        if (v.size() != 0 && v.size() != sizeof(T)) {
            throw marshal_exception();
        }
    }
    T parse_int(sstring_view s) const {
        try {
            return boost::lexical_cast<T>(s.begin(), s.size());
        } catch (const boost::bad_lexical_cast& e) {
            throw marshal_exception(sprint("Invalid number format '%s'", s));
        }
    }
    virtual bytes from_string(sstring_view s) const override {
        return decompose_value(parse_int(s));
    }
    virtual sstring to_string(const bytes& b) const override {
        if (b.empty()) {
            return {};
        }
        return to_sstring(compose_value(b));
    }
};

struct int32_type_impl : integer_type_impl<int32_t> {
    int32_type_impl() : integer_type_impl{"org.apache.cassandra.db.marshal.Int32Type"}
    { }

    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() const override {
        return cql3::cql3_type::int_;
    }
};

struct long_type_impl : integer_type_impl<int64_t> {
    long_type_impl() : integer_type_impl{"org.apache.cassandra.db.marshal.LongType"}
    { }

    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() const override {
        return cql3::cql3_type::bigint;
    }
    virtual bool is_value_compatible_with_internal(const abstract_type& other) const override {
        return &other == this || &other == date_type.get() || &other == timestamp_type.get();
    }
};

struct string_type_impl : public abstract_type {
    string_type_impl(sstring name, std::function<shared_ptr<cql3::cql3_type>()> cql3_type)
        : abstract_type(name), _cql3_type(cql3_type) {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) const override {
        if (value.empty()) {
            return;
        }
        auto& v = boost::any_cast<const sstring&>(value);
        out = std::copy(v.begin(), v.end(), out);
    }
    virtual size_t serialized_size(const boost::any& value) const override {
        if (value.empty()) {
            return 0;
        }
        auto& v = boost::any_cast<const sstring&>(value);
        return v.size();
    }
    virtual boost::any deserialize(bytes_view v) const override {
        if (v.empty()) {
            return {};
        }
        // FIXME: validation?
        return boost::any(sstring(reinterpret_cast<const char*>(v.begin()), v.size()));
    }
    virtual bool less(bytes_view v1, bytes_view v2) const override {
        return less_unsigned(v1, v2);
    }
    virtual bool is_byte_order_equal() const override {
        return true;
    }
    virtual bool is_byte_order_comparable() const override {
        return true;
    }
    virtual size_t hash(bytes_view v) const override {
        return std::hash<bytes_view>()(v);
    }
    virtual void validate(bytes_view v) const override {
        if (as_cql3_type() == cql3::cql3_type::ascii) {
            if (std::any_of(v.begin(), v.end(), [] (int8_t b) { return b < 0; })) {
                throw marshal_exception();
            }
        } else {
            try {
                boost::locale::conv::utf_to_utf<char>(v.data(), boost::locale::conv::stop);
            } catch (const boost::locale::conv::conversion_error& ex) {
                throw marshal_exception(ex.what());
            }
        }
    }
    virtual bytes from_string(sstring_view s) const override {
        return to_bytes(bytes_view(reinterpret_cast<const int8_t*>(s.begin()), s.size()));
    }
    virtual sstring to_string(const bytes& b) const override {
        return sstring(reinterpret_cast<const char*>(b.begin()), b.size());
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() const override {
        return _cql3_type();
    }
    std::function<shared_ptr<cql3::cql3_type>()> _cql3_type;
};

struct bytes_type_impl final : public abstract_type {
    bytes_type_impl() : abstract_type("org.apache.cassandra.db.marshal.BytesType") {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) const override {
        if (value.empty()) {
            return;
        }
        auto& v = boost::any_cast<const bytes&>(value);
        out = std::copy(v.begin(), v.end(), out);
    }
    virtual size_t serialized_size(const boost::any& value) const override {
        if (value.empty()) {
            return 0;
        }
        auto& v = boost::any_cast<const bytes&>(value);
        return v.size();
    }
    virtual boost::any deserialize(bytes_view v) const override {
        if (v.empty()) {
            return {};
        }
        return boost::any(bytes(v.begin(), v.end()));
    }
    virtual bool less(bytes_view v1, bytes_view v2) const override {
        return less_unsigned(v1, v2);
    }
    virtual bool is_byte_order_equal() const override {
        return true;
    }
    virtual bool is_byte_order_comparable() const override {
        return true;
    }
    virtual size_t hash(bytes_view v) const override {
        return std::hash<bytes_view>()(v);
    }
    virtual bytes from_string(sstring_view s) const override {
        return from_hex(s);
    }
    virtual sstring to_string(const bytes& b) const override {
        return to_hex(b);
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() const override {
        return cql3::cql3_type::blob;
    }
    virtual bool is_value_compatible_with_internal(const abstract_type& other) const override {
        return true;
    }
};

struct boolean_type_impl : public simple_type_impl<bool> {
    boolean_type_impl() : simple_type_impl<bool>("org.apache.cassandra.db.marshal.BooleanType") {}
    void serialize_value(bool value, bytes::iterator& out) const {
        *out++ = char(value);
    }
    virtual void serialize(const boost::any& value, bytes::iterator& out) const override {
        if (value.empty()) {
            return;
        }
        serialize_value(boost::any_cast<bool>(value), out);
    }
    virtual size_t serialized_size(const boost::any& value) const override {
        if (value.empty()) {
            return 0;
        }
        return 1;
    }
    size_t serialized_size(bool value) const {
        return 1;
    }
    virtual boost::any deserialize(bytes_view v) const override {
        if (v.empty()) {
            return {};
        }
        if (v.size() != 1) {
            throw marshal_exception();
        }
        return boost::any(*v.begin() != 0);
    }
    virtual void validate(bytes_view v) const override {
        if (v.size() != 0 && v.size() != 1) {
            throw marshal_exception();
        }
    }
    virtual bytes from_string(sstring_view s) const override {
        sstring s_lower(s.begin(), s.end());
        std::transform(s_lower.begin(), s_lower.end(), s_lower.begin(), ::tolower);
        if (s.empty() || s_lower == "false") {
            return ::serialize_value(*this, false);
        } else if (s_lower == "true") {
            return ::serialize_value(*this, true);
        } else {
            throw marshal_exception(sprint("unable to make boolean from '%s'", s));
        }
    }
    virtual sstring to_string(const bytes& b) const override {
        if (b.empty()) {
            return "";
        }
        if (b.size() != 1) {
            throw marshal_exception();
        }
        return *b.begin() ? "true" : "false";
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() const override {
        return cql3::cql3_type::boolean;
    }
};

struct date_type_impl : public abstract_type {
    date_type_impl() : abstract_type("date") {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) const override {
        if (value.empty()) {
            return;
        }
        auto v = boost::any_cast<db_clock::time_point>(value);
        int64_t i = v.time_since_epoch().count();
        i = net::hton(uint64_t(i));
        out = std::copy_n(reinterpret_cast<const char*>(&i), sizeof(i), out);
    }
    virtual size_t serialized_size(const boost::any& value) const override {
        if (value.empty()) {
            return 0;
        }
        return 8;
    }
    virtual boost::any deserialize(bytes_view v) const override {
        if (v.empty()) {
            return {};
        }
        auto tmp = read_simple_exactly<uint64_t>(v);
        return boost::any(db_clock::time_point(db_clock::duration(tmp)));
    }
    virtual bool less(bytes_view b1, bytes_view b2) const override {
        return compare_unsigned(b1, b2);
    }
    virtual bool is_byte_order_comparable() const override {
        return true;
    }
    virtual size_t hash(bytes_view v) const override {
        return std::hash<bytes_view>()(v);
    }
    virtual bytes from_string(sstring_view s) const override {
        throw std::runtime_error("not implemented");
    }
    virtual sstring to_string(const bytes& b) const override {
        throw std::runtime_error("not implemented");
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() const override {
        return cql3::cql3_type::timestamp;
    }
    virtual bool is_value_compatible_with_internal(const abstract_type& other) const override {
        return &other == this || &other == timestamp_type.get() || &other == long_type.get();
    }
};

struct timeuuid_type_impl : public abstract_type {
    timeuuid_type_impl() : abstract_type("org.apache.cassandra.db.marshal.TimeUUIDType") {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) const override {
        if (value.empty()) {
            return;
        }
        auto& uuid = boost::any_cast<const utils::UUID&>(value);
        out = std::copy_n(uuid.to_bytes().begin(), sizeof(uuid), out);
    }
    virtual size_t serialized_size(const boost::any& value) const override {
        if (value.empty()) {
            return 0;
        }
        return 16;
    }
    virtual boost::any deserialize(bytes_view v) const override {
        if (v.empty()) {
            return {};
        }
        uint64_t msb, lsb;
        if (v.empty()) {
            return {};
        }
        msb = read_simple<uint64_t>(v);
        lsb = read_simple<uint64_t>(v);
        if (!v.empty()) {
            throw marshal_exception();
        }
        return boost::any(utils::UUID(msb, lsb));
    }
    virtual bool less(bytes_view b1, bytes_view b2) const override {
        if (b1.empty()) {
            return b2.empty() ? false : true;
        }
        if (b2.empty()) {
            return false;
        }
        auto r = compare_bytes(b1, b2);
        if (r != 0) {
            return r < 0;
        } else {
            return std::lexicographical_compare(b1.begin(), b1.end(), b2.begin(), b2.end());
        }
    }
    virtual bool is_byte_order_equal() const override {
        return true;
    }
    virtual size_t hash(bytes_view v) const override {
        return std::hash<bytes_view>()(v);
    }
    virtual void validate(bytes_view v) const override {
        if (v.size() != 0 && v.size() != 16) {
            throw marshal_exception();
        }
        auto msb = read_simple<uint64_t>(v);
        auto lsb = read_simple<uint64_t>(v);
        utils::UUID uuid(msb, lsb);
        if (uuid.version() != 1) {
            throw marshal_exception();
        }
    }
    virtual bytes from_string(sstring_view s) const override {
        if (s.empty()) {
            return bytes();
        }
        static const std::regex re("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$");
        if (!std::regex_match(s.data(), re)) {
            throw marshal_exception();
        }
        utils::UUID v(s);
        if (v.version() != 1) {
            throw marshal_exception();
        }
        return v.to_bytes();
    }
    virtual sstring to_string(const bytes& b) const override {
        auto v = deserialize(b);
        if (v.empty()) {
            return "";
        }
        return boost::any_cast<const utils::UUID&>(v).to_sstring();
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() const override {
        return cql3::cql3_type::timeuuid;
    }
private:
    static int compare_bytes(bytes_view o1, bytes_view o2) {
        auto compare_pos = [&] (unsigned pos, int mask, int ifequal) {
            int d = (o1[pos] & mask) - (o2[pos] & mask);
            return d ? d : ifequal;
        };
        return compare_pos(6, 0xf,
            compare_pos(7, 0xff,
                compare_pos(4, 0xff,
                    compare_pos(5, 0xff,
                        compare_pos(0, 0xff,
                            compare_pos(1, 0xff,
                                compare_pos(2, 0xff,
                                    compare_pos(3, 0xff, 0))))))));
    }
    friend class uuid_type_impl;
};

struct timestamp_type_impl : simple_type_impl<db_clock::time_point> {
    timestamp_type_impl() : simple_type_impl("org.apache.cassandra.db.marshal.TimestampType") {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) const override {
        if (value.empty()) {
            return;
        }
        uint64_t v = boost::any_cast<db_clock::time_point>(value).time_since_epoch().count();
        v = net::hton(v);
        out = std::copy_n(reinterpret_cast<const char*>(&v), sizeof(v), out);
    }
    virtual size_t serialized_size(const boost::any& value) const override {
        if (value.empty()) {
            return 0;
        }
        return 8;
    }
    virtual boost::any deserialize(bytes_view in) const override {
        if (in.empty()) {
            return {};
        }
        auto v = read_simple_exactly<uint64_t>(in);
        return boost::any(db_clock::time_point(db_clock::duration(v)));
    }
    // FIXME: isCompatibleWith(timestampuuid)
    virtual void validate(bytes_view v) const override {
        if (v.size() != 0 && v.size() != sizeof(uint64_t)) {
            throw marshal_exception();
        }
    }
    boost::posix_time::ptime get_time(const std::string& s) const {
        // Apparently, the code below doesn't leak the input facet.
        // std::locale::facet has some internal, custom reference counting
        // and deletes the object when it's no longer used.
        auto tif = new boost::posix_time::time_input_facet("%Y-%m-%d %H:%M:%S%F");
        std::istringstream ss(s);
        ss.imbue(std::locale(ss.getloc(), tif));
        boost::posix_time::ptime t;
        ss >> t;
        if (ss.fail() || ss.peek() != std::istringstream::traits_type::eof()) {
            throw marshal_exception();
        }
        return t;
    }
    boost::posix_time::time_duration get_utc_offset(const std::string& s) const {
        static constexpr const char* formats[] = {
            "%H:%M",
            "%H%M",
        };
        for (auto&& f : formats) {
            auto tif = new boost::posix_time::time_input_facet(f);
            std::istringstream ss(s);
            ss.imbue(std::locale(ss.getloc(), tif));
            auto sign = ss.get();
            boost::posix_time::ptime p;
            ss >> p;
            if (ss.good() && ss.peek() == std::istringstream::traits_type::eof()) {
                return p.time_of_day() * (sign == '-' ? -1 : 1);
            }
        }
        throw marshal_exception();
    }
    int64_t timestamp_from_string(sstring_view s) const {
        std::string str;
        str.resize(s.size());
        std::transform(s.begin(), s.end(), str.begin(), ::tolower);
        if (str == "now") {
            return db_clock::now().time_since_epoch().count();
        }

        char* end;
        auto v = std::strtoll(s.begin(), &end, 10);
        if (end == s.begin() + s.size()) {
            return v;
        }

        std::regex date_re("^\\d{4}-\\d{2}-\\d{2}([ t]\\d{2}:\\d{2}(:\\d{2}(\\.\\d+)?)?)?");
        std::smatch dsm;
        if (!std::regex_search(str, dsm, date_re)) {
            throw marshal_exception();
        }
        auto t = get_time(dsm.str());

        auto tz = dsm.suffix().str();
        std::regex tz_re("([\\+-]\\d{2}:?(\\d{2})?)");
        std::smatch tsm;
        if (std::regex_match(tz, tsm, tz_re)) {
            t -= get_utc_offset(tsm.str());
        } else if (tz.empty()) {
            typedef boost::date_time::c_local_adjustor<boost::posix_time::ptime> local_tz;
            // local_tz::local_to_utc(), where are you?
            auto t1 = local_tz::utc_to_local(t);
            auto tz_offset = t1 - t;
            auto t2 = local_tz::utc_to_local(t - tz_offset);
            auto dst_offset = t2 - t;
            t -= tz_offset + dst_offset;
        } else {
            throw marshal_exception();
        }
        return (t - boost::posix_time::from_time_t(0)).total_milliseconds();
    }
    virtual bytes from_string(sstring_view s) const override {
        if (s.empty()) {
            return bytes();
        }
        int64_t ts;
        try {
            ts = timestamp_from_string(s);
        } catch (...) {
            throw marshal_exception(sprint("unable to parse date '%s'", s));
        }
        bytes b(bytes::initialized_later(), sizeof(int64_t));
        *unaligned_cast<int64_t*>(b.begin()) = net::hton(ts);
        return b;
    }
    virtual sstring to_string(const bytes& b) const override {
        throw std::runtime_error("not implemented");
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() const override {
        return cql3::cql3_type::timestamp;
    }
    virtual bool is_value_compatible_with_internal(const abstract_type& other) const override {
        return &other == this || &other == date_type.get() || &other == long_type.get();
    }
};

struct uuid_type_impl : abstract_type {
    uuid_type_impl() : abstract_type("org.apache.cassandra.db.marshal.UUIDType") {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) const override {
        if (value.empty()) {
            return;
        }
        auto& uuid = boost::any_cast<const utils::UUID&>(value);
        out = std::copy_n(uuid.to_bytes().begin(), sizeof(uuid), out);
    }
    virtual size_t serialized_size(const boost::any& value) const override {
        if (value.empty()) {
            return 0;
        }
        return 16;
    }
    virtual boost::any deserialize(bytes_view v) const override {
        if (v.empty()) {
            return {};
        }
        auto msb = read_simple<uint64_t>(v);
        auto lsb = read_simple<uint64_t>(v);
        if (!v.empty()) {
            throw marshal_exception();
        }
        return boost::any(utils::UUID(msb, lsb));
    }
    virtual bool less(bytes_view b1, bytes_view b2) const override {
        if (b1.size() < 16) {
            return b2.size() < 16 ? false : true;
        }
        if (b2.size() < 16) {
            return false;
        }
        auto v1 = (b1[6] >> 4) & 0x0f;
        auto v2 = (b2[6] >> 4) & 0x0f;

        if (v1 != v2) {
            return v1 < v2;
        }

        if (v1 == 1) {
            auto c1 = timeuuid_type_impl::compare_bytes(b1, b2);
            auto c2 = timeuuid_type_impl::compare_bytes(b2, b1);
            // Require strict ordering
            if (c1 != c2) {
                return c1;
            }
        }
        return less_unsigned(b1, b2);
    }
    // FIXME: isCompatibleWith(uuid)
    virtual bool is_byte_order_equal() const override {
        return true;
    }
    virtual size_t hash(bytes_view v) const override {
        return std::hash<bytes_view>()(v);
    }
    virtual void validate(bytes_view v) const override {
        if (v.size() != 0 && v.size() != 16) {
            throw marshal_exception();
        }
    }
    virtual bytes from_string(sstring_view s) const override {
        if (s.empty()) {
            return bytes();
        }
        static const std::regex re("^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$");
        if (!std::regex_match(s.data(), re)) {
            throw marshal_exception();
        }
        utils::UUID v(s);
        return v.to_bytes();
    }
    virtual sstring to_string(const bytes& b) const override {
        auto v = deserialize(b);
        if (v.empty()) {
            return "";
        }
        return boost::any_cast<const utils::UUID&>(v).to_sstring();
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() const override {
        return cql3::cql3_type::uuid;
    }
    virtual bool is_value_compatible_with_internal(const abstract_type& other) const override {
        return &other == this || &other == timeuuid_type.get();
    }
};

struct inet_addr_type_impl : abstract_type {
    inet_addr_type_impl() : abstract_type("org.apache.cassandra.db.marshal.InetAddressType") {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) const override {
        if (value.empty()) {
            return;
        }
        // FIXME: support ipv6
        auto& ipv4 = boost::any_cast<const net::ipv4_address&>(value);
        uint32_t u = htonl(ipv4.ip);
        out = std::copy_n(reinterpret_cast<const char*>(&u), sizeof(u), out);
    }
    virtual size_t serialized_size(const boost::any& value) const override {
        if (value.empty()) {
            return 0;
        }
        return 4;
    }
    virtual boost::any deserialize(bytes_view v) const override {
        if (v.empty()) {
            return {};
        }
        if (v.size() == 16) {
            throw std::runtime_error("IPv6 addresses not supported");
        }
        auto ip = read_simple<int32_t>(v);
        if (!v.empty()) {
            throw marshal_exception();
        }
        return boost::any(net::ipv4_address(ip));
    }
    virtual bool less(bytes_view v1, bytes_view v2) const override {
        return less_unsigned(v1, v2);
    }
    virtual bool is_byte_order_equal() const override {
        return true;
    }
    virtual bool is_byte_order_comparable() const override {
        return true;
    }
    virtual size_t hash(bytes_view v) const override {
        return std::hash<bytes_view>()(v);
    }
    virtual void validate(bytes_view v) const override {
        if (v.size() != 0 && v.size() != sizeof(uint32_t)) {
            throw marshal_exception();
        }
    }
    virtual bytes from_string(sstring_view s) const override {
        // FIXME: support host names
        if (s.empty()) {
            return bytes();
        }
        net::ipv4_address ipv4;
        try {
            ipv4 = net::ipv4_address(s.data());
        } catch (...) {
            throw marshal_exception();
        }
        bytes b(bytes::initialized_later(), sizeof(uint32_t));
        auto out = b.begin();
        serialize(boost::any(ipv4), out);
        return b;
    }
    virtual sstring to_string(const bytes& b) const override {
        auto v = deserialize(b);
        if (v.empty()) {
            return  "";
        }
        boost::asio::ip::address_v4 ipv4(boost::any_cast<const net::ipv4_address&>(v).ip);
        return ipv4.to_string();
    }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() const override {
        return cql3::cql3_type::inet;
    }
};

// Integer of same length of a given type. This is useful because our
// ntoh functions only know how to operate on integers.
template <typename T> struct int_of_size;
template <typename D, typename I> struct int_of_size_impl {
    using dtype = D;
    using itype = I;
    static_assert(sizeof(dtype) == sizeof(itype), "size mismatch");
    static_assert(alignof(dtype) == alignof(itype), "align mismatch");
};
template <> struct int_of_size<double> :
    public int_of_size_impl<double, uint64_t> {};
template <> struct int_of_size<float> :
    public int_of_size_impl<float, uint32_t> {};

template <typename T>
struct float_type_traits {
    static double read_nonempty(bytes_view v) {
        union {
            T d;
            typename int_of_size<T>::itype i;
        } x;
        x.i = read_simple_exactly<typename int_of_size<T>::itype>(v);
        return x.d;
    }
};
template<> struct simple_type_traits<float> : public float_type_traits<float> {};
template<> struct simple_type_traits<double> : public float_type_traits<double> {};


template <typename T>
struct floating_type_impl : public simple_type_impl<T> {
    floating_type_impl(sstring name) : simple_type_impl<T>(std::move(name)) {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) const override {
        if (value.empty()) {
            return;
        }
        T d = boost::any_cast<const T&>(value);
        if (std::isnan(d)) {
            // Java's Double.doubleToLongBits() documentation specifies that
            // any nan must be serialized to the same specific value
            d = std::numeric_limits<T>::quiet_NaN();
        }
        union {
            T d;
            typename int_of_size<T>::itype i;
        } x;
        x.d = d;
        auto u = net::hton(x.i);
        out = std::copy_n(reinterpret_cast<const char*>(&u), sizeof(u), out);
    }
    virtual size_t serialized_size(const boost::any& value) const override {
        if (value.empty()) {
            return 0;
        }
        return sizeof(T);
    }

    virtual boost::any deserialize(bytes_view v) const override {
        if (v.empty()) {
            return {};
        }
        union {
            T d;
            typename int_of_size<T>::itype i;
        } x;
        x.i = read_simple<typename int_of_size<T>::itype>(v);
        if (!v.empty()) {
            throw marshal_exception();
        }
        return boost::any(x.d);
    }
    virtual int32_t compare(bytes_view v1, bytes_view v2) const override {
        if (v1.empty()) {
            return v2.empty() ? 0 : -1;
        }
        if (v2.empty()) {
            return 1;
        }
        T a = simple_type_traits<T>::read_nonempty(v1);
        T b = simple_type_traits<T>::read_nonempty(v2);

        // in java world NaN == NaN and NaN is greater than anything else
        if (std::isnan(a) && std::isnan(b)) {
            return 0;
        } else if (std::isnan(a)) {
            return 1;
        } else if (std::isnan(b)) {
            return -1;
        }
        // also -0 < 0
        if (std::signbit(a) && !std::signbit(b)) {
            return -1;
        } else if (!std::signbit(a) && std::signbit(b))  {
            return 1;
        }
        return a == b ? 0 : a < b ? -1 : 1;
    }
    virtual void validate(bytes_view v) const override {
        if (v.size() != 0 && v.size() != sizeof(T)) {
            throw marshal_exception();
        }
    }
    virtual bytes from_string(sstring_view s) const override {
        if (s.empty()) {
            return bytes();
        }
        try {
            auto d = boost::lexical_cast<T>(s.begin(), s.size());
            bytes b(bytes::initialized_later(), sizeof(T));
            auto out = b.begin();
            serialize(boost::any(d), out);
            return b;
        }
        catch(const boost::bad_lexical_cast& e) {
            throw marshal_exception(sprint("Invalid number format '%s'", s));
        }
    }
    virtual sstring to_string(const bytes& b) const override {
        auto v = deserialize(b);
        if (v.empty()) {
            return "";
        }
        return to_sstring(boost::any_cast<T>(v));
    }
};

struct double_type_impl : floating_type_impl<double> {
    double_type_impl() : floating_type_impl{"org.apache.cassandra.db.marshal.DoubleType"} { }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() const override {
        return cql3::cql3_type::double_;
    }
};

struct float_type_impl : floating_type_impl<float> {
    float_type_impl() : floating_type_impl{"org.apache.cassandra.db.marshal.FloatType"} { }
    virtual ::shared_ptr<cql3::cql3_type> as_cql3_type() const override {
        return cql3::cql3_type::float_;
    }
};

struct empty_type_impl : abstract_type {
    empty_type_impl() : abstract_type("org.apache.cassandra.db.marshal.EmptyType") {}
    virtual void serialize(const boost::any& value, bytes::iterator& out) const override {
    }
    virtual size_t serialized_size(const boost::any& value) const override {
        return 0;
    }

    virtual bool less(bytes_view v1, bytes_view v2) const override {
        return false;
    }
    virtual size_t hash(bytes_view v) const override {
        return 0;
    }
    virtual boost::any deserialize(bytes_view v) const override {
        return {};
    }
    virtual sstring to_string(const bytes& b) const override {
        // FIXME:
        abort();
    }
    virtual bytes from_string(sstring_view text) const override {
        // FIXME:
        abort();
    }
    virtual shared_ptr<cql3::cql3_type> as_cql3_type() const override {
        // Can't happen
        abort();
    }
};


thread_local logging::logger collection_type_impl::_logger("collection_type_impl");
const size_t collection_type_impl::max_elements;

const collection_type_impl::kind collection_type_impl::kind::map(
        [] (shared_ptr<cql3::column_specification> collection, bool is_key) -> shared_ptr<cql3::column_specification> {
            // FIXME: implement
            // return isKey ? Maps.keySpecOf(collection) : Maps.valueSpecOf(collection);
            abort();
        });
const collection_type_impl::kind collection_type_impl::kind::set(
        [] (shared_ptr<cql3::column_specification> collection, bool is_key) -> shared_ptr<cql3::column_specification> {
            // FIXME: implement
            // return Sets.valueSpecOf(collection);
            abort();
        });
const collection_type_impl::kind collection_type_impl::kind::list(
        [] (shared_ptr<cql3::column_specification> collection, bool is_key) -> shared_ptr<cql3::column_specification> {
            // FIXME: implement
            // return Lists.valueSpecOf(collection);
            abort();
        });

shared_ptr<cql3::column_specification>
collection_type_impl::kind::make_collection_receiver(shared_ptr<cql3::column_specification> collection, bool is_key) const {
    return _impl(std::move(collection), is_key);
}

shared_ptr<cql3::column_specification>
collection_type_impl::make_collection_receiver(shared_ptr<cql3::column_specification> collection, bool is_key) const {
    return _kind.make_collection_receiver(std::move(collection), is_key);
}

std::vector<atomic_cell>
collection_type_impl::enforce_limit(std::vector<atomic_cell> cells, int version) const {
    assert(is_multi_cell());
    if (version >= 3 || cells.size() <= max_elements) {
        return cells;
    }
    _logger.error("Detected collection with {} elements, more than the {} limit. Only the first {} elements will be returned to the client. "
            "Please see http://cassandra.apache.org/doc/cql3/CQL.html#collections for more details.", cells.size(), max_elements, max_elements);
    cells.erase(cells.begin() + max_elements, cells.end());
    return cells;
}

bytes
collection_type_impl::serialize_for_native_protocol(std::vector<atomic_cell> cells, int version) const {
    assert(is_multi_cell());
    cells = enforce_limit(std::move(cells), version);
    std::vector<bytes> values = serialized_values(std::move(cells));
    // FIXME: implement
    abort();
    // return CollectionSerializer.pack(values, cells.size(), version);
}

bool
collection_type_impl::is_compatible_with(const abstract_type& previous) const {
    // FIXME: implement
    abort();
}

shared_ptr<cql3::cql3_type>
collection_type_impl::as_cql3_type() const {
    if (!_cql3_type) {
        auto name = cql3_type_name();
        if (!is_multi_cell()) {
            name = "frozen<" + name + ">";
        }
        _cql3_type = make_shared<cql3::cql3_type>(name, shared_from_this(), false);
    }
    return _cql3_type;
}

bytes
collection_type_impl::to_value(collection_mutation::view mut, serialization_format sf) const {
    return to_value(deserialize_mutation_form(mut), sf);
}

collection_type_impl::mutation
collection_type_impl::mutation_view::materialize() const {
    collection_type_impl::mutation m;
    m.tomb = tomb;
    m.cells.reserve(cells.size());
    for (auto&& e : cells) {
        m.cells.emplace_back(bytes(e.first.begin(), e.first.end()), e.second);
    }
    return m;
}


size_t collection_size_len(serialization_format sf) {
    if (sf.using_32_bits_for_collections()) {
        return sizeof(int32_t);
    }
    return sizeof(uint16_t);
}

size_t collection_value_len(serialization_format sf) {
    if (sf.using_32_bits_for_collections()) {
        return sizeof(int32_t);
    }
    return sizeof(uint16_t);
}


int read_collection_size(bytes_view& in, serialization_format sf) {
    if (sf.using_32_bits_for_collections()) {
        return read_simple<int32_t>(in);
    } else {
        return read_simple<uint16_t>(in);
    }
}

void write_collection_size(bytes::iterator& out, int size, serialization_format sf) {
    if (sf.using_32_bits_for_collections()) {
        serialize_int32(out, size);
    } else {
        serialize_int16(out, uint16_t(size));
    }
}

bytes_view read_collection_value(bytes_view& in, serialization_format sf) {
    auto size = sf.using_32_bits_for_collections() ? read_simple<int32_t>(in) : read_simple<uint16_t>(in);
    return read_simple_bytes(in, size);
}

void write_collection_value(bytes::iterator& out, serialization_format sf, bytes_view val_bytes) {
    if (sf.using_32_bits_for_collections()) {
        serialize_int32(out, int32_t(val_bytes.size()));
    } else {
        serialize_int16(out, uint16_t(val_bytes.size()));
    }
    out = std::copy_n(val_bytes.begin(), val_bytes.size(), out);
}

void write_collection_value(bytes::iterator& out, serialization_format sf, data_type type, const boost::any& value) {
    size_t val_len = type->serialized_size(value);

    if (sf.using_32_bits_for_collections()) {
        serialize_int32(out, val_len);
    } else {
        serialize_int16(out, val_len);
    }

    type->serialize(value, out);
}

map_type
map_type_impl::get_instance(data_type keys, data_type values, bool is_multi_cell) {
    return intern::get_instance(std::move(keys), std::move(values), is_multi_cell);
}

map_type_impl::map_type_impl(data_type keys, data_type values, bool is_multi_cell)
        : collection_type_impl("org.apache.cassandra.db.marshal.MapType(" + keys->name() + "," + values->name() + ")", kind::map)
        , _keys(std::move(keys))
        , _values(std::move(values))
        , _is_multi_cell(is_multi_cell) {
}

data_type
map_type_impl::freeze() const {
    if (_is_multi_cell) {
        return get_instance(_keys, _values, false);
    } else {
        return shared_from_this();
    }
}

bool
map_type_impl::is_compatible_with_frozen(const collection_type_impl& previous) const {
    assert(!_is_multi_cell);
    auto* p = dynamic_cast<const map_type_impl*>(&previous);
    if (!p) {
        return false;
    }
    return _keys->is_compatible_with(*p->_keys)
            && _values->is_compatible_with(*p->_values);
}

bool
map_type_impl::is_value_compatible_with_frozen(const collection_type_impl& previous) const {
    assert(!_is_multi_cell);
    auto* p = dynamic_cast<const map_type_impl*>(&previous);
    if (!p) {
        return false;
    }
    return _keys->is_compatible_with(*p->_keys)
            && _values->is_value_compatible_with(*p->_values);
}

bool
map_type_impl::less(bytes_view o1, bytes_view o2) const {
    return compare_maps(_keys, _values, o1, o2) < 0;
}

int32_t
map_type_impl::compare_maps(data_type keys, data_type values, bytes_view o1, bytes_view o2) {
    if (o1.empty()) {
        return o2.empty() ? 0 : -1;
    } else if (o2.empty()) {
        return 1;
    }
    auto sf = serialization_format::internal();
    int size1 = read_collection_size(o1, sf);
    int size2 = read_collection_size(o2, sf);
    // FIXME: use std::lexicographical_compare()
    for (int i = 0; i < std::min(size1, size2); ++i) {
        auto k1 = read_collection_value(o1, sf);
        auto k2 = read_collection_value(o2, sf);
        auto cmp = keys->compare(k1, k2);
        if (cmp != 0) {
            return cmp;
        }
        auto v1 = read_collection_value(o1, sf);
        auto v2 = read_collection_value(o2, sf);
        cmp = values->compare(v1, v2);
        if (cmp != 0) {
            return cmp;
        }
    }
    return size1 == size2 ? 0 : (size1 < size2 ? -1 : 1);
}

void
map_type_impl::serialize(const boost::any& value, bytes::iterator& out) const {
    return serialize(value, out, serialization_format::internal());
}

size_t
map_type_impl::serialized_size(const boost::any& value) const {
    auto& m = boost::any_cast<const native_type&>(value);
    size_t len = collection_size_len(serialization_format::internal());
    size_t psz = collection_value_len(serialization_format::internal());
    for (auto&& kv : m) {
        len += psz + _keys->serialized_size(kv.first);
        len += psz + _values->serialized_size(kv.second);
    }

    return len;
}

void
map_type_impl::serialize(const boost::any& value, bytes::iterator& out, serialization_format sf) const {
    auto& m = boost::any_cast<const native_type&>(value);
    write_collection_size(out, m.size(), sf);
    for (auto&& kv : m) {
        write_collection_value(out, sf, _keys, kv.first);
        write_collection_value(out, sf, _values, kv.second);
    }
}

boost::any
map_type_impl::deserialize(bytes_view v) const {
    return deserialize(v, serialization_format::internal());
}

boost::any
map_type_impl::deserialize(bytes_view in, serialization_format sf) const {
    if (in.empty()) {
        return {};
    }
    native_type m;
    auto size = read_collection_size(in, sf);
    for (int i = 0; i < size; ++i) {
        auto kb = read_collection_value(in, sf);
        auto k = _keys->deserialize(kb);
        auto vb = read_collection_value(in, sf);
        auto v = _values->deserialize(vb);
        m.insert(m.end(), std::make_pair(std::move(k), std::move(v)));
    }
    return { std::move(m) };
}

sstring
map_type_impl::to_string(const bytes& b) const {
    // FIXME:
    abort();
}

size_t
map_type_impl::hash(bytes_view v) const {
    // FIXME:
    abort();
}

bytes
map_type_impl::from_string(sstring_view text) const {
    // FIXME:
    abort();
}

std::vector<bytes>
map_type_impl::serialized_values(std::vector<atomic_cell> cells) const {
    // FIXME:
    abort();
}

bytes
map_type_impl::to_value(mutation_view mut, serialization_format sf) const {
    std::vector<bytes_view> tmp;
    tmp.reserve(mut.cells.size() * 2);
    for (auto&& e : mut.cells) {
        if (e.second.is_live(mut.tomb)) {
            tmp.emplace_back(e.first);
            tmp.emplace_back(e.second.value());
        }
    }
    return pack(tmp.begin(), tmp.end(), tmp.size() / 2, sf);
}

bytes
map_type_impl::serialize_partially_deserialized_form(
        const std::vector<std::pair<bytes_view, bytes_view>>& v, serialization_format sf) {
    size_t len = collection_value_len(sf) * v.size() * 2 + collection_size_len(sf);
    for (auto&& e : v) {
        len += e.first.size() + e.second.size();
    }
    bytes b(bytes::initialized_later(), len);
    bytes::iterator out = b.begin();

    write_collection_size(out, v.size(), sf);
    for (auto&& e : v) {
        write_collection_value(out, sf, e.first);
        write_collection_value(out, sf, e.second);
    }
    return b;


}
sstring
map_type_impl::cql3_type_name() const {
    return sprint("map<%s, %s>", _keys->as_cql3_type(), _values->as_cql3_type());
}

auto collection_type_impl::deserialize_mutation_form(collection_mutation::view cm) const -> mutation_view {
    auto&& in = cm.data;
    mutation_view ret;
    auto has_tomb = read_simple<bool>(in);
    if (has_tomb) {
        auto ts = read_simple<api::timestamp_type>(in);
        auto ttl = read_simple<gc_clock::duration::rep>(in);
        ret.tomb = tombstone{ts, gc_clock::time_point(gc_clock::duration(ttl))};
    }
    auto nr = read_simple<uint32_t>(in);
    ret.cells.reserve(nr);
    for (uint32_t i = 0; i != nr; ++i) {
        // FIXME: we could probably avoid the need for size
        auto ksize = read_simple<uint32_t>(in);
        auto key = read_simple_bytes(in, ksize);
        auto vsize = read_simple<uint32_t>(in);
        auto value = atomic_cell_view::from_bytes(read_simple_bytes(in, vsize));
        ret.cells.emplace_back(key, value);
    }
    assert(in.empty());
    return ret;
}

bool collection_type_impl::is_empty(collection_mutation::view cm) const {
    auto&& in = cm.data;
    auto has_tomb = read_simple<bool>(in);
    if (has_tomb) {
        in.remove_prefix(sizeof(api::timestamp_type) + sizeof(gc_clock::duration::rep));
    }
    return read_simple<uint32_t>(in) == 0;
}

bool collection_type_impl::is_any_live(collection_mutation::view cm, tombstone tomb, gc_clock::time_point now) const {
    auto&& in = cm.data;
    auto has_tomb = read_simple<bool>(in);
    if (has_tomb) {
        auto ts = read_simple<api::timestamp_type>(in);
        auto ttl = read_simple<gc_clock::duration::rep>(in);
        tomb.apply(tombstone{ts, gc_clock::time_point(gc_clock::duration(ttl))});
    }
    auto nr = read_simple<uint32_t>(in);
    for (uint32_t i = 0; i != nr; ++i) {
        auto ksize = read_simple<uint32_t>(in);
        in.remove_prefix(ksize);
        auto vsize = read_simple<uint32_t>(in);
        auto value = atomic_cell_view::from_bytes(read_simple_bytes(in, vsize));
        if (value.is_live(tomb, now)) {
            return true;
        }
    }
    return false;
}

template <typename Iterator>
collection_mutation::one
do_serialize_mutation_form(
        std::experimental::optional<tombstone> tomb,
        boost::iterator_range<Iterator> cells) {
    auto element_size = [] (size_t c, auto&& e) -> size_t {
        return c + 8 + e.first.size() + e.second.serialize().size();
    };
    auto size = accumulate(cells, (size_t)4, element_size);
    size += 1;
    if (tomb) {
        size += sizeof(tomb->timestamp) + sizeof(tomb->deletion_time);
    }
    bytes ret(bytes::initialized_later(), size);
    bytes::iterator out = ret.begin();
    *out++ = bool(tomb);
    if (tomb) {
        write(out, tomb->timestamp);
        write(out, tomb->deletion_time.time_since_epoch().count());
    }
    auto writeb = [&out] (bytes_view v) {
        serialize_int32(out, v.size());
        out = std::copy_n(v.begin(), v.size(), out);
    };
    // FIXME: overflow?
    serialize_int32(out, boost::distance(cells));
    for (auto&& kv : cells) {
        auto&& k = kv.first;
        auto&& v = kv.second;
        writeb(k);
        writeb(v.serialize());
    }
    return collection_mutation::one{std::move(ret)};
}

bool collection_type_impl::mutation::compact_and_expire(tombstone base_tomb, gc_clock::time_point query_time) {
    bool any_live = false;
    tomb.apply(base_tomb);
    std::vector<std::pair<bytes, atomic_cell>> survivors;
    for (auto&& name_and_cell : cells) {
        atomic_cell& cell = name_and_cell.second;
        if (cell.is_covered_by(tomb)) {
            continue;
        }
        if (cell.has_expired(query_time)) {
            survivors.emplace_back(std::make_pair(
                std::move(name_and_cell.first), atomic_cell::make_dead(cell.timestamp(), cell.deletion_time())));
        } else {
            any_live |= cell.is_live();
            survivors.emplace_back(std::move(name_and_cell));
        }
    }
    cells = std::move(survivors);
    return any_live;
}

collection_mutation::one
collection_type_impl::serialize_mutation_form(const mutation& mut) const {
    return do_serialize_mutation_form(mut.tomb, boost::make_iterator_range(mut.cells.begin(), mut.cells.end()));
}

collection_mutation::one
collection_type_impl::serialize_mutation_form(mutation_view mut) const {
    return do_serialize_mutation_form(mut.tomb, boost::make_iterator_range(mut.cells.begin(), mut.cells.end()));
}

collection_mutation::one
collection_type_impl::serialize_mutation_form_only_live(mutation_view mut, gc_clock::time_point now) const {
    return do_serialize_mutation_form(mut.tomb, mut.cells | boost::adaptors::filtered([t = mut.tomb, now] (auto&& e) {
        return e.second.is_live(t, now);
    }));
}

collection_mutation::one
collection_type_impl::merge(collection_mutation::view a, collection_mutation::view b) const {
    auto aa = deserialize_mutation_form(a);
    auto bb = deserialize_mutation_form(b);
    mutation_view merged;
    merged.cells.reserve(aa.cells.size() + bb.cells.size());
    using element_type = std::pair<bytes_view, atomic_cell_view>;
    auto key_type = name_comparator();
    auto compare = [key_type] (const element_type& e1, const element_type& e2) {
        return key_type->less(e1.first, e2.first);
    };
    auto merge = [this] (const element_type& e1, const element_type& e2) {
        // FIXME: use std::max()?
        return std::make_pair(e1.first, compare_atomic_cell_for_merge(e1.second, e2.second) > 0 ? e1.second : e2.second);
    };
    // applied to a tombstone, returns a predicate checking whether a cell is killed by
    // the tombstone
    auto cell_killed = [] (const std::experimental::optional<tombstone>& t) {
        return [&t] (const element_type& e) {
            if (!t) {
                return false;
            }
            // tombstone wins if timestamps equal here, unlike row tombstones
            if (t->timestamp < e.second.timestamp()) {
                return false;
            }
            return true;
            // FIXME: should we consider TTLs too?
        };
    };
    combine(aa.cells.begin(), std::remove_if(aa.cells.begin(), aa.cells.end(), cell_killed(bb.tomb)),
            bb.cells.begin(), std::remove_if(bb.cells.begin(), bb.cells.end(), cell_killed(aa.tomb)),
            std::back_inserter(merged.cells),
            compare,
            merge);
    merged.tomb = std::max(aa.tomb, bb.tomb);
    return serialize_mutation_form(merged);
}

bytes_opt
collection_type_impl::reserialize(serialization_format from, serialization_format to, bytes_view_opt v) const {
    if (!v) {
        return std::experimental::nullopt;
    }
    auto val = deserialize(*v, from);
    bytes ret(bytes::initialized_later(), serialized_size(v));  // FIXME: serialized_size want @to
    auto out = ret.begin();
    serialize(std::move(val), out, to);
    return ret;
}

// iterator that takes a set or list in serialized form, and emits
// each element, still in serialized form
class listlike_partial_deserializing_iterator
          : public std::iterator<std::input_iterator_tag, bytes_view> {
    bytes_view* _in;
    int _remain;
    bytes_view _cur;
    serialization_format _sf;
private:
    struct end_tag {};
    listlike_partial_deserializing_iterator(bytes_view& in, serialization_format sf)
            : _in(&in), _sf(sf) {
        _remain = read_collection_size(*_in, _sf);
        parse();
    }
    listlike_partial_deserializing_iterator(end_tag)
            : _remain(0), _sf(serialization_format::internal()) {  // _sf is bogus, but doesn't matter
    }
public:
    bytes_view operator*() const { return _cur; }
    listlike_partial_deserializing_iterator& operator++() {
        --_remain;
        parse();
        return *this;
    }
    void operator++(int) {
        --_remain;
        parse();
    }
    bool operator==(const listlike_partial_deserializing_iterator& x) const {
        return _remain == x._remain;
    }
    bool operator!=(const listlike_partial_deserializing_iterator& x) const {
        return _remain != x._remain;
    }
    static listlike_partial_deserializing_iterator begin(bytes_view& in, serialization_format sf) {
        return { in, sf };
    }
    static listlike_partial_deserializing_iterator end(bytes_view in, serialization_format sf) {
        return { end_tag() };
    }
private:
    void parse() {
        if (_remain) {
            _cur = read_collection_value(*_in, _sf);
        } else {
            _cur = {};
        }
    }
};

set_type
set_type_impl::get_instance(data_type elements, bool is_multi_cell) {
    return intern::get_instance(elements, is_multi_cell);
}

set_type_impl::set_type_impl(data_type elements, bool is_multi_cell)
        : collection_type_impl("org.apache.cassandra.db.marshal.SetType(" + elements->name() + ")", kind::set)
        , _elements(std::move(elements))
        , _is_multi_cell(is_multi_cell) {
}

data_type
set_type_impl::value_comparator() const {
    return empty_type;
}

data_type
set_type_impl::freeze() const {
    if (_is_multi_cell) {
        return get_instance(_elements, false);
    } else {
        return shared_from_this();
    }
}

bool
set_type_impl::is_compatible_with_frozen(const collection_type_impl& previous) const {
    assert(!_is_multi_cell);
    auto* p = dynamic_cast<const set_type_impl*>(&previous);
    if (!p) {
        return false;
    }
    return _elements->is_compatible_with(*p->_elements);

}

bool
set_type_impl::is_value_compatible_with_frozen(const collection_type_impl& previous) const {
    return is_compatible_with(previous);
}

bool
set_type_impl::less(bytes_view o1, bytes_view o2) const {
    using llpdi = listlike_partial_deserializing_iterator;
    auto sf = serialization_format::internal();
    return std::lexicographical_compare(
            llpdi::begin(o1, sf), llpdi::end(o1, sf),
            llpdi::begin(o2, sf), llpdi::end(o2, sf),
            [this] (bytes_view o1, bytes_view o2) { return _elements->less(o1, o2); });
}

void
set_type_impl::serialize(const boost::any& value, bytes::iterator& out) const {
    return serialize(value, out, serialization_format::internal());
}

size_t
set_type_impl::serialized_size(const boost::any& value) const {
    auto& s = boost::any_cast<const native_type&>(value);
    size_t len = collection_size_len(serialization_format::internal());
    size_t psz = collection_value_len(serialization_format::internal());
    for (auto&& e : s) {
        len += psz + _elements->serialized_size(e);
    }
    return len;
}



void
set_type_impl::serialize(const boost::any& value, bytes::iterator& out, serialization_format sf) const {
    auto& s = boost::any_cast<const native_type&>(value);
    write_collection_size(out, s.size(), sf);
    for (auto&& e : s) {
        write_collection_value(out, sf, _elements, e);
    }
}

boost::any
set_type_impl::deserialize(bytes_view in) const {
    return deserialize(in, serialization_format::internal());
}

boost::any
set_type_impl::deserialize(bytes_view in, serialization_format sf) const {
    if (in.empty()) {
        return {};
    }
    auto nr = read_collection_size(in, sf);
    native_type s;
    s.reserve(nr);
    for (int i = 0; i != nr; ++i) {
        auto e = _elements->deserialize(read_collection_value(in, sf));
        if (e.empty()) {
            throw marshal_exception();
        }
        s.push_back(std::move(e));
    }
    return { s };
}

sstring
set_type_impl::to_string(const bytes& b) const {
    using llpdi = listlike_partial_deserializing_iterator;
    std::ostringstream out;
    bool first = true;
    auto v = bytes_view(b);
    auto sf = serialization_format::internal();
    std::for_each(llpdi::begin(v, sf), llpdi::end(v, sf), [&first, &out, this] (bytes_view e) {
        if (first) {
            first = false;
        } else {
            out << "; ";
        }
        out << _elements->to_string(bytes(e.begin(), e.end()));
    });
    return out.str();
}

size_t
set_type_impl::hash(bytes_view v) const {
    // FIXME:
    abort();
}

bytes
set_type_impl::from_string(sstring_view text) const {
    // FIXME:
    abort();
}

std::vector<bytes>
set_type_impl::serialized_values(std::vector<atomic_cell> cells) const {
    // FIXME:
    abort();
}

bytes
set_type_impl::to_value(mutation_view mut, serialization_format sf) const {
    std::vector<bytes_view> tmp;
    tmp.reserve(mut.cells.size());
    for (auto&& e : mut.cells) {
        if (e.second.is_live(mut.tomb)) {
            tmp.emplace_back(e.first);
        }
    }
    return pack(tmp.begin(), tmp.end(), tmp.size(), sf);
}

bytes
set_type_impl::serialize_partially_deserialized_form(
        const std::vector<bytes_view>& v, serialization_format sf) const {
    return pack(v.begin(), v.end(), v.size(), sf);
}

sstring
set_type_impl::cql3_type_name() const {
    return sprint("set<%s>", _elements->as_cql3_type());
}

list_type
list_type_impl::get_instance(data_type elements, bool is_multi_cell) {
    return intern::get_instance(elements, is_multi_cell);
}

list_type_impl::list_type_impl(data_type elements, bool is_multi_cell)
        : collection_type_impl("org.apache.cassandra.db.marshal.ListType(" + elements->name() + ")", kind::list)
        , _elements(std::move(elements))
        , _is_multi_cell(is_multi_cell) {
}

data_type
list_type_impl::name_comparator() const {
    return timeuuid_type;
}

data_type
list_type_impl::value_comparator() const {
    return _elements;
}

data_type
list_type_impl::freeze() const {
    if (_is_multi_cell) {
        return get_instance(_elements, false);
    } else {
        return shared_from_this();
    }
}

bool
list_type_impl::is_compatible_with_frozen(const collection_type_impl& previous) const {
    assert(!_is_multi_cell);
    auto* p = dynamic_cast<const list_type_impl*>(&previous);
    if (!p) {
        return false;
    }
    return _elements->is_compatible_with(*p->_elements);

}

bool
list_type_impl::is_value_compatible_with_frozen(const collection_type_impl& previous) const {
    auto& lp = dynamic_cast<const list_type_impl&>(previous);
    return _elements->is_value_compatible_with_internal(*lp._elements);
}

bool
list_type_impl::less(bytes_view o1, bytes_view o2) const {
    using llpdi = listlike_partial_deserializing_iterator;
    auto sf = serialization_format::internal();
    return std::lexicographical_compare(
            llpdi::begin(o1, sf), llpdi::end(o1, sf),
            llpdi::begin(o2, sf), llpdi::end(o2, sf),
            [this] (bytes_view o1, bytes_view o2) { return _elements->less(o1, o2); });
}

void
list_type_impl::serialize(const boost::any& value, bytes::iterator& out) const {
    return serialize(value, out, serialization_format::internal());
}

void
list_type_impl::serialize(const boost::any& value, bytes::iterator& out, serialization_format sf) const {
    auto& s = boost::any_cast<const native_type&>(value);
    write_collection_size(out, s.size(), sf);
    for (auto&& e : s) {
        write_collection_value(out, sf, _elements, e);
    }
}

size_t
list_type_impl::serialized_size(const boost::any& value) const {
    auto& s = boost::any_cast<const native_type&>(value);
    size_t len = collection_size_len(serialization_format::internal());
    size_t psz = collection_value_len(serialization_format::internal());
    for (auto&& e : s) {
        len += psz + _elements->serialized_size(e);
    }
    return len;
}

boost::any
list_type_impl::deserialize(bytes_view in) const {
    return deserialize(in, serialization_format::internal());
}

boost::any
list_type_impl::deserialize(bytes_view in, serialization_format sf) const {
    if (in.empty()) {
        return {};
    }
    auto nr = read_collection_size(in, sf);
    native_type s;
    s.reserve(nr);
    for (int i = 0; i != nr; ++i) {
        auto e = _elements->deserialize(read_collection_value(in, sf));
        if (e.empty()) {
            throw marshal_exception();
        }
        s.push_back(std::move(e));
    }
    return { s };
}

sstring
list_type_impl::to_string(const bytes& b) const {
    using llpdi = listlike_partial_deserializing_iterator;
    std::ostringstream out;
    bool first = true;
    auto v = bytes_view(b);
    auto sf = serialization_format::internal();
    std::for_each(llpdi::begin(v, sf), llpdi::end(v, sf), [&first, &out, this] (bytes_view e) {
        if (first) {
            first = false;
        } else {
            out << ", ";
        }
        out << _elements->to_string(bytes(e.begin(), e.end()));
    });
    return out.str();
}

size_t
list_type_impl::hash(bytes_view v) const {
    // FIXME:
    abort();
}

bytes
list_type_impl::from_string(sstring_view text) const {
    // FIXME:
    abort();
}

std::vector<bytes>
list_type_impl::serialized_values(std::vector<atomic_cell> cells) const {
    // FIXME:
    abort();
}

bytes
list_type_impl::to_value(mutation_view mut, serialization_format sf) const {
    std::vector<bytes_view> tmp;
    tmp.reserve(mut.cells.size());
    for (auto&& e : mut.cells) {
        if (e.second.is_live(mut.tomb)) {
            tmp.emplace_back(e.second.value());
        }
    }
    return pack(tmp.begin(), tmp.end(), tmp.size(), sf);
}

sstring
list_type_impl::cql3_type_name() const {
    return sprint("list<%s>", _elements->as_cql3_type());
}

tuple_type_impl::tuple_type_impl(sstring name, std::vector<data_type> types)
        : abstract_type(std::move(name)), _types(std::move(types)) {
    for (auto& t : _types) {
        t = t->freeze();
    }
}

tuple_type_impl::tuple_type_impl(std::vector<data_type> types)
        : tuple_type_impl(make_name(types), std::move(types)) {
}

shared_ptr<tuple_type_impl>
tuple_type_impl::get_instance(std::vector<data_type> types) {
    return ::make_shared<tuple_type_impl>(std::move(types));
}

int32_t
tuple_type_impl::compare(bytes_view v1, bytes_view v2) const {
    return lexicographical_tri_compare(_types.begin(), _types.end(),
            tuple_deserializing_iterator::start(v1), tuple_deserializing_iterator::finish(v1),
            tuple_deserializing_iterator::start(v2), tuple_deserializing_iterator::finish(v2),
            tri_compare_opt);
}

bool
tuple_type_impl::less(bytes_view v1, bytes_view v2) const {
    return tuple_type_impl::compare(v1, v2) < 0;
}

size_t
tuple_type_impl::serialized_size(const boost::any& value) const {
    size_t size = 0;
    if (value.empty()) {
        return size;
    }
    auto&& v = boost::any_cast<const native_type&>(value);
    auto find_serialized_size = [] (auto&& t_v) {
        const data_type& t = boost::get<0>(t_v);
        const boost::any& v = boost::get<1>(t_v);
        return 4 + (v.empty() ? 0 : t->serialized_size(v));
    };
    return boost::accumulate(boost::combine(_types, v) | boost::adaptors::transformed(find_serialized_size), 0);
}

void
tuple_type_impl::serialize(const boost::any& value, bytes::iterator& out) const {
    if (value.empty()) {
        return;
    }
    auto&& v = boost::any_cast<const native_type&>(value);
    auto do_serialize = [&out] (auto&& t_v) {
        const data_type& t = boost::get<0>(t_v);
        const boost::any& v = boost::get<1>(t_v);
        if (v.empty()) {
            write(out, int32_t(-1));
        } else {
            write(out, int32_t(t->serialized_size(v)));
            t->serialize(v, out);
        }
    };
    boost::range::for_each(boost::combine(_types, v), do_serialize);
}

boost::any
tuple_type_impl::deserialize(bytes_view v) const {
    native_type ret;
    ret.reserve(_types.size());
    auto ti = _types.begin();
    auto vi = tuple_deserializing_iterator::start(v);
    while (ti != _types.end() && vi != tuple_deserializing_iterator::finish(v)) {
        boost::any obj;
        if (*vi) {
            obj = (*ti)->deserialize(**vi);
        }
        ret.push_back(std::move(obj));
        ++ti;
        ++vi;
    }
    ret.resize(_types.size());
    return { ret };
}

std::vector<bytes_view_opt>
tuple_type_impl::split(bytes_view v) const {
    return { tuple_deserializing_iterator::start(v), tuple_deserializing_iterator::finish(v) };
}

bytes
tuple_type_impl::from_string(sstring_view s) const {
    throw std::runtime_error("not implemented");
}

sstring
tuple_type_impl::to_string(const bytes& b) const {
    throw std::runtime_error("not implemented");
}

bool
tuple_type_impl::equals(const abstract_type& other) const {
    auto x = dynamic_cast<const tuple_type_impl*>(&other);
    return x && std::equal(_types.begin(), _types.end(), x->_types.begin(), x->_types.end(),
            [] (auto&& a, auto&& b) { return a->equals(b); });
}

bool
tuple_type_impl::is_compatible_with(const abstract_type& previous) const {
    return check_compatibility(previous, &abstract_type::is_compatible_with);
}

bool
tuple_type_impl::is_value_compatible_with_internal(const abstract_type& previous) const {
    return check_compatibility(previous, &abstract_type::is_value_compatible_with);
}

bool
tuple_type_impl::check_compatibility(const abstract_type& previous, bool (abstract_type::*predicate)(const abstract_type&) const) const {
    auto* x = dynamic_cast<const tuple_type_impl*>(&previous);
    if (!x) {
        return false;
    }
    auto c = std::mismatch(
                _types.begin(), _types.end(),
                x->_types.begin(), x->_types.end(),
                [predicate] (data_type a, data_type b) { return ((*a).*predicate)(*b); });
    return c.first == _types.end();  // previous allowed to be longer
}

size_t
tuple_type_impl::hash(bytes_view v) const {
    auto apply_hash = [] (auto&& type_value) {
        auto&& type = boost::get<0>(type_value);
        auto&& value = boost::get<1>(type_value);
        return value ? type->hash(*value) : 0;
    };
    // FIXME: better accumulation function
    return boost::accumulate(combine(_types, make_range(v))
                             | boost::adaptors::transformed(apply_hash),
                             0,
                             std::bit_xor<>());
}

shared_ptr<cql3::cql3_type>
tuple_type_impl::as_cql3_type() const {
    return cql3::make_cql3_tuple_type(static_pointer_cast<const tuple_type_impl>(shared_from_this()));
}

sstring
tuple_type_impl::make_name(const std::vector<data_type>& types) {
    return sprint("tuple<%s>", ::join(", ", types | boost::adaptors::transformed(std::mem_fn(&abstract_type::name))));
}

sstring
user_type_impl::get_name_as_string() const {
    return boost::any_cast<sstring>(utf8_type->deserialize(_name));
}

shared_ptr<cql3::cql3_type>
user_type_impl::as_cql3_type() const {
    throw "not yet";
}

sstring
user_type_impl::make_name(sstring keyspace, bytes name, std::vector<bytes> field_names, std::vector<data_type> field_types) {
    std::ostringstream os;
    os << "(" << keyspace << "," << to_hex(name);
    for (size_t i = 0; i < field_names.size(); ++i) {
        os << ",";
        os << to_hex(field_names[i]) << ":";
        os << field_types[i]->name(); // FIXME: ignore frozen<>
    }
    os << ")";
    return os.str();
}

thread_local const shared_ptr<const abstract_type> int32_type(make_shared<int32_type_impl>());
thread_local const shared_ptr<const abstract_type> long_type(make_shared<long_type_impl>());
thread_local const shared_ptr<const abstract_type> ascii_type(make_shared<string_type_impl>("org.apache.cassandra.db.marshal.AsciiType", [] { return cql3::cql3_type::ascii; }));
thread_local const shared_ptr<const abstract_type> bytes_type(make_shared<bytes_type_impl>());
thread_local const shared_ptr<const abstract_type> utf8_type(make_shared<string_type_impl>("org.apache.cassandra.db.marshal.UTF8Type", [] { return cql3::cql3_type::text; }));
thread_local const shared_ptr<const abstract_type> boolean_type(make_shared<boolean_type_impl>());
thread_local const shared_ptr<const abstract_type> date_type(make_shared<date_type_impl>());
thread_local const shared_ptr<const abstract_type> timeuuid_type(make_shared<timeuuid_type_impl>());
thread_local const shared_ptr<const abstract_type> timestamp_type(make_shared<timestamp_type_impl>());
thread_local const shared_ptr<const abstract_type> uuid_type(make_shared<uuid_type_impl>());
thread_local const shared_ptr<const abstract_type> inet_addr_type(make_shared<inet_addr_type_impl>());
thread_local const shared_ptr<const abstract_type> float_type(make_shared<float_type_impl>());
thread_local const shared_ptr<const abstract_type> double_type(make_shared<double_type_impl>());
thread_local const data_type empty_type(make_shared<empty_type_impl>());

static collection_type parse_collection_type(sstring name) {
    static sstring frozen_str("org.apache.cassandra.db.marshal.FrozenType");

    // given a string of form type(X) will return X.
    auto match_str = [&name] (sstring token) {
        if (name.compare(0, token.size(), token) == 0) {
            name = name.substr(token.size() + 1, name.size() - token.size() - 2);
            return true;
        }
        return false;
    };

    bool is_multi_cell = true;
    if (match_str(frozen_str)) {
        is_multi_cell = false;
    }

    auto hash = name.find(':');
    if (hash != sstring::npos) {
        // As long as we always use get_instance to generate the types, they
        // will always hash equally. So instead of complicating the collection
        // internals with a specialized version of get_instance to look for
        // this particular hash, just remove the string and recreate the type
        name = name.substr(hash + 1, name.size());
    }

    auto params = [&name] (size_t expected) {
        std::vector<sstring> types_str;
        boost::split(types_str, name, boost::is_any_of(","));
        if (types_str.size() != expected) {
            throw std::invalid_argument(sprint("Could not parse types in %s\n", name));
        }

        std::vector<data_type> types_vec;
        for (auto&&t : types_str) {
            types_vec.push_back(abstract_type::parse_type(t));
        }
        return types_vec;
    };

    if (match_str("org.apache.cassandra.db.marshal.MapType")) {
        auto p = params(2);
        return static_pointer_cast<const collection_type_impl>(map_type_impl::get_instance(p[0], p[1], is_multi_cell));
    } else if (match_str("org.apache.cassandra.db.marshal.SetType")) {
        auto p = params(1);
        return static_pointer_cast<const collection_type_impl>(set_type_impl::get_instance(p[0], is_multi_cell));
    } else if (match_str("org.apache.cassandra.db.marshal.ListType")) {
        auto p = params(1);
        return static_pointer_cast<const collection_type_impl>(list_type_impl::get_instance(p[0], is_multi_cell));
    } else {
        throw std::invalid_argument(sprint("unknown type: %s\n", name));
    }
}

data_type abstract_type::parse_type(const sstring& name)
{
    static thread_local const std::unordered_map<sstring, data_type> types = {
        { "org.apache.cassandra.db.marshal.Int32Type",       int32_type     },
        { "org.apache.cassandra.db.marshal.LongType",        long_type      },
        { "org.apache.cassandra.db.marshal.AsciiType",       ascii_type     },
        { "org.apache.cassandra.db.marshal.BytesType",       bytes_type     },
        { "org.apache.cassandra.db.marshal.UTF8Type",        utf8_type      },
        { "org.apache.cassandra.db.marshal.BooleanType",     boolean_type   },
        { "org.apache.cassandra.db.marshal.TimeUUIDType",    timeuuid_type  },
        { "org.apache.cassandra.db.marshal.TimestampType",   timestamp_type },
        { "org.apache.cassandra.db.marshal.UUIDType",        uuid_type      },
        { "org.apache.cassandra.db.marshal.InetAddressType", inet_addr_type },
        { "org.apache.cassandra.db.marshal.FloatType",       float_type     },
        { "org.apache.cassandra.db.marshal.DoubleType",      double_type    },
        { "org.apache.cassandra.db.marshal.EmptyType",       empty_type     },
    };
    auto it = types.find(name);
    if (it != types.end()) {
        return it->second;
    }
    return parse_collection_type(name);
}
