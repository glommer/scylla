/*
 * Copyright 2015 Cloudius Systems
 */

#ifndef DB_SERIALIZER_HH_
#define DB_SERIALIZER_HH_

#include "utils/data_input.hh"
#include "utils/data_output.hh"
#include "bytes.hh"
#include "mutation.hh"
#include "keys.hh"
#include "database_fwd.hh"
#include "frozen_mutation.hh"

namespace db {
/**
 * Serialization objects for various types and using "internal" format. (Not CQL, origin whatnot).
 * The design rationale is that a "serializer" can be instantiated for an object, and will contain
 * the obj + size, and is usable as a functor.
 *
 * Serialization can also be done "explicitly" through the static method "write"
 * (Not using "serialize", because writing "serializer<apa>::serialize" all the time is tiring and redundant)
 * though care should be takes than data will fit of course.
 */
template<typename T>
class serializer {
public:
    typedef T type;
    typedef data_output output;
    typedef data_input input;
    typedef serializer<T> _MyType;

    serializer(const type&);

    // apply to memory, must be at least size() large.
    const _MyType& operator()(output& out) const {
        write(out, _item);
        return *this;
    }

    static void write(output&, const T&);
    static void read(T&, input&);
    static T read(input&);
    static void skip(input& in);

    size_t size() const {
        return _size;
    }
private:
    const T& _item;
    size_t _size;
};

template <typename T>
class integral_serializer {
    const T& _item;
public:
    using type = T;
    using output = data_output;
    using input = data_input;
public:
    integral_serializer(const T& item) : _item(item) {}
    const serializer<T>& operator()(output& out) const {
        write(out, _item);
        return static_cast<const serializer<T>&>(*this);
    }
    static void write(output& out, const T& item) {
        out.write_integral<T>(item);
    }
    static void read(T& item, input& in) {
        item = read(in);
    }
    static T read(input& in) {
        return in.read<T>(in);
    }
    static void skip(input& in) {
        in.skip(sizeof(T));
    }
    static size_t size() {
        return sizeof(T);
    }
};

template <> struct serializer<bool> : integral_serializer<bool> { using integral_serializer<bool>::integral_serializer; };
template <> struct serializer<int8_t> : integral_serializer<int8_t> { using integral_serializer<int8_t>::integral_serializer; };
template <> struct serializer<uint8_t> : integral_serializer<uint8_t> { using integral_serializer<uint8_t>::integral_serializer; };
template <> struct serializer<int16_t> : integral_serializer<int16_t> { using integral_serializer<int16_t>::integral_serializer; };
template <> struct serializer<uint16_t> : integral_serializer<uint16_t> { using integral_serializer<uint16_t>::integral_serializer; };
template <> struct serializer<int32_t> : integral_serializer<int32_t> { using integral_serializer<int32_t>::integral_serializer; };
template <> struct serializer<uint32_t> : integral_serializer<uint32_t> { using integral_serializer<uint32_t>::integral_serializer; };
template <> struct serializer<int64_t> : integral_serializer<int64_t> { using integral_serializer<int64_t>::integral_serializer; };
template <> struct serializer<uint64_t> : integral_serializer<uint64_t> { using integral_serializer<uint64_t>::integral_serializer; };

template<> serializer<utils::UUID>::serializer(const utils::UUID &);
template<> void serializer<utils::UUID>::write(output&, const type&);
template<> void serializer<utils::UUID>::read(utils::UUID&, input&);
template<> void serializer<utils::UUID>::skip(input&);
template<> utils::UUID serializer<utils::UUID>::read(input&);

template<> serializer<bytes>::serializer(const bytes &);
template<> void serializer<bytes>::write(output&, const type&);
template<> void serializer<bytes>::read(bytes&, input&);

template<> serializer<bytes_view>::serializer(const bytes_view&);
template<> void serializer<bytes_view>::write(output&, const type&);
template<> void serializer<bytes_view>::read(bytes_view&, input&);
template<> bytes_view serializer<bytes_view>::read(input&);

template<> serializer<sstring>::serializer(const sstring&);
template<> void serializer<sstring>::write(output&, const type&);
template<> void serializer<sstring>::read(sstring&, input&);

template<> serializer<tombstone>::serializer(const tombstone &);
template<> void serializer<tombstone>::write(output&, const type&);
template<> void serializer<tombstone>::read(tombstone&, input&);

template<> serializer<atomic_cell_view>::serializer(const atomic_cell_view &);
template<> void serializer<atomic_cell_view>::write(output&, const type&);
template<> void serializer<atomic_cell_view>::read(atomic_cell_view&, input&);
template<> atomic_cell_view serializer<atomic_cell_view>::read(input&);

template<> serializer<collection_mutation::view>::serializer(const collection_mutation::view &);
template<> void serializer<collection_mutation::view>::write(output&, const type&);
template<> void serializer<collection_mutation::view>::read(collection_mutation::view&, input&);

template<> serializer<frozen_mutation>::serializer(const frozen_mutation &);
template<> void serializer<frozen_mutation>::write(output&, const type&);
template<> void serializer<frozen_mutation>::read(frozen_mutation&, input&) = delete;
template<> frozen_mutation serializer<frozen_mutation>::read(input&);

template<> serializer<partition_key_view>::serializer(const partition_key_view &);
template<> void serializer<partition_key_view>::write(output&, const partition_key_view&);
template<> void serializer<partition_key_view>::read(partition_key_view&, input&);
template<> partition_key_view serializer<partition_key_view>::read(input&);
template<> void serializer<partition_key_view>::skip(input&);

template<> serializer<clustering_key_view>::serializer(const clustering_key_view &);
template<> void serializer<clustering_key_view>::write(output&, const clustering_key_view&);
template<> void serializer<clustering_key_view>::read(clustering_key_view&, input&);
template<> clustering_key_view serializer<clustering_key_view>::read(input&);

template<> serializer<clustering_key_prefix_view>::serializer(const clustering_key_prefix_view &);
template<> void serializer<clustering_key_prefix_view>::write(output&, const clustering_key_prefix_view&);
template<> void serializer<clustering_key_prefix_view>::read(clustering_key_prefix_view&, input&);
template<> clustering_key_prefix_view serializer<clustering_key_prefix_view>::read(input&);

template<typename T>
T serializer<T>::read(input& in) {
    type t;
    read(t, in);
    return t;
}

extern template class serializer<tombstone>;
extern template class serializer<bytes>;
extern template class serializer<bytes_view>;
extern template class serializer<sstring>;
extern template class serializer<utils::UUID>;
extern template class serializer<partition_key_view>;
extern template class serializer<clustering_key_view>;
extern template class serializer<clustering_key_prefix_view>;

typedef serializer<tombstone> tombstone_serializer;
typedef serializer<bytes> bytes_serializer; // Compatible with bytes_view_serializer
typedef serializer<bytes_view> bytes_view_serializer; // Compatible with bytes_serializer
typedef serializer<sstring> sstring_serializer;
typedef serializer<atomic_cell_view> atomic_cell_view_serializer;
typedef serializer<collection_mutation::view> collection_mutation_view_serializer;
typedef serializer<utils::UUID> uuid_serializer;
typedef serializer<partition_key_view> partition_key_view_serializer;
typedef serializer<clustering_key_view> clustering_key_view_serializer;
typedef serializer<clustering_key_prefix_view> clustering_key_prefix_view_serializer;
typedef serializer<frozen_mutation> frozen_mutation_serializer;

}

#endif /* DB_SERIALIZER_HH_ */
