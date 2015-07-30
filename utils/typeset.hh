/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <stdint.h>
#include <limits>
#include <type_traits>
#include <array>
#include <assert.h>

//
// Virtual dispatch helper for a closed set of types.
//
template<typename... Types>
class typeset {
public:
    static constexpr auto cardinality = sizeof...(Types);

    using id_type = uint8_t; // FIXME: make dynamic
    static_assert(cardinality <= std::numeric_limits<id_type>::max() + 1, "Too many types");

    template <typename T, typename... Tail>
    struct first {
        using type = T;
    };

    template <typename... Type>
    using first_t = typename first<Type...>::type;
private:
    template<typename T, typename Func, typename RetType>
    static RetType invoker_for_type(Func& func, void* obj) {
        return func(reinterpret_cast<T*>(obj));
    }

    template <typename T, typename... Tail>
    struct index_of;

    template <typename T, typename... Tail>
    struct index_of<T, T, Tail...> : std::integral_constant<id_type, 0> {};

    template <typename T, typename U, typename... Tail>
    struct index_of<T, U, Tail...> : std::integral_constant<id_type, 1 + index_of<T, Tail...>::value> {};
public:
    template<typename T>
    static constexpr id_type id() {
        return index_of<T, Types...>::value;
    }

    template<typename Func, typename RetType, id_type pos, typename Head>
    static constexpr RetType search(id_type id, Func& func, void* obj) {
        return invoker_for_type<Head, Func, RetType>(func, obj);
    };

    template<typename Func, typename RetType, id_type pos, typename H1, typename H2, typename... Tail>
    static constexpr RetType search(id_type id, Func& func, void* obj) {
        return id == pos
               ? invoker_for_type<H1, Func, RetType>(func, obj)
               : search<Func, RetType, pos + 1, H2, Tail...>(id, func, obj);
    };

    // Branch dispatch version
    template<typename Func>
    static
    std::result_of_t<Func(std::add_pointer_t<first_t<Types...>>)>
    dispatch(id_type id, Func&& func, void* obj) {
        using ret_type = std::result_of_t<Func(std::add_pointer_t<first_t<Types...>>)>;
        return search<Func, ret_type, 0, Types...>(id, func, obj);
    }

    // Table dispatch version
    //template<typename Func>
    //static
    //std::result_of_t<Func(std::add_pointer_t<first_t<Types...>>)>
    //dispatch(id_type id, Func&& func, void* obj) {
    //    using ret_type = std::result_of_t<Func(std::add_pointer_t<first_t<Types...>>)>;
    //    using func_type = ret_type(*)(Func&, void*);
    //    static const func_type table[sizeof...(Types)] = {
    //        invoker_for_type<Types, Func, ret_type>...
    //    };
    //    return table[id](func, obj);
    //}
};
