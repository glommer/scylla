/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/included/unit_test.hpp>
#include "core/sstring.hh"
#include "database.hh"
#include "utils/UUID_gen.hh"

static sstring some_keyspace("ks");
static sstring some_column_family("cf");

static atomic_cell make_atomic_cell(bytes value) {
    return atomic_cell::make_live(0, ttl_opt{}, std::move(value));
};

BOOST_AUTO_TEST_CASE(test_mutation_is_applied) {
    auto s = make_lw_shared(schema(some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

    column_family cf(s);

    column_definition& r1_col = *s->get_column_definition("r1");
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(2)});

    mutation m(key, s);
    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(3)));
    cf.apply(std::move(m));

    row& r = cf.find_or_create_row(key, c_key);
    auto i = r.find(r1_col.id);
    BOOST_REQUIRE(i != r.end());
    auto cell = i->second.as_atomic_cell();
    BOOST_REQUIRE(cell.is_live());
    BOOST_REQUIRE(int32_type->equal(cell.value(), int32_type->decompose(3)));
}

BOOST_AUTO_TEST_CASE(test_multi_level_row_tombstones) {
    auto s = make_lw_shared(schema(some_keyspace, some_column_family,
        {{"p1", utf8_type}},
        {{"c1", int32_type}, {"c2", int32_type}, {"c3", int32_type}},
        {{"r1", int32_type}}, {}, utf8_type));

    auto ttl = gc_clock::now() + std::chrono::seconds(1);

    mutation m(partition_key::from_exploded(*s, {to_bytes("key1")}), s);

    auto make_prefix = [s] (const std::vector<boost::any>& v) {
        return clustering_key_prefix::from_deeply_exploded(*s, v);
    };
    auto make_key = [s] (const std::vector<boost::any>& v) {
        return clustering_key::from_deeply_exploded(*s, v);
    };

    m.p.apply_row_tombstone(s, make_prefix({1, 2}), tombstone(9, ttl));
    BOOST_REQUIRE_EQUAL(m.p.tombstone_for_row(s, make_key({1, 2, 3})), tombstone(9, ttl));

    m.p.apply_row_tombstone(s, make_prefix({1, 3}), tombstone(8, ttl));
    BOOST_REQUIRE_EQUAL(m.p.tombstone_for_row(s, make_key({1, 2, 0})), tombstone(9, ttl));
    BOOST_REQUIRE_EQUAL(m.p.tombstone_for_row(s, make_key({1, 3, 0})), tombstone(8, ttl));

    m.p.apply_row_tombstone(s, make_prefix({1}), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.p.tombstone_for_row(s, make_key({1, 2, 0})), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.p.tombstone_for_row(s, make_key({1, 3, 0})), tombstone(11, ttl));

    m.p.apply_row_tombstone(s, make_prefix({1, 4}), tombstone(6, ttl));
    BOOST_REQUIRE_EQUAL(m.p.tombstone_for_row(s, make_key({1, 2, 0})), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.p.tombstone_for_row(s, make_key({1, 3, 0})), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.p.tombstone_for_row(s, make_key({1, 4, 0})), tombstone(11, ttl));
}

BOOST_AUTO_TEST_CASE(test_row_tombstone_updates) {
    auto s = make_lw_shared(schema(some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}, {"c2", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

    column_family cf(s);

    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto c_key1 = clustering_key::from_deeply_exploded(*s, {1, 0});
    auto c_key1_prefix = clustering_key_prefix::from_deeply_exploded(*s, {1});
    auto c_key2 = clustering_key::from_deeply_exploded(*s, {2, 0});
    auto c_key2_prefix = clustering_key_prefix::from_deeply_exploded(*s, {2});

    auto ttl = gc_clock::now() + std::chrono::seconds(1);

    mutation m(key, s);
    m.p.apply_row_tombstone(s, c_key1_prefix, tombstone(1, ttl));
    m.p.apply_row_tombstone(s, c_key2_prefix, tombstone(0, ttl));

    BOOST_REQUIRE_EQUAL(m.p.tombstone_for_row(s, c_key1), tombstone(1, ttl));
    BOOST_REQUIRE_EQUAL(m.p.tombstone_for_row(s, c_key2), tombstone(0, ttl));

    m.p.apply_row_tombstone(s, c_key2_prefix, tombstone(1, ttl));
    BOOST_REQUIRE_EQUAL(m.p.tombstone_for_row(s, c_key2), tombstone(1, ttl));
}

BOOST_AUTO_TEST_CASE(test_map_mutations) {
    auto my_map_type = map_type_impl::get_instance(int32_type, utf8_type, true);
    auto s = make_lw_shared(schema(some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", my_map_type}}, utf8_type));
    column_family cf(s);
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto& column = *s->get_column_definition("s1");
    map_type_impl::mutation mmut1{{{int32_type->decompose(101), make_atomic_cell(utf8_type->decompose(sstring("101")))}}};
    mutation m1(key, s);
    m1.set_static_cell(column, my_map_type->serialize_mutation_form(mmut1));
    cf.apply(m1);
    map_type_impl::mutation mmut2{{{int32_type->decompose(102), make_atomic_cell(utf8_type->decompose(sstring("102")))}}};
    mutation m2(key, s);
    m2.set_static_cell(column, my_map_type->serialize_mutation_form(mmut2));
    cf.apply(m2);
    map_type_impl::mutation mmut3{{{int32_type->decompose(103), make_atomic_cell(utf8_type->decompose(sstring("103")))}}};
    mutation m3(key, s);
    m3.set_static_cell(column, my_map_type->serialize_mutation_form(mmut3));
    cf.apply(m3);
    map_type_impl::mutation mmut2o{{{int32_type->decompose(102), make_atomic_cell(utf8_type->decompose(sstring("102 override")))}}};
    mutation m2o(key, s);
    m2o.set_static_cell(column, my_map_type->serialize_mutation_form(mmut2o));
    cf.apply(m2o);

    row& r = cf.find_or_create_partition(key).static_row();
    auto i = r.find(column.id);
    BOOST_REQUIRE(i != r.end());
    auto cell = i->second.as_collection_mutation();
    auto muts = my_map_type->deserialize_mutation_form(cell.data);
    BOOST_REQUIRE(muts.cells.size() == 3);
    // FIXME: more strict tests
}

BOOST_AUTO_TEST_CASE(test_set_mutations) {
    auto my_set_type = set_type_impl::get_instance(int32_type, true);
    auto s = make_lw_shared(schema(some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", my_set_type}}, utf8_type));
    column_family cf(s);
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto& column = *s->get_column_definition("s1");
    map_type_impl::mutation mmut1{{{int32_type->decompose(101), make_atomic_cell({})}}};
    mutation m1(key, s);
    m1.set_static_cell(column, my_set_type->serialize_mutation_form(mmut1));
    cf.apply(m1);
    map_type_impl::mutation mmut2{{{int32_type->decompose(102), make_atomic_cell({})}}};
    mutation m2(key, s);
    m2.set_static_cell(column, my_set_type->serialize_mutation_form(mmut2));
    cf.apply(m2);
    map_type_impl::mutation mmut3{{{int32_type->decompose(103), make_atomic_cell({})}}};
    mutation m3(key, s);
    m3.set_static_cell(column, my_set_type->serialize_mutation_form(mmut3));
    cf.apply(m3);
    map_type_impl::mutation mmut2o{{{int32_type->decompose(102), make_atomic_cell({})}}};
    mutation m2o(key, s);
    m2o.set_static_cell(column, my_set_type->serialize_mutation_form(mmut2o));
    cf.apply(m2o);

    row& r = cf.find_or_create_partition(key).static_row();
    auto i = r.find(column.id);
    BOOST_REQUIRE(i != r.end());
    auto cell = i->second.as_collection_mutation();
    auto muts = my_set_type->deserialize_mutation_form(cell.data);
    BOOST_REQUIRE(muts.cells.size() == 3);
    // FIXME: more strict tests
}

BOOST_AUTO_TEST_CASE(test_list_mutations) {
    auto my_list_type = list_type_impl::get_instance(int32_type, true);
    auto s = make_lw_shared(schema(some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", my_list_type}}, utf8_type));
    column_family cf(s);
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto& column = *s->get_column_definition("s1");
    auto make_key = [] { return timeuuid_type->decompose(utils::UUID_gen::get_time_UUID()); };
    collection_type_impl::mutation mmut1{{{make_key(), make_atomic_cell(int32_type->decompose(101))}}};
    mutation m1(key, s);
    m1.set_static_cell(column, my_list_type->serialize_mutation_form(mmut1));
    cf.apply(m1);
    collection_type_impl::mutation mmut2{{{make_key(), make_atomic_cell(int32_type->decompose(102))}}};
    mutation m2(key, s);
    m2.set_static_cell(column, my_list_type->serialize_mutation_form(mmut2));
    cf.apply(m2);
    collection_type_impl::mutation mmut3{{{make_key(), make_atomic_cell(int32_type->decompose(103))}}};
    mutation m3(key, s);
    m3.set_static_cell(column, my_list_type->serialize_mutation_form(mmut3));
    cf.apply(m3);
    collection_type_impl::mutation mmut2o{{{make_key(), make_atomic_cell(int32_type->decompose(102))}}};
    mutation m2o(key, s);
    m2o.set_static_cell(column, my_list_type->serialize_mutation_form(mmut2o));
    cf.apply(m2o);

    row& r = cf.find_or_create_partition(key).static_row();
    auto i = r.find(column.id);
    BOOST_REQUIRE(i != r.end());
    auto cell = i->second.as_collection_mutation();
    auto muts = my_list_type->deserialize_mutation_form(cell.data);
    BOOST_REQUIRE(muts.cells.size() == 4);
    // FIXME: more strict tests
}

