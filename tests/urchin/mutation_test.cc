/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK

#include <random>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>

#include "core/sstring.hh"
#include "core/do_with.hh"
#include "core/thread.hh"

#include "database.hh"
#include "utils/UUID_gen.hh"
#include "mutation_reader.hh"
#include "schema_builder.hh"
#include "query-result-set.hh"
#include "query-result-reader.hh"
#include "partition_slice_builder.hh"

#include "tests/test-utils.hh"
#include "tests/urchin/mutation_assertions.hh"
#include "tests/urchin/mutation_reader_assertions.hh"
#include "tests/urchin/result_set_assertions.hh"

static sstring some_keyspace("ks");
static sstring some_column_family("cf");

static atomic_cell make_atomic_cell(bytes value) {
    return atomic_cell::make_live(0, std::move(value));
};

SEASTAR_TEST_CASE(test_mutation_is_applied) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

    memtable mt(s, make_control_group());

    const column_definition& r1_col = *s->get_column_definition("r1");
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(2)});

    mutation m(key, s);
    m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(3)));
    mt.apply(std::move(m));

    row& r = mt.find_or_create_row_slow(key, c_key);
    auto i = r.find_cell(r1_col.id);
    BOOST_REQUIRE(i);
    auto cell = i->as_atomic_cell();
    BOOST_REQUIRE(cell.is_live());
    BOOST_REQUIRE(int32_type->equal(cell.value(), int32_type->decompose(3)));
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_multi_level_row_tombstones) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
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

    m.partition().apply_row_tombstone(*s, make_prefix({1, 2}), tombstone(9, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 3})), tombstone(9, ttl));

    m.partition().apply_row_tombstone(*s, make_prefix({1, 3}), tombstone(8, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 0})), tombstone(9, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 3, 0})), tombstone(8, ttl));

    m.partition().apply_row_tombstone(*s, make_prefix({1}), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 0})), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 3, 0})), tombstone(11, ttl));

    m.partition().apply_row_tombstone(*s, make_prefix({1, 4}), tombstone(6, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 0})), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 3, 0})), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 4, 0})), tombstone(11, ttl));
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_row_tombstone_updates) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}, {"c2", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

    memtable mt(s, make_control_group());

    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto c_key1 = clustering_key::from_deeply_exploded(*s, {1, 0});
    auto c_key1_prefix = clustering_key_prefix::from_deeply_exploded(*s, {1});
    auto c_key2 = clustering_key::from_deeply_exploded(*s, {2, 0});
    auto c_key2_prefix = clustering_key_prefix::from_deeply_exploded(*s, {2});

    auto ttl = gc_clock::now() + std::chrono::seconds(1);

    mutation m(key, s);
    m.partition().apply_row_tombstone(*s, c_key1_prefix, tombstone(1, ttl));
    m.partition().apply_row_tombstone(*s, c_key2_prefix, tombstone(0, ttl));

    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, c_key1), tombstone(1, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, c_key2), tombstone(0, ttl));

    m.partition().apply_row_tombstone(*s, c_key2_prefix, tombstone(1, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, c_key2), tombstone(1, ttl));
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_map_mutations) {
    auto my_map_type = map_type_impl::get_instance(int32_type, utf8_type, true);
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", my_map_type}}, utf8_type));
    memtable mt(s, make_control_group());
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto& column = *s->get_column_definition("s1");
    map_type_impl::mutation mmut1{{}, {{int32_type->decompose(101), make_atomic_cell(utf8_type->decompose(sstring("101")))}}};
    mutation m1(key, s);
    m1.set_static_cell(column, my_map_type->serialize_mutation_form(mmut1));
    mt.apply(m1);
    map_type_impl::mutation mmut2{{}, {{int32_type->decompose(102), make_atomic_cell(utf8_type->decompose(sstring("102")))}}};
    mutation m2(key, s);
    m2.set_static_cell(column, my_map_type->serialize_mutation_form(mmut2));
    mt.apply(m2);
    map_type_impl::mutation mmut3{{}, {{int32_type->decompose(103), make_atomic_cell(utf8_type->decompose(sstring("103")))}}};
    mutation m3(key, s);
    m3.set_static_cell(column, my_map_type->serialize_mutation_form(mmut3));
    mt.apply(m3);
    map_type_impl::mutation mmut2o{{}, {{int32_type->decompose(102), make_atomic_cell(utf8_type->decompose(sstring("102 override")))}}};
    mutation m2o(key, s);
    m2o.set_static_cell(column, my_map_type->serialize_mutation_form(mmut2o));
    mt.apply(m2o);

    row& r = mt.find_or_create_partition_slow(key).static_row();
    auto i = r.find_cell(column.id);
    BOOST_REQUIRE(i);
    auto cell = i->as_collection_mutation();
    auto muts = my_map_type->deserialize_mutation_form(cell);
    BOOST_REQUIRE(muts.cells.size() == 3);
    // FIXME: more strict tests
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_set_mutations) {
    auto my_set_type = set_type_impl::get_instance(int32_type, true);
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", my_set_type}}, utf8_type));
    memtable mt(s, make_control_group());
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto& column = *s->get_column_definition("s1");
    map_type_impl::mutation mmut1{{}, {{int32_type->decompose(101), make_atomic_cell({})}}};
    mutation m1(key, s);
    m1.set_static_cell(column, my_set_type->serialize_mutation_form(mmut1));
    mt.apply(m1);
    map_type_impl::mutation mmut2{{}, {{int32_type->decompose(102), make_atomic_cell({})}}};
    mutation m2(key, s);
    m2.set_static_cell(column, my_set_type->serialize_mutation_form(mmut2));
    mt.apply(m2);
    map_type_impl::mutation mmut3{{}, {{int32_type->decompose(103), make_atomic_cell({})}}};
    mutation m3(key, s);
    m3.set_static_cell(column, my_set_type->serialize_mutation_form(mmut3));
    mt.apply(m3);
    map_type_impl::mutation mmut2o{{}, {{int32_type->decompose(102), make_atomic_cell({})}}};
    mutation m2o(key, s);
    m2o.set_static_cell(column, my_set_type->serialize_mutation_form(mmut2o));
    mt.apply(m2o);

    row& r = mt.find_or_create_partition_slow(key).static_row();
    auto i = r.find_cell(column.id);
    BOOST_REQUIRE(i);
    auto cell = i->as_collection_mutation();
    auto muts = my_set_type->deserialize_mutation_form(cell);
    BOOST_REQUIRE(muts.cells.size() == 3);
    // FIXME: more strict tests
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_list_mutations) {
    auto my_list_type = list_type_impl::get_instance(int32_type, true);
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", my_list_type}}, utf8_type));
    memtable mt(s, make_control_group());
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto& column = *s->get_column_definition("s1");
    auto make_key = [] { return timeuuid_type->decompose(utils::UUID_gen::get_time_UUID()); };
    collection_type_impl::mutation mmut1{{}, {{make_key(), make_atomic_cell(int32_type->decompose(101))}}};
    mutation m1(key, s);
    m1.set_static_cell(column, my_list_type->serialize_mutation_form(mmut1));
    mt.apply(m1);
    collection_type_impl::mutation mmut2{{}, {{make_key(), make_atomic_cell(int32_type->decompose(102))}}};
    mutation m2(key, s);
    m2.set_static_cell(column, my_list_type->serialize_mutation_form(mmut2));
    mt.apply(m2);
    collection_type_impl::mutation mmut3{{}, {{make_key(), make_atomic_cell(int32_type->decompose(103))}}};
    mutation m3(key, s);
    m3.set_static_cell(column, my_list_type->serialize_mutation_form(mmut3));
    mt.apply(m3);
    collection_type_impl::mutation mmut2o{{}, {{make_key(), make_atomic_cell(int32_type->decompose(102))}}};
    mutation m2o(key, s);
    m2o.set_static_cell(column, my_list_type->serialize_mutation_form(mmut2o));
    mt.apply(m2o);

    row& r = mt.find_or_create_partition_slow(key).static_row();
    auto i = r.find_cell(column.id);
    BOOST_REQUIRE(i);
    auto cell = i->as_collection_mutation();
    auto muts = my_list_type->deserialize_mutation_form(cell);
    BOOST_REQUIRE(muts.cells.size() == 4);
    // FIXME: more strict tests
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_multiple_memtables_one_partition) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

    column_family::config cfg;
    cfg.enable_disk_reads = false;
    cfg.enable_disk_writes = false;
    auto cf = make_lw_shared<column_family>(s, cfg, column_family::no_commitlog());

    const column_definition& r1_col = *s->get_column_definition("r1");
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});

    auto insert_row = [&] (int32_t c1, int32_t r1) {
        auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(c1)});
        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(r1)));
        cf->apply(std::move(m));
        cf->flush();
    };
    insert_row(1001, 2001);
    insert_row(1002, 2002);
    insert_row(1003, 2003);

    auto verify_row = [&] (int32_t c1, int32_t r1) {
      auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(c1)});
      return cf->find_row(dht::global_partitioner().decorate_key(*s, key), std::move(c_key)).then([r1, r1_col] (auto r) {
        BOOST_REQUIRE(r);
        auto i = r->find_cell(r1_col.id);
        BOOST_REQUIRE(i);
        auto cell = i->as_atomic_cell();
        BOOST_REQUIRE(cell.is_live());
        BOOST_REQUIRE(int32_type->equal(cell.value(), int32_type->decompose(r1)));
      });
    };
    verify_row(1001, 2001);
    verify_row(1002, 2002);
    verify_row(1003, 2003);
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_multiple_memtables_multiple_partitions) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", int32_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

    column_family::config cfg;
    cfg.enable_disk_reads = false;
    cfg.enable_disk_writes = false;
    return do_with(make_lw_shared<column_family>(s, cfg, column_family::no_commitlog()), [s] (auto& cf_ptr) mutable {
        column_family& cf = *cf_ptr;
        std::map<int32_t, std::map<int32_t, int32_t>> shadow, result;

        const column_definition& r1_col = *s->get_column_definition("r1");

        api::timestamp_type ts = 0;
        auto insert_row = [&] (int32_t p1, int32_t c1, int32_t r1) {
            auto key = partition_key::from_exploded(*s, {int32_type->decompose(p1)});
            auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(c1)});
            mutation m(key, s);
            m.set_clustered_cell(c_key, r1_col, atomic_cell::make_live(ts++, int32_type->decompose(r1)));
            cf.apply(std::move(m));
            shadow[p1][c1] = r1;
        };
        std::minstd_rand random_engine;
        std::normal_distribution<> pk_distribution(0, 10);
        std::normal_distribution<> ck_distribution(0, 5);
        std::normal_distribution<> r_distribution(0, 100);
        for (unsigned i = 0; i < 10; ++i) {
            for (unsigned j = 0; j < 100; ++j) {
                insert_row(pk_distribution(random_engine), ck_distribution(random_engine), r_distribution(random_engine));
            }
            cf.flush();
        }

        return do_with(std::move(result), [&cf, s, &r1_col, shadow] (auto& result) {
            return cf.for_all_partitions_slow([&, s] (const dht::decorated_key& pk, const mutation_partition& mp) {
                auto p1 = boost::any_cast<int32_t>(int32_type->deserialize(pk._key.explode(*s)[0]));
                for (const rows_entry& re : mp.range(*s, query::range<clustering_key_prefix>())) {
                    auto c1 = boost::any_cast<int32_t>(int32_type->deserialize(re.key().explode(*s)[0]));
                    auto cell = re.row().cells().find_cell(r1_col.id);
                    if (cell) {
                        result[p1][c1] = boost::any_cast<int32_t>(int32_type->deserialize(cell->as_atomic_cell().value()));
                    }
                }
                return true;
            }).then([&result, shadow] (bool ok) {
                BOOST_REQUIRE(shadow == result);
            });
        });
    });
}

SEASTAR_TEST_CASE(test_cell_ordering) {
    auto now = gc_clock::now();
    auto ttl_1 = gc_clock::duration(1);
    auto ttl_2 = gc_clock::duration(2);
    auto expiry_1 = now + ttl_1;
    auto expiry_2 = now + ttl_2;

    auto assert_order = [] (atomic_cell_view first, atomic_cell_view second) {
        if (compare_atomic_cell_for_merge(first, second) >= 0) {
            BOOST_FAIL(sprint("Expected %s < %s", first, second));
        }
        if (compare_atomic_cell_for_merge(second, first) <= 0) {
            BOOST_FAIL(sprint("Expected %s < %s", second, first));
        }
    };

    auto assert_equal = [] (atomic_cell_view c1, atomic_cell_view c2) {
        BOOST_REQUIRE(compare_atomic_cell_for_merge(c1, c2) == 0);
        BOOST_REQUIRE(compare_atomic_cell_for_merge(c2, c1) == 0);
    };

    assert_equal(
        atomic_cell::make_live(0, bytes("value")),
        atomic_cell::make_live(0, bytes("value")));

    assert_equal(
        atomic_cell::make_live(1, bytes("value"), expiry_1, ttl_1),
        atomic_cell::make_live(1, bytes("value")));

    assert_equal(
        atomic_cell::make_dead(1, expiry_1),
        atomic_cell::make_dead(1, expiry_1));

    // If one cell doesn't have an expiry, Origin considers them equal.
    assert_equal(
        atomic_cell::make_live(1, bytes(), expiry_2, ttl_2),
        atomic_cell::make_live(1, bytes()));

    // Origin doesn't compare ttl (is it wise?)
    assert_equal(
        atomic_cell::make_live(1, bytes("value"), expiry_1, ttl_1),
        atomic_cell::make_live(1, bytes("value"), expiry_1, ttl_2));

    assert_order(
        atomic_cell::make_live(0, bytes("value1")),
        atomic_cell::make_live(0, bytes("value2")));

    assert_order(
        atomic_cell::make_live(0, bytes("value12")),
        atomic_cell::make_live(0, bytes("value2")));

    // Live cells are ordered first by timestamp...
    assert_order(
        atomic_cell::make_live(0, bytes("value2")),
        atomic_cell::make_live(1, bytes("value1")));

    // ..then by value
    assert_order(
        atomic_cell::make_live(1, bytes("value1"), expiry_2, ttl_2),
        atomic_cell::make_live(1, bytes("value2"), expiry_1, ttl_1));

    // ..then by expiry
    assert_order(
        atomic_cell::make_live(1, bytes(), expiry_1, ttl_1),
        atomic_cell::make_live(1, bytes(), expiry_2, ttl_1));

    // Dead wins
    assert_order(
        atomic_cell::make_live(1, bytes("value")),
        atomic_cell::make_dead(1, expiry_1));

    // Dead wins with expiring cell
    assert_order(
        atomic_cell::make_live(1, bytes("value"), expiry_2, ttl_2),
        atomic_cell::make_dead(1, expiry_1));

    // Deleted cells are ordered first by timestamp
    assert_order(
        atomic_cell::make_dead(1, expiry_2),
        atomic_cell::make_dead(2, expiry_1));

    // ...then by expiry
    assert_order(
        atomic_cell::make_dead(1, expiry_1),
        atomic_cell::make_dead(1, expiry_2));
    return make_ready_future<>();
}

static query::partition_slice make_full_slice(const schema& s) {
    return partition_slice_builder(s).build();
}

SEASTAR_TEST_CASE(test_querying_of_mutation) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("v", bytes_type, column_kind::regular_column)
            .build();

        auto resultify = [s] (const mutation& m) -> query::result_set {
            auto slice = make_full_slice(*s);
            return query::result_set::from_raw_result(s, slice, m.query(slice));
        };

        mutation m(partition_key::from_single_value(*s, "key1"), s);
        m.set_clustered_cell(clustering_key::make_empty(*s), "v", bytes("v1"), 1);

        assert_that(resultify(m))
            .has_only(a_row()
                .with_column("pk", bytes("key1"))
                .with_column("v", bytes("v1")));

        m.partition().apply(tombstone(2, gc_clock::now()));

        assert_that(resultify(m)).is_empty();
    });
}

SEASTAR_TEST_CASE(test_partition_with_no_live_data_is_absent_in_data_query_results) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("sc1", bytes_type, column_kind::static_column)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("v", bytes_type, column_kind::regular_column)
            .build();

        mutation m(partition_key::from_single_value(*s, "key1"), s);
        m.partition().apply(tombstone(1, gc_clock::now()));
        m.partition().static_row().apply(*s->get_column_definition("sc1"),
            atomic_cell::make_dead(2, gc_clock::now()));
        m.set_clustered_cell(clustering_key::from_single_value(*s, bytes_type->decompose(bytes("A"))),
            *s->get_column_definition("v"), atomic_cell::make_dead(2, gc_clock::now()));

        auto slice = make_full_slice(*s);

        assert_that(query::result_set::from_raw_result(s, slice, m.query(slice)))
            .is_empty();
    });
}

SEASTAR_TEST_CASE(test_partition_with_live_data_in_static_row_is_present_in_the_results_even_if_static_row_was_not_queried) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("sc1", bytes_type, column_kind::static_column)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("v", bytes_type, column_kind::regular_column)
            .build();

        mutation m(partition_key::from_single_value(*s, "key1"), s);
        m.partition().static_row().apply(*s->get_column_definition("sc1"),
            atomic_cell::make_live(2, bytes_type->decompose(bytes("sc1:value"))));

        auto slice = partition_slice_builder(*s)
            .with_no_static_columns()
            .with_regular_column("v")
            .build();

        assert_that(query::result_set::from_raw_result(s, slice, m.query(slice)))
            .has_only(a_row()
                .with_column("pk", bytes("key1"))
                .with_column("v", {}));
    });
}

SEASTAR_TEST_CASE(test_query_result_with_one_regular_column_missing) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("v1", bytes_type, column_kind::regular_column)
            .with_column("v2", bytes_type, column_kind::regular_column)
            .build();

        mutation m(partition_key::from_single_value(*s, "key1"), s);
        m.set_clustered_cell(clustering_key::from_single_value(*s, bytes("ck:A")),
            *s->get_column_definition("v1"),
            atomic_cell::make_live(2, bytes_type->decompose(bytes("v1:value"))));

        auto slice = partition_slice_builder(*s).build();

        assert_that(query::result_set::from_raw_result(s, slice, m.query(slice)))
            .has_only(a_row()
                .with_column("pk", bytes("key1"))
                .with_column("ck", bytes("ck:A"))
                .with_column("v1", bytes("v1:value"))
                .with_column("v2", {}));
    });
}

SEASTAR_TEST_CASE(test_row_counting) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("sc1", bytes_type, column_kind::static_column)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("v", bytes_type, column_kind::regular_column)
            .build();

        auto col_v = *s->get_column_definition("v");

        mutation m(partition_key::from_single_value(*s, "key1"), s);

        BOOST_REQUIRE_EQUAL(0, m.live_row_count());

        auto ckey1 = clustering_key::from_single_value(*s, bytes_type->decompose(bytes("A")));
        auto ckey2 = clustering_key::from_single_value(*s, bytes_type->decompose(bytes("B")));

        m.set_clustered_cell(ckey1, col_v, atomic_cell::make_live(2, bytes_type->decompose(bytes("v:value"))));

        BOOST_REQUIRE_EQUAL(1, m.live_row_count());

        m.partition().static_row().apply(*s->get_column_definition("sc1"),
            atomic_cell::make_live(2, bytes_type->decompose(bytes("sc1:value"))));

        BOOST_REQUIRE_EQUAL(1, m.live_row_count());

        m.set_clustered_cell(ckey1, col_v, atomic_cell::make_dead(2, gc_clock::now()));

        BOOST_REQUIRE_EQUAL(1, m.live_row_count());

        m.partition().static_row().apply(*s->get_column_definition("sc1"),
            atomic_cell::make_dead(2, gc_clock::now()));

        BOOST_REQUIRE_EQUAL(0, m.live_row_count());

        m.partition().clustered_row(*s, ckey1).apply(api::timestamp_type(3));

        BOOST_REQUIRE_EQUAL(1, m.live_row_count());

        m.partition().apply(tombstone(3, gc_clock::now()));

        BOOST_REQUIRE_EQUAL(0, m.live_row_count());

        m.set_clustered_cell(ckey1, col_v, atomic_cell::make_live(4, bytes_type->decompose(bytes("v:value"))));
        m.set_clustered_cell(ckey2, col_v, atomic_cell::make_live(4, bytes_type->decompose(bytes("v:value"))));

        BOOST_REQUIRE_EQUAL(2, m.live_row_count());
    });
}
