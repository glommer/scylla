/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#include "cql3/statements/select_statement.hh"

#include "transport/messages/result_message.hh"
#include "cql3/selection/selection.hh"
#include "core/shared_ptr.hh"
#include "query-result-reader.hh"
#include "query_result_merger.hh"

namespace cql3 {

namespace statements {

thread_local const shared_ptr<select_statement::parameters> select_statement::_default_parameters = ::make_shared<select_statement::parameters>();

select_statement::select_statement(schema_ptr schema,
    uint32_t bound_terms,
    ::shared_ptr<parameters> parameters,
    ::shared_ptr<selection::selection> selection,
    ::shared_ptr<restrictions::statement_restrictions> restrictions,
    bool is_reversed,
    ordering_comparator_type ordering_comparator,
    ::shared_ptr<term> limit)
        : _schema(schema)
        , _bound_terms(bound_terms)
        , _parameters(std::move(parameters))
        , _selection(std::move(selection))
        , _restrictions(std::move(restrictions))
        , _is_reversed(is_reversed)
        , _limit(std::move(limit))
        , _ordering_comparator(std::move(ordering_comparator))
{
    _opts = _selection->get_query_options();
}

bool select_statement::uses_function(const sstring& ks_name, const sstring& function_name) const {
    return _selection->uses_function(ks_name, function_name)
        || _restrictions->uses_function(ks_name, function_name)
        || (_limit && _limit->uses_function(ks_name, function_name));
}

::shared_ptr<select_statement>
select_statement::for_selection(schema_ptr schema, ::shared_ptr<selection::selection> selection) {
    return ::make_shared<select_statement>(schema,
        0,
        _default_parameters,
        selection,
        ::make_shared<restrictions::statement_restrictions>(schema),
        false,
        ordering_comparator_type{},
        ::shared_ptr<term>{});
}

uint32_t select_statement::get_bound_terms() {
    return _bound_terms;
}

void select_statement::check_access(const service::client_state& state) {
    warn(unimplemented::cause::PERMISSIONS);
#if 0
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.SELECT);
#endif
}

void select_statement::validate(distributed<service::storage_proxy>&, const service::client_state& state) {
    // Nothing to do, all validation has been done by raw_statemet::prepare()
}

query::partition_slice
select_statement::make_partition_slice(const query_options& options) {
    std::vector<column_id> static_columns;
    std::vector<column_id> regular_columns;

    if (_selection->contains_static_columns()) {
        static_columns.reserve(_selection->get_column_count());
    }

    regular_columns.reserve(_selection->get_column_count());

    for (auto&& col : _selection->get_columns()) {
        if (col->is_static()) {
            static_columns.push_back(col->id);
        } else if (col->is_regular()) {
            regular_columns.push_back(col->id);
        }
    }

    if (_parameters->is_distinct()) {
        _opts.set(query::partition_slice::option::distinct);
        return query::partition_slice({ query::clustering_range::make_open_ended_both_sides() },
            std::move(static_columns), {}, _opts);
    }

    auto bounds = _restrictions->get_clustering_bounds(options);
    if (_is_reversed) {
        _opts.set(query::partition_slice::option::reversed);
        std::reverse(bounds.begin(), bounds.end());
    }
    return query::partition_slice(std::move(bounds),
        std::move(static_columns), std::move(regular_columns), _opts);
}

int32_t select_statement::get_limit(const query_options& options) const {
    if (!_limit) {
        return std::numeric_limits<int32_t>::max();
    }

    auto val = _limit->bind_and_get(options);
    if (!val) {
        throw exceptions::invalid_request_exception("Invalid null value of limit");
    }

    try {
        int32_type->validate(*val);
        auto l = boost::any_cast<int32_t>(int32_type->deserialize(*val));
        if (l <= 0) {
            throw exceptions::invalid_request_exception("LIMIT must be strictly positive");
        }
        return l;
    } catch (const marshal_exception& e) {
        throw exceptions::invalid_request_exception("Invalid limit value");
    }
}

bool select_statement::needs_post_query_ordering() const {
    // We need post-query ordering only for queries with IN on the partition key and an ORDER BY.
    return _restrictions->key_is_in_relation() && !_parameters->orderings().empty();
}

future<shared_ptr<transport::messages::result_message>>
select_statement::execute(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    auto cl = options.get_consistency();

    validate_for_read(_schema->ks_name(), cl);

    int32_t limit = get_limit(options);
    auto now = db_clock::now();

    auto command = ::make_lw_shared<query::read_command>(_schema->id(), make_partition_slice(options), limit, to_gc_clock(now));

    int32_t page_size = options.get_page_size();

    // An aggregation query will never be paged for the user, but we always page it internally to avoid OOM.
    // If we user provided a page_size we'll use that to page internally (because why not), otherwise we use our default
    // Note that if there are some nodes in the cluster with a version less than 2.0, we can't use paging (CASSANDRA-6707).
    if (_selection->is_aggregate() && page_size <= 0) {
        page_size = DEFAULT_COUNT_PAGE_SIZE;
    }

    warn(unimplemented::cause::PAGING);
    return execute(proxy, command, _restrictions->get_partition_key_ranges(options), state, options, now);

#if 0
    if (page_size <= 0 || !command || !query_pagers::may_need_paging(command, page_size)) {
        return execute(proxy, command, state, options, now);
    }

    auto pager = query_pagers::pager(command, cl, state.get_client_state(), options.get_paging_state());

    if (selection->isAggregate()) {
        return page_aggregate_query(pager, options, page_size, now);
    }

    // We can't properly do post-query ordering if we page (see #6722)
    if (needs_post_query_ordering()) {
        throw exceptions::invalid_request_exception(
              "Cannot page queries with both ORDER BY and a IN restriction on the partition key;"
              " you must either remove the ORDER BY or the IN and sort client side, or disable paging for this query");
    }

    return pager->fetch_page(page_size).then([this, pager, &options, limit, now] (auto page) {
        auto msg = process_results(page, options, limit, now);

        if (!pager->is_exhausted()) {
            msg->result->metadata->set_has_more_pages(pager->state());
        }

        return msg;
    });
#endif
}

future<shared_ptr<transport::messages::result_message>>
select_statement::execute(distributed<service::storage_proxy>& proxy, lw_shared_ptr<query::read_command> cmd, std::vector<query::partition_range>&& partition_ranges,
        service::query_state& state, const query_options& options, db_clock::time_point now) {

    // If this is a query with IN on partition key, ORDER BY clause and LIMIT
    // is specified we need to get "limit" rows from each partition since there
    // is no way to tell which of these rows belong to the query result before
    // doing post-query ordering.
    if (needs_post_query_ordering() && _limit) {
        return do_with(std::forward<std::vector<query::partition_range>>(partition_ranges), [this, &proxy, &state, &options, cmd](auto prs) {
            query::result_merger merger;
            return map_reduce(prs.begin(), prs.end(), [this, &proxy, &state, &options, cmd] (auto pr) {
                std::vector<query::partition_range> prange { pr };
                auto command = ::make_lw_shared<query::read_command>(*cmd);
                return proxy.local().query(_schema, command, std::move(prange), options.get_consistency());
            }, std::move(merger));
        }).then([this, &options, now, cmd] (auto result) {
            return this->process_results(std::move(result), cmd, options, now);
        });
    } else {
        return proxy.local().query(_schema, cmd, std::move(partition_ranges), options.get_consistency())
            .then([this, &options, now, cmd] (auto result) {
                return this->process_results(std::move(result), cmd, options, now);
            });
    }
}


future<::shared_ptr<transport::messages::result_message>>
select_statement::execute_internal(distributed<service::storage_proxy>& proxy, service::query_state& state, const query_options& options) {
    int32_t limit = get_limit(options);
    auto now = db_clock::now();
    auto command = ::make_lw_shared<query::read_command>(_schema->id(), make_partition_slice(options), limit);
    auto partition_ranges = _restrictions->get_partition_key_ranges(options);

    if (needs_post_query_ordering() && _limit) {
        return do_with(std::move(partition_ranges), [this, &proxy, &state, command] (auto prs) {
            query::result_merger merger;
            return map_reduce(prs.begin(), prs.end(), [this, &proxy, &state, command] (auto pr) {
                std::vector<query::partition_range> prange { pr };
                auto cmd = ::make_lw_shared<query::read_command>(*command);
                return proxy.local().query(_schema, cmd, std::move(prange), db::consistency_level::ONE);
            }, std::move(merger));
        }).then([command, this, &options, now] (auto result) {
            return this->process_results(std::move(result), command, options, now);
        }).finally([command] { });
    } else {
        return proxy.local().query(_schema, command, std::move(partition_ranges), db::consistency_level::ONE).then([command, this, &options, now] (auto result) {
            return this->process_results(std::move(result), command, options, now);
        }).finally([command] {});
    }
}

// Implements ResultVisitor concept from query.hh
class result_set_building_visitor {
    cql3::selection::result_set_builder& builder;
    select_statement& stmt;
    uint32_t _row_count;
    std::vector<bytes> _partition_key;
    std::vector<bytes> _clustering_key;
public:
    result_set_building_visitor(cql3::selection::result_set_builder& builder, select_statement& stmt)
        : builder(builder)
        , stmt(stmt)
        , _row_count(0)
    { }

    void add_value(const column_definition& def, query::result_row_view::iterator_type& i) {
        if (def.type->is_multi_cell()) {
            auto cell = i.next_collection_cell();
            if (!cell) {
                builder.add_empty();
                return;
            }
            builder.add(def, *cell);
        } else {
            auto cell = i.next_atomic_cell();
            if (!cell) {
                builder.add_empty();
                return;
            }
            builder.add(def, *cell);
        }
    };

    void accept_new_partition(const partition_key& key, uint32_t row_count) {
        _partition_key = key.explode(*stmt._schema);
        _row_count = row_count;
    }

    void accept_new_partition(uint32_t row_count) {
        _row_count = row_count;
    }

    void accept_new_row(const clustering_key& key, const query::result_row_view& static_row,
            const query::result_row_view& row) {
        _clustering_key = key.explode(*stmt._schema);
        accept_new_row(static_row, row);
    }

    void accept_new_row(const query::result_row_view& static_row, const query::result_row_view& row) {
        auto static_row_iterator = static_row.iterator();
        auto row_iterator = row.iterator();
        builder.new_row();
        for (auto&& def : stmt._selection->get_columns()) {
            switch (def->kind) {
                case column_kind::partition_key:
                    builder.add(_partition_key[def->component_index()]);
                    break;
                case column_kind::clustering_key:
                    builder.add(_clustering_key[def->component_index()]);
                    break;
                case column_kind::regular_column:
                    add_value(*def, row_iterator);
                    break;
                case column_kind::compact_column:
                    add_value(*def, row_iterator);
                    break;
                case column_kind::static_column:
                    add_value(*def, static_row_iterator);
                    break;
                default:
                    assert(0);
            }
        }
    }

    void accept_partition_end(const query::result_row_view& static_row) {
        if (_row_count == 0) {
            builder.new_row();
            auto static_row_iterator = static_row.iterator();
            for (auto&& def : stmt._selection->get_columns()) {
                if (def->is_partition_key()) {
                    builder.add(_partition_key[def->component_index()]);
                } else if (def->is_static()) {
                    add_value(*def, static_row_iterator);
                } else {
                    builder.add_empty();
                }
            }
        }
    }
};

shared_ptr<transport::messages::result_message>
select_statement::process_results(foreign_ptr<lw_shared_ptr<query::result>> results, lw_shared_ptr<query::read_command> cmd,
        const query_options& options, db_clock::time_point now) {
    cql3::selection::result_set_builder builder(*_selection, now, options.get_serialization_format());

    // FIXME: This special casing saves us the cost of copying an already
    // linearized response. When we switch views to scattered_reader this will go away.
    if (results->buf().is_linearized()) {
        query::result_view view(results->buf().view());
        view.consume(cmd->slice, result_set_building_visitor(builder, *this));
    } else {
        bytes_ostream w(results->buf());
        query::result_view view(w.linearize());
        view.consume(cmd->slice, result_set_building_visitor(builder, *this));
    }

    auto rs = builder.build();
    if (needs_post_query_ordering()) {
        rs->sort(_ordering_comparator);
        if (_is_reversed) {
            rs->reverse();
        }
    }
    rs->trim(cmd->row_limit);
    return ::make_shared<transport::messages::result_message::rows>(std::move(rs));
}

::shared_ptr<parsed_statement::prepared>
select_statement::raw_statement::prepare(database& db) {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());
    auto bound_names = get_bound_variables();

    auto selection = _select_clause.empty()
                     ? selection::selection::wildcard(schema)
                     : selection::selection::from_selectors(db, schema, _select_clause);

    auto restrictions = prepare_restrictions(db, schema, bound_names, selection);

    if (_parameters->is_distinct()) {
        validate_distinct_selection(schema, selection, restrictions);
    }

    select_statement::ordering_comparator_type ordering_comparator;
    bool is_reversed_ = false;

    if (!_parameters->orderings().empty()) {
        verify_ordering_is_allowed(restrictions);
        ordering_comparator = get_ordering_comparator(schema, selection, restrictions);
        is_reversed_ = is_reversed(schema);
    }

    check_needs_filtering(restrictions);

    auto stmt = ::make_shared<select_statement>(schema,
        bound_names->size(),
        _parameters,
        std::move(selection),
        std::move(restrictions),
        is_reversed_,
        std::move(ordering_comparator),
        prepare_limit(db, bound_names));

    return ::make_shared<parsed_statement::prepared>(std::move(stmt), std::move(*bound_names));
}

::shared_ptr<restrictions::statement_restrictions>
select_statement::raw_statement::prepare_restrictions(database& db, schema_ptr schema,
    ::shared_ptr<variable_specifications> bound_names,
    ::shared_ptr<selection::selection> selection)
{
    try {
        return ::make_shared<restrictions::statement_restrictions>(db, schema, std::move(_where_clause), bound_names,
            selection->contains_only_static_columns(), selection->contains_a_collection());
    } catch (const exceptions::unrecognized_entity_exception& e) {
        if (contains_alias(e.entity)) {
            throw exceptions::invalid_request_exception(sprint("Aliases aren't allowed in the where clause ('%s')", e.relation->to_string()));
        }
        throw;
    }
}

/** Returns a ::shared_ptr<term> for the limit or null if no limit is set */
::shared_ptr<term>
select_statement::raw_statement::prepare_limit(database& db, ::shared_ptr<variable_specifications> bound_names) {
    if (!_limit) {
        return {};
    }

    auto prep_limit = _limit->prepare(db, keyspace(), limit_receiver());
    prep_limit->collect_marker_specification(bound_names);
    return prep_limit;
}

void select_statement::raw_statement::verify_ordering_is_allowed(
    ::shared_ptr<restrictions::statement_restrictions> restrictions)
{
    if (restrictions->uses_secondary_indexing()) {
        throw exceptions::invalid_request_exception("ORDER BY with 2ndary indexes is not supported.");
    }
    if (restrictions->is_key_range()) {
        throw exceptions::invalid_request_exception("ORDER BY is only supported when the partition key is restricted by an EQ or an IN.");
    }
}

void select_statement::raw_statement::validate_distinct_selection(schema_ptr schema,
    ::shared_ptr<selection::selection> selection,
    ::shared_ptr<restrictions::statement_restrictions> restrictions)
{
    for (auto&& def : selection->get_columns()) {
        if (!def->is_partition_key() && !def->is_static()) {
            throw exceptions::invalid_request_exception(sprint(
                "SELECT DISTINCT queries must only request partition key columns and/or static columns (not %s)",
                def->name_as_text()));
        }
    }

    // If it's a key range, we require that all partition key columns are selected so we don't have to bother
    // with post-query grouping.
    if (!restrictions->is_key_range()) {
        return;
    }

    for (auto&& def : schema->partition_key_columns()) {
        if (!selection->has_column(def)) {
            throw exceptions::invalid_request_exception(sprint(
                "SELECT DISTINCT queries must request all the partition key columns (missing %s)", def.name_as_text()));
        }
    }
}

void select_statement::raw_statement::handle_unrecognized_ordering_column(
    ::shared_ptr<column_identifier> column)
{
    if (contains_alias(column)) {
        throw exceptions::invalid_request_exception(sprint("Aliases are not allowed in order by clause ('%s')", *column));
    }
    throw exceptions::invalid_request_exception(sprint("Order by on unknown column %s", *column));
}

select_statement::ordering_comparator_type
select_statement::raw_statement::get_ordering_comparator(schema_ptr schema,
    ::shared_ptr<selection::selection> selection,
    ::shared_ptr<restrictions::statement_restrictions> restrictions)
{
    if (!restrictions->key_is_in_relation()) {
        return {};
    }

    std::vector<std::pair<uint32_t, data_type>> sorters;
    sorters.reserve(_parameters->orderings().size());

    // If we order post-query (see orderResults), the sorted column needs to be in the ResultSet for sorting,
    // even if we don't
    // ultimately ship them to the client (CASSANDRA-4911).
    for (auto&& e : _parameters->orderings()) {
        auto&& raw = e.first;
        ::shared_ptr<column_identifier> column = raw->prepare_column_identifier(schema);
        const column_definition* def = schema->get_column_definition(column->name());
        if (!def) {
            handle_unrecognized_ordering_column(column);
        }
        auto index = selection->index_of(*def);
        if (index < 0) {
            index = selection->add_column_for_ordering(*def);
        }

        sorters.emplace_back(index, def->type);
    }

    return [sorters = std::move(sorters)] (const result_row_type& r1, const result_row_type& r2) mutable {
        for (auto&& e : sorters) {
            auto& c1 = r1[e.first];
            auto& c2 = r2[e.first];
            auto type = e.second;

            if (bool(c1) != bool(c2)) {
                return bool(c2);
            }
            if (c1) {
                int result = type->compare(*c1, *c2);
                if (result != 0) {
                    return result < 0;
                }
            }
        }
        return false;
    };
}

bool select_statement::raw_statement::is_reversed(schema_ptr schema) {
    std::experimental::optional<bool> reversed_map[schema->clustering_key_size()];

    uint32_t i = 0;
    for (auto&& e : _parameters->orderings()) {
        ::shared_ptr<column_identifier> column = e.first->prepare_column_identifier(schema);
        bool reversed = e.second;

        auto def = schema->get_column_definition(column->name());
        if (!def) {
            handle_unrecognized_ordering_column(column);
        }

        if (!def->is_clustering_key()) {
            throw exceptions::invalid_request_exception(sprint(
                "Order by is currently only supported on the clustered columns of the PRIMARY KEY, got %s", *column));
        }

        if (i != def->component_index()) {
            throw exceptions::invalid_request_exception(
                "Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY");
        }

        reversed_map[i] = std::experimental::make_optional(reversed != def->type->is_reversed());
        ++i;
    }

    // GCC incorrenctly complains about "*is_reversed_" below
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

    // Check that all bool in reversedMap, if set, agrees
    std::experimental::optional<bool> is_reversed_{};
    for (auto&& b : reversed_map) {
        if (b) {
            if (!is_reversed_) {
                is_reversed_ = b;
            } else {
                if ((*is_reversed_) != *b) {
                    throw exceptions::invalid_request_exception("Unsupported order by relation");
                }
            }
        }
    }

    assert(is_reversed_);
    return *is_reversed_;

#pragma GCC diagnostic pop
}

/** If ALLOW FILTERING was not specified, this verifies that it is not needed */
void select_statement::raw_statement::check_needs_filtering(
    ::shared_ptr<restrictions::statement_restrictions> restrictions)
{
    // non-key-range non-indexed queries cannot involve filtering underneath
    if (!_parameters->allow_filtering() && (restrictions->is_key_range() || restrictions->uses_secondary_indexing())) {
        // We will potentially filter data if either:
        //  - Have more than one IndexExpression
        //  - Have no index expression and the column filter is not the identity
        if (restrictions->need_filtering()) {
            throw exceptions::invalid_request_exception(
                "Cannot execute this query as it might involve data filtering and "
                    "thus may have unpredictable performance. If you want to execute "
                    "this query despite the performance unpredictability, use ALLOW FILTERING");
        }
    }
}

bool select_statement::raw_statement::contains_alias(::shared_ptr<column_identifier> name) {
    return std::any_of(_select_clause.begin(), _select_clause.end(), [name] (auto raw) {
        return raw->alias && *name == *raw->alias;
    });
}

::shared_ptr<column_specification> select_statement::raw_statement::limit_receiver() {
    return ::make_shared<column_specification>(keyspace(), column_family(), ::make_shared<column_identifier>("[limit]", true),
        int32_type);
}

}
}
