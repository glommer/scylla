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

#pragma once

#include "cql3/restrictions/restriction.hh"
#include "cql3/statements/cf_statement.hh"
#include "cql3/column_identifier.hh"
#include "cql3/update_parameters.hh"
#include "cql3/column_condition.hh"
#include "cql3/cql_statement.hh"
#include "cql3/attributes.hh"
#include "cql3/operation.hh"
#include "cql3/relation.hh"

#include "db/consistency_level.hh"

#include "core/shared_ptr.hh"
#include "core/future-util.hh"

#include "unimplemented.hh"
#include "validation.hh"
#include "service/storage_proxy.hh"

#include <memory>

namespace cql3 {

namespace statements {

/*
 * Abstract parent class of individual modifications, i.e. INSERT, UPDATE and DELETE.
 */
class modification_statement : public cql_statement {
private:
    static thread_local const ::shared_ptr<column_identifier> CAS_RESULT_COLUMN;

public:
    enum class statement_type { INSERT, UPDATE, DELETE };

    const statement_type type;

private:
    const uint32_t _bound_terms;

public:
    const schema_ptr s;
    const std::unique_ptr<attributes> attrs;

protected:
    std::unordered_map<const column_definition*, ::shared_ptr<restrictions::restriction>> _processed_keys;
    std::vector<::shared_ptr<operation>> _column_operations;
private:
    // Separating normal and static conditions makes things somewhat easier
    std::vector<::shared_ptr<column_condition>> _column_conditions;
    std::vector<::shared_ptr<column_condition>> _static_conditions;

    bool _if_not_exists = false;
    bool _if_exists = false;

    bool _has_no_clustering_columns = true;

    bool _sets_static_columns = false;
    bool _sets_regular_columns = false;

    const std::function<const column_definition&(::shared_ptr<column_condition>)> get_column_for_condition =
        [](::shared_ptr<column_condition> cond) -> const column_definition& {
            return cond->column;
        };

public:
    modification_statement(statement_type type_, uint32_t bound_terms, schema_ptr schema_, std::unique_ptr<attributes> attrs_)
        : type{type_}
        , _bound_terms{bound_terms}
        , s{schema_}
        , attrs{std::move(attrs_)}
        , _column_operations{}
    { }

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        if (attrs->uses_function(ks_name, function_name)) {
            return true;
        }
        for (auto&& e : _processed_keys) {
            auto r = e.second;
            if (r && r->uses_function(ks_name, function_name)) {
                return true;
            }
        }
        for (auto&& operation : _column_operations) {
            if (operation && operation->uses_function(ks_name, function_name)) {
                return true;
            }
        }
        for (auto&& condition : _column_conditions) {
            if (condition && condition->uses_function(ks_name, function_name)) {
                return true;
            }
        }
        for (auto&& condition : _static_conditions) {
            if (condition && condition->uses_function(ks_name, function_name)) {
                return true;
            }
        }
        return false;
    }

    virtual bool require_full_clustering_key() const = 0;

    virtual void add_update_for_key(mutation& m, const exploded_clustering_prefix& prefix, const update_parameters& params) = 0;

    virtual uint32_t get_bound_terms() override {
        return _bound_terms;
    }

    virtual sstring keyspace() const {
        return s->ks_name();
    }

    virtual sstring column_family() const {
        return s->cf_name();
    }

    virtual bool is_counter() const {
        return s->is_counter();
    }

    int64_t get_timestamp(int64_t now, const query_options& options) const {
        return attrs->get_timestamp(now, options);
    }

    bool is_timestamp_set() const {
        return attrs->is_timestamp_set();
    }

    gc_clock::duration get_time_to_live(const query_options& options) const {
        return gc_clock::duration(attrs->get_time_to_live(options));
    }

    virtual void check_access(const service::client_state& state) override {
        warn(unimplemented::cause::PERMISSIONS);
#if 0
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.MODIFY);

        // CAS updates can be used to simulate a SELECT query, so should require Permission.SELECT as well.
        if (hasConditions())
            state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.SELECT);
#endif
    }

    void validate(distributed<service::storage_proxy>&, const service::client_state& state) override;

    void add_operation(::shared_ptr<operation> op) {
        if (op->column.is_static()) {
            _sets_static_columns = true;
        } else {
            _sets_regular_columns = true;
        }
        _column_operations.push_back(std::move(op));
    }

#if 0
    public Iterable<ColumnDefinition> getColumnsWithConditions()
    {
        if (ifNotExists || ifExists)
            return null;

        return Iterables.concat(columnConditions == null ? Collections.<ColumnDefinition>emptyList() : Iterables.transform(columnConditions, getColumnForCondition),
                                staticConditions == null ? Collections.<ColumnDefinition>emptyList() : Iterables.transform(staticConditions, getColumnForCondition));
    }
#endif
public:
    void add_condition(::shared_ptr<column_condition> cond) {
        if (cond->column.is_static()) {
            _sets_static_columns = true;
            _static_conditions.emplace_back(std::move(cond));
        } else {
            _sets_regular_columns = true;
            _column_conditions.emplace_back(std::move(cond));
        }
    }

    void set_if_not_exist_condition() {
        _if_not_exists = true;
    }

    bool has_if_not_exist_condition() const {
        return _if_not_exists;
    }

    void set_if_exist_condition() {
        _if_exists = true;
    }

    bool has_if_exist_condition() const {
        return _if_exists;
    }

private:
    void add_key_values(const column_definition& def, ::shared_ptr<restrictions::restriction> values);

public:
    void add_key_value(const column_definition& def, ::shared_ptr<term> value);
    void process_where_clause(database& db, std::vector<relation_ptr> where_clause, ::shared_ptr<variable_specifications> names);
    std::vector<partition_key> build_partition_keys(const query_options& options);

private:
    exploded_clustering_prefix create_exploded_clustering_prefix(const query_options& options);
    exploded_clustering_prefix create_exploded_clustering_prefix_internal(const query_options& options);

protected:
    const column_definition* get_first_empty_key();

public:
    bool requires_read() {
        return std::any_of(_column_operations.begin(), _column_operations.end(), [] (auto&& op) {
            return op->requires_read();
        });
    }

protected:
    future<update_parameters::prefetched_rows_type> read_required_rows(
                distributed<service::storage_proxy>& proxy,
                lw_shared_ptr<std::vector<partition_key>> keys,
                lw_shared_ptr<exploded_clustering_prefix> prefix,
                bool local,
                db::consistency_level cl);

public:
    bool has_conditions() {
        return _if_not_exists || _if_exists || !_column_conditions.empty() || !_static_conditions.empty();
    }

    virtual future<::shared_ptr<transport::messages::result_message>>
    execute(distributed<service::storage_proxy>& proxy, service::query_state& qs, const query_options& options) override;

    virtual future<::shared_ptr<transport::messages::result_message>>
    execute_internal(distributed<service::storage_proxy>& proxy, service::query_state& qs, const query_options& options) override {
        throw std::runtime_error("not implemented");
    }

private:
    future<>
    execute_without_condition(distributed<service::storage_proxy>& proxy, service::query_state& qs, const query_options& options);

    future<::shared_ptr<transport::messages::result_message>>
    execute_with_condition(distributed<service::storage_proxy>& proxy, service::query_state& qs, const query_options& options);

#if 0
    public void addConditions(Composite clusteringPrefix, CQL3CasRequest request, QueryOptions options) throws InvalidRequestException
    {
        if (ifNotExists)
        {
            // If we use ifNotExists, if the statement applies to any non static columns, then the condition is on the row of the non-static
            // columns and the prefix should be the clusteringPrefix. But if only static columns are set, then the ifNotExists apply to the existence
            // of any static columns and we should use the prefix for the "static part" of the partition.
            request.addNotExist(clusteringPrefix);
        }
        else if (ifExists)
        {
            request.addExist(clusteringPrefix);
        }
        else
        {
            if (columnConditions != null)
                request.addConditions(clusteringPrefix, columnConditions, options);
            if (staticConditions != null)
                request.addConditions(cfm.comparator.staticPrefix(), staticConditions, options);
        }
    }

    private ResultSet buildCasResultSet(ByteBuffer key, ColumnFamily cf, QueryOptions options) throws InvalidRequestException
    {
        return buildCasResultSet(keyspace(), key, columnFamily(), cf, getColumnsWithConditions(), false, options);
    }

    public static ResultSet buildCasResultSet(String ksName, ByteBuffer key, String cfName, ColumnFamily cf, Iterable<ColumnDefinition> columnsWithConditions, boolean isBatch, QueryOptions options)
    throws InvalidRequestException
    {
        boolean success = cf == null;

        ColumnSpecification spec = new ColumnSpecification(ksName, cfName, CAS_RESULT_COLUMN, BooleanType.instance);
        ResultSet.Metadata metadata = new ResultSet.Metadata(Collections.singletonList(spec));
        List<List<ByteBuffer>> rows = Collections.singletonList(Collections.singletonList(BooleanType.instance.decompose(success)));

        ResultSet rs = new ResultSet(metadata, rows);
        return success ? rs : merge(rs, buildCasFailureResultSet(key, cf, columnsWithConditions, isBatch, options));
    }

    private static ResultSet merge(ResultSet left, ResultSet right)
    {
        if (left.size() == 0)
            return right;
        else if (right.size() == 0)
            return left;

        assert left.size() == 1;
        int size = left.metadata.names.size() + right.metadata.names.size();
        List<ColumnSpecification> specs = new ArrayList<ColumnSpecification>(size);
        specs.addAll(left.metadata.names);
        specs.addAll(right.metadata.names);
        List<List<ByteBuffer>> rows = new ArrayList<>(right.size());
        for (int i = 0; i < right.size(); i++)
        {
            List<ByteBuffer> row = new ArrayList<ByteBuffer>(size);
            row.addAll(left.rows.get(0));
            row.addAll(right.rows.get(i));
            rows.add(row);
        }
        return new ResultSet(new ResultSet.Metadata(specs), rows);
    }

    private static ResultSet buildCasFailureResultSet(ByteBuffer key, ColumnFamily cf, Iterable<ColumnDefinition> columnsWithConditions, boolean isBatch, QueryOptions options)
    throws InvalidRequestException
    {
        CFMetaData cfm = cf.metadata();
        Selection selection;
        if (columnsWithConditions == null)
        {
            selection = Selection.wildcard(cfm);
        }
        else
        {
            // We can have multiple conditions on the same columns (for collections) so use a set
            // to avoid duplicate, but preserve the order just to it follows the order of IF in the query in general
            Set<ColumnDefinition> defs = new LinkedHashSet<>();
            // Adding the partition key for batches to disambiguate if the conditions span multipe rows (we don't add them outside
            // of batches for compatibility sakes).
            if (isBatch)
            {
                defs.addAll(cfm.partitionKeyColumns());
                defs.addAll(cfm.clusteringColumns());
            }
            for (ColumnDefinition def : columnsWithConditions)
                defs.add(def);
            selection = Selection.forColumns(cfm, new ArrayList<>(defs));

        }

        long now = System.currentTimeMillis();
        Selection.ResultSetBuilder builder = selection.resultSetBuilder(now);
        SelectStatement.forSelection(cfm, selection).processColumnFamily(key, cf, options, now, builder);

        return builder.build(options.getProtocolVersion());
    }

    public ResultMessage executeInternal(QueryState queryState, QueryOptions options) throws RequestValidationException, RequestExecutionException
    {
        if (hasConditions())
            throw new UnsupportedOperationException();

        for (IMutation mutation : getMutations(options, true, queryState.getTimestamp()))
        {
            // We don't use counters internally.
            assert mutation instanceof Mutation;

            ((Mutation) mutation).apply();
        }
        return null;
    }
#endif

public:
    /**
     * Convert statement into a list of mutations to apply on the server
     *
     * @param options value for prepared statement markers
     * @param local if true, any requests (for collections) performed by getMutation should be done locally only.
     * @param now the current timestamp in microseconds to use if no timestamp is user provided.
     *
     * @return vector of the mutations
     * @throws invalid_request_exception on invalid requests
     */
    future<std::vector<mutation>> get_mutations(distributed<service::storage_proxy>& proxy, const query_options& options, bool local, int64_t now);

public:
    future<std::unique_ptr<update_parameters>> make_update_parameters(
                distributed<service::storage_proxy>& proxy,
                lw_shared_ptr<std::vector<partition_key>> keys,
                lw_shared_ptr<exploded_clustering_prefix> prefix,
                const query_options& options,
                bool local,
                int64_t now);

protected:
    /**
     * If there are conditions on the statement, this is called after the where clause and conditions have been
     * processed to check that they are compatible.
     * @throws InvalidRequestException
     */
    virtual void validate_where_clause_for_conditions() {
        //  no-op by default
    }

public:
    class parsed : public cf_statement {
    public:
        using conditions_vector = std::vector<std::pair<::shared_ptr<column_identifier::raw>, ::shared_ptr<column_condition::raw>>>;
    protected:
        const ::shared_ptr<attributes::raw> _attrs;
        const std::vector<std::pair<::shared_ptr<column_identifier::raw>, ::shared_ptr<column_condition::raw>>> _conditions;
    private:
        const bool _if_not_exists;
        const bool _if_exists;
    protected:
        parsed(::shared_ptr<cf_name> name, ::shared_ptr<attributes::raw> attrs, conditions_vector conditions, bool if_not_exists, bool if_exists)
            : cf_statement{std::move(name)}
            , _attrs{std::move(attrs)}
            , _conditions{std::move(conditions)}
            , _if_not_exists{if_not_exists}
            , _if_exists{if_exists}
        { }

    public:
        virtual ::shared_ptr<parsed_statement::prepared> prepare(database& db) override;
        ::shared_ptr<modification_statement> prepare(database& db, ::shared_ptr<variable_specifications> bound_names);;
    protected:
        virtual ::shared_ptr<modification_statement> prepare_internal(database& db, schema_ptr schema,
            ::shared_ptr<variable_specifications> bound_names, std::unique_ptr<attributes> attrs) = 0;
    };
};

std::ostream& operator<<(std::ostream& out, modification_statement::statement_type t);

}

}
