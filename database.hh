/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef DATABASE_HH_
#define DATABASE_HH_

#include "dht/i_partitioner.hh"
#include "locator/abstract_replication_strategy.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "net/byteorder.hh"
#include "utils/UUID.hh"
#include "utils/hash.hh"
#include "db_clock.hh"
#include "gc_clock.hh"
#include "core/distributed.hh"
#include <functional>
#include <cstdint>
#include <unordered_map>
#include <map>
#include <set>
#include <iostream>
#include <boost/functional/hash.hpp>
#include <experimental/optional>
#include <string.h>
#include "types.hh"
#include "compound.hh"
#include "core/future.hh"
#include "core/gate.hh"
#include "cql3/column_specification.hh"
#include "db/commitlog/replay_position.hh"
#include <limits>
#include <cstddef>
#include "schema.hh"
#include "timestamp.hh"
#include "tombstone.hh"
#include "atomic_cell.hh"
#include "query-request.hh"
#include "query-result.hh"
#include "keys.hh"
#include "mutation.hh"
#include "memtable.hh"
#include <list>
#include "mutation_reader.hh"
#include "row_cache.hh"

class frozen_mutation;

namespace service {
class storage_proxy;
}

namespace sstables {

class sstable;

}

namespace db {
template<typename T>
class serializer;

class commitlog;
class config;

namespace system_keyspace {
void make(database& db, bool durable);
}
}

class replay_position_reordered_exception : public std::exception {};

using memtable_list = std::vector<lw_shared_ptr<memtable>>;
using sstable_list = std::map<unsigned long, lw_shared_ptr<sstables::sstable>>;

class column_family {
public:
    struct config {
        sstring datadir;
        bool enable_disk_writes = true;
        bool enable_disk_reads = true;
        bool enable_cache = true;
    };
private:
    schema_ptr _schema;
    config _config;
    lw_shared_ptr<memtable_list> _memtables;
    // generation -> sstable. Ordered by key so we can easily get the most recent.
    lw_shared_ptr<sstable_list> _sstables;
    mutable row_cache _cache; // Cache covers only sstables.
    unsigned _sstable_generation = 1;
    unsigned _mutation_count = 0;
    db::replay_position _highest_flushed_rp;
private:
    void add_sstable(sstables::sstable&& sstable);
    void add_memtable();
    memtable& active_memtable() { return *_memtables->back(); }
    future<> update_cache(memtable&);
    struct merge_comparator;
private:
    // Creates a mutation reader which covers sstables.
    // Caller needs to ensure that column_family remains live (FIXME: relax this).
    // The 'range' parameter must be live as long as the reader is used.
    mutation_reader make_sstable_reader(const query::partition_range& range) const;

    mutation_source sstables_as_mutation_source();
public:
    // Creates a mutation reader which covers all data sources for this column family.
    // Caller needs to ensure that column_family remains live (FIXME: relax this).
    // Note: for data queries use query() instead.
    // The 'range' parameter must be live as long as the reader is used.
    mutation_reader make_reader(const query::partition_range& range = query::full_partition_range) const;

    // Queries can be satisfied from multiple data sources, so they are returned
    // as temporaries.
    //
    // FIXME: in case a query is satisfied from a single memtable, avoid a copy
    using const_mutation_partition_ptr = std::unique_ptr<const mutation_partition>;
    using const_row_ptr = std::unique_ptr<const row>;
public:
    column_family(schema_ptr schema, config cfg);
    column_family(column_family&&) = delete; // 'this' is being captured during construction
    ~column_family();
    schema_ptr schema() const { return _schema; }
    future<const_mutation_partition_ptr> find_partition(const dht::decorated_key& key) const;
    future<const_mutation_partition_ptr> find_partition_slow(const partition_key& key) const;
    future<const_row_ptr> find_row(const dht::decorated_key& partition_key, clustering_key clustering_key) const;
    void apply(const frozen_mutation& m, const db::replay_position& = db::replay_position(), database* = nullptr);
    void apply(const mutation& m, const db::replay_position& = db::replay_position(), database* = nullptr);

    // Returns at most "cmd.limit" rows
    future<lw_shared_ptr<query::result>> query(const query::read_command& cmd, const std::vector<query::partition_range>& ranges) const;

    future<> populate(sstring datadir);
    void seal_active_memtable(database* = nullptr);

    future<> stop(database* db = nullptr) {
        seal_active_memtable(db);
        return _in_flight_seals.close();
    }
private:
    seastar::gate _in_flight_seals;

    // Iterate over all partitions.  Protocol is the same as std::all_of(),
    // so that iteration can be stopped by returning false.
    // Func signature: bool (const decorated_key& dk, const mutation_partition& mp)
    template <typename Func>
    future<bool> for_all_partitions(Func&& func) const;
    future<> probe_file(sstring sstdir, sstring fname);
    void seal_on_overflow(database*);
    void check_valid_rp(const db::replay_position&) const;
public:
    // Iterate over all partitions.  Protocol is the same as std::all_of(),
    // so that iteration can be stopped by returning false.
    future<bool> for_all_partitions_slow(std::function<bool (const dht::decorated_key&, const mutation_partition&)> func) const;

    friend std::ostream& operator<<(std::ostream& out, const column_family& cf);
};

class user_types_metadata {
    std::unordered_map<bytes, user_type> _user_types;
public:
    user_type get_type(bytes name) const {
        return _user_types.at(name);
    }
    const std::unordered_map<bytes, user_type>& get_all_types() const {
        return _user_types;
    }
    void add_type(user_type type) {
        auto i = _user_types.find(type->_name);
        assert(i == _user_types.end() || type->is_compatible_with(*i->second));
        _user_types[type->_name] = std::move(type);
    }
    void remove_type(user_type type) {
        _user_types.erase(type->_name);
    }
};

class keyspace_metadata final {
    sstring _name;
    sstring _strategy_name;
    std::map<sstring, sstring> _strategy_options;
    std::unordered_map<sstring, schema_ptr> _cf_meta_data;
    bool _durable_writes;
    lw_shared_ptr<user_types_metadata> _user_types;
public:
    keyspace_metadata(sstring name,
                 sstring strategy_name,
                 std::map<sstring, sstring> strategy_options,
                 bool durable_writes,
                 std::vector<schema_ptr> cf_defs = std::vector<schema_ptr>{},
                 lw_shared_ptr<user_types_metadata> user_types = make_lw_shared<user_types_metadata>())
        : _name{std::move(name)}
        , _strategy_name{strategy_name.empty() ? "NetworkTopologyStrategy" : strategy_name}
        , _strategy_options{std::move(strategy_options)}
        , _durable_writes{durable_writes}
        , _user_types{std::move(user_types)}
    {
        for (auto&& s : cf_defs) {
            _cf_meta_data.emplace(s->cf_name(), s);
        }
    }
    static lw_shared_ptr<keyspace_metadata>
    new_keyspace(sstring name,
                 sstring strategy_name,
                 std::map<sstring, sstring> options,
                 bool durables_writes,
                 std::vector<schema_ptr> cf_defs = std::vector<schema_ptr>{})
    {
        return ::make_lw_shared<keyspace_metadata>(name, strategy_name, options, durables_writes, cf_defs);
    }
    const sstring& name() const {
        return _name;
    }
    const sstring& strategy_name() const {
        return _strategy_name;
    }
    const std::map<sstring, sstring>& strategy_options() const {
        return _strategy_options;
    }
    const std::unordered_map<sstring, schema_ptr>& cf_meta_data() const {
        return _cf_meta_data;
    }
    bool durable_writes() const {
        return _durable_writes;
    }
    const lw_shared_ptr<user_types_metadata>& user_types() const {
        return _user_types;
    }
    void add_column_family(const schema_ptr& s) {
        _cf_meta_data.emplace(s->cf_name(), s);
    }
};

class keyspace {
public:
    struct config {
        sstring datadir;
        bool enable_disk_reads = true;
        bool enable_disk_writes = true;
    };
private:
    std::unique_ptr<locator::abstract_replication_strategy> _replication_strategy;
    lw_shared_ptr<keyspace_metadata> _metadata;
    config _config;
public:
    explicit keyspace(lw_shared_ptr<keyspace_metadata> metadata, config cfg)
        : _metadata(std::move(metadata))
        , _config(std::move(cfg))
    {}
    user_types_metadata _user_types;
    const lw_shared_ptr<keyspace_metadata>& metadata() const {
        return _metadata;
    }
    void create_replication_strategy(const std::map<sstring, sstring>& options);
    locator::abstract_replication_strategy& get_replication_strategy();
    column_family::config make_column_family_config(const schema& s) const;
    future<> make_directory_for_column_family(const sstring& name, utils::UUID uuid);
    void add_column_family(const schema_ptr& s) {
        _metadata->add_column_family(s);
    }

    // FIXME to allow simple registration at boostrap
    void set_replication_strategy(std::unique_ptr<locator::abstract_replication_strategy> replication_strategy);
private:
    sstring column_family_directory(const sstring& name, utils::UUID uuid) const;
};

class no_such_keyspace : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

class no_such_column_family : public std::runtime_error {
public:
    using runtime_error::runtime_error;
};

// Policy for distributed<database>:
//   broadcast metadata writes
//   local metadata reads
//   use shard_of() for data

class database {
    std::unordered_map<sstring, keyspace> _keyspaces;
    std::unordered_map<utils::UUID, lw_shared_ptr<column_family>> _column_families;
    std::unordered_map<std::pair<sstring, sstring>, utils::UUID, utils::tuple_hash> _ks_cf_to_uuid;
    std::unique_ptr<db::commitlog> _commitlog;
    std::unique_ptr<db::config> _cfg;

    future<> init_commitlog();
    future<> apply_in_memory(const frozen_mutation&, const db::replay_position&);
    future<> populate(sstring datadir);
    future<> populate_keyspace(sstring datadir, sstring ks_name);

private:
    // Unless you are an earlier boostraper or the database itself, you should
    // not be using this directly.  Go for the public create_keyspace instead.
    void add_keyspace(sstring name, keyspace k);
    void create_in_memory_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm);
    friend void db::system_keyspace::make(database& db, bool durable);

public:
    future<> parse_system_tables(distributed<service::storage_proxy>&);
    database();
    database(const db::config&);
    database(database&&) = default;
    ~database();

    db::commitlog* commitlog() const {
        return _commitlog.get();
    }

    future<> init_from_data_directory(distributed<service::storage_proxy>& p);

    void add_column_family(schema_ptr schema, column_family::config cfg);

    void update_column_family(const sstring& ks_name, const sstring& cf_name);
    void drop_column_family(const sstring& ks_name, const sstring& cf_name);

    /* throws std::out_of_range if missing */
    const utils::UUID& find_uuid(const sstring& ks, const sstring& cf) const throw (std::out_of_range);
    const utils::UUID& find_uuid(const schema_ptr&) const throw (std::out_of_range);

    /**
     * Creates a keyspace for a given metadata if it still doesn't exist.
     *
     * @return ready future when the operation is complete
     */
    future<> create_keyspace(const lw_shared_ptr<keyspace_metadata>&);
    /* below, find_keyspace throws no_such_<type> on fail */
    keyspace& find_keyspace(const sstring& name) throw (no_such_keyspace);
    const keyspace& find_keyspace(const sstring& name) const throw (no_such_keyspace);
    bool has_keyspace(const sstring& name) const;
    void update_keyspace(const sstring& name);
    void drop_keyspace(const sstring& name);
    const auto& keyspaces() const { return _keyspaces; }
    column_family& find_column_family(const sstring& ks, const sstring& name) throw (no_such_column_family);
    const column_family& find_column_family(const sstring& ks, const sstring& name) const throw (no_such_column_family);
    column_family& find_column_family(const utils::UUID&) throw (no_such_column_family);
    const column_family& find_column_family(const utils::UUID&) const throw (no_such_column_family);
    column_family& find_column_family(const schema_ptr&) throw (no_such_column_family);
    const column_family& find_column_family(const schema_ptr&) const throw (no_such_column_family);
    schema_ptr find_schema(const sstring& ks_name, const sstring& cf_name) const throw (no_such_column_family);
    schema_ptr find_schema(const utils::UUID&) const throw (no_such_column_family);
    bool has_schema(const sstring& ks_name, const sstring& cf_name) const;
    std::set<sstring> existing_index_names(const sstring& cf_to_exclude = sstring()) const;
    future<> stop();
    unsigned shard_of(const dht::token& t);
    unsigned shard_of(const mutation& m);
    unsigned shard_of(const frozen_mutation& m);
    future<lw_shared_ptr<query::result>> query(const query::read_command& cmd, const std::vector<query::partition_range>& ranges);
    future<> apply(const frozen_mutation&);
    keyspace::config make_keyspace_config(const keyspace_metadata& ksm) const;
    const sstring& get_snitch_name() const;

    friend std::ostream& operator<<(std::ostream& out, const database& db);
    friend future<> create_keyspace(distributed<database>&, const lw_shared_ptr<keyspace_metadata>&);
    const std::unordered_map<sstring, keyspace>& get_keyspaces() const {
        return _keyspaces;
    }
    const std::unordered_map<utils::UUID, lw_shared_ptr<column_family>>& get_column_families() const {
        return _column_families;
    }
    const std::unordered_map<std::pair<sstring, sstring>, utils::UUID, utils::tuple_hash>&
    get_column_families_mapping() const {
        return _ks_cf_to_uuid;
    }
};

// Creates a keyspace.  Keyspaces have a non-sharded
// component (the directory), so a global function is needed.
future<> create_keyspace(distributed<database>& db, const lw_shared_ptr<keyspace_metadata>&);

// FIXME: stub
class secondary_index_manager {};

inline
void
column_family::apply(const mutation& m, const db::replay_position& rp, database* db) {
    active_memtable().apply(m, rp);
    seal_on_overflow(db);
}

inline
void
column_family::seal_on_overflow(database* db) {
    // FIXME: something better
    if (++_mutation_count == 100000) {
        _mutation_count = 0;
        seal_active_memtable(db);
    }
}

inline
void
column_family::check_valid_rp(const db::replay_position& rp) const {
    if (rp < _highest_flushed_rp) {
        throw replay_position_reordered_exception();
    }
}

inline
void
column_family::apply(const frozen_mutation& m, const db::replay_position& rp, database* db) {
    check_valid_rp(rp);
    active_memtable().apply(m, rp);
    seal_on_overflow(db);
}

#endif /* DATABASE_HH_ */
