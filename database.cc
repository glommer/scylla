/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "log.hh"
#include "database.hh"
#include "unimplemented.hh"
#include "core/future-util.hh"
#include "db/system_keyspace.hh"
#include "db/consistency_level.hh"
#include "db/serializer.hh"
#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "to_string.hh"
#include "query-result-writer.hh"
#include "nway_merger.hh"
#include "cql3/column_identifier.hh"
#include "core/seastar.hh"
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include "sstables/sstables.hh"
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/adaptor/map.hpp>
#include "locator/simple_snitch.hh"
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/function_output_iterator.hpp>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <boost/range/algorithm/find.hpp>
#include "frozen_mutation.hh"
#include "mutation_partition_applier.hh"
#include "core/do_with.hh"
#include "service/storage_service.hh"

thread_local logging::logger dblog("database");

memtable::memtable(schema_ptr schema)
        : _schema(std::move(schema))
        , partitions(dht::decorated_key::less_comparator(_schema)) {
}

column_family::column_family(schema_ptr schema, config config)
    : _schema(std::move(schema))
    , _config(std::move(config))
    , _memtables(make_lw_shared(memtable_list{}))
    , _sstables(make_lw_shared<sstable_list>())
    , _cache(_schema, sstables_as_mutation_source(), global_cache_tracker())
{
    add_memtable();
}

mutation_source
column_family::sstables_as_mutation_source() {
    return [this] (const query::partition_range& r) {
        return make_sstable_reader(r);
    };
}

// define in .cc, since sstable is forward-declared in .hh
column_family::~column_family() {
}

memtable::const_mutation_partition_ptr
memtable::find_partition(const dht::decorated_key& key) const {
    auto i = partitions.find(key);
    // FIXME: remove copy if only one data source
    return i == partitions.end() ? const_mutation_partition_ptr() : std::make_unique<const mutation_partition>(i->second);
}

class range_sstable_reader {
    dht::token _min_token;
    dht::token _max_token;
    lw_shared_ptr<sstable_list> _sstables;
    mutation_reader _reader;
public:
    range_sstable_reader(schema_ptr s, lw_shared_ptr<sstable_list> sstables, const query::partition_range& pr)
        : _min_token(dht::minimum_token())
        , _max_token(dht::maximum_token())
        , _sstables(std::move(sstables))
    {
        if (!pr.is_full()) {
            // FIXME: make sstable::read_range_rows() accept query::partition_range
            fail(unimplemented::cause::RANGE_QUERIES);
        }

        std::vector<mutation_reader> readers;
        for (const lw_shared_ptr<sstables::sstable>& sst : *_sstables | boost::adaptors::map_values) {
            // FIXME: make sstable::read_range_rows() return ::mutation_reader so that we can drop this wrapper.
            readers.emplace_back([r = make_lw_shared(sst->read_range_rows(s, _min_token, _max_token))] () mutable { return r->read(); });
        }
        _reader = make_combined_reader(std::move(readers));
    }

    range_sstable_reader(range_sstable_reader&&) = delete; // reader takes reference to member fields

    future<mutation_opt> operator()() {
        return _reader();
    }
};

class single_key_sstable_reader {
    schema_ptr _schema;
    sstables::key _key;
    mutation_opt _m;
    bool _done = false;
    lw_shared_ptr<sstable_list> _sstables;
public:
    single_key_sstable_reader(schema_ptr schema, lw_shared_ptr<sstable_list> sstables, const partition_key& key)
        : _schema(std::move(schema))
        , _key(sstables::key::from_partition_key(*_schema, key))
        , _sstables(std::move(sstables))
    { }

    future<mutation_opt> operator()() {
        if (_done) {
            return make_ready_future<mutation_opt>();
        }
        return parallel_for_each(*_sstables | boost::adaptors::map_values, [this](const lw_shared_ptr<sstables::sstable>& sstable) {
            return sstable->read_row(_schema, _key).then([this](mutation_opt mo) {
                apply(_m, std::move(mo));
            });
        }).then([this] {
            _done = true;
            return std::move(_m);
        });
    }
};

mutation_reader
column_family::make_sstable_reader(const query::partition_range& pr) const {
    if (pr.is_singular() && pr.start_value().has_key()) {
        return single_key_sstable_reader(_schema, _sstables, *pr.start_value().key());
    } else {
        // range_sstable_reader is not movable so we need to wrap it
        return [r = make_lw_shared<range_sstable_reader>(_schema, _sstables, pr)] () mutable {
            return (*r)();
        };
    }
}

// Exposed for testing, not performance critical.
future<column_family::const_mutation_partition_ptr>
column_family::find_partition(const dht::decorated_key& key) const {
    return do_with(query::partition_range::make_singular(key), [this] (auto& range) {
        return do_with(this->make_reader(range), [] (mutation_reader& reader) {
            return reader().then([] (mutation_opt&& mo) -> std::unique_ptr<const mutation_partition> {
                if (!mo) {
                    return {};
                }
                return std::make_unique<const mutation_partition>(std::move(mo->partition()));
            });
        });
    });
}

future<column_family::const_mutation_partition_ptr>
column_family::find_partition_slow(const partition_key& key) const {
    return find_partition(dht::global_partitioner().decorate_key(*_schema, key));
}

future<column_family::const_row_ptr>
column_family::find_row(const dht::decorated_key& partition_key, clustering_key clustering_key) const {
    return find_partition(partition_key).then([clustering_key = std::move(clustering_key)] (const_mutation_partition_ptr p) {
        if (!p) {
            return make_ready_future<const_row_ptr>();
        }
        auto r = p->find_row(clustering_key);
        if (r) {
            // FIXME: remove copy if only one data source
            return make_ready_future<const_row_ptr>(std::make_unique<row>(*r));
        } else {
            return make_ready_future<const_row_ptr>();
        }
    });
}

mutation_partition&
memtable::find_or_create_partition_slow(partition_key_view key) {
    // FIXME: Perform lookup using std::pair<token, partition_key_view>
    // to avoid unconditional copy of the partition key.
    // We can't do it right now because std::map<> which holds
    // partitions doesn't support heterogenous lookup.
    // We could switch to boost::intrusive_map<> similar to what we have for row keys.
    return find_or_create_partition(dht::global_partitioner().decorate_key(*_schema, key));
}

mutation_partition&
memtable::find_or_create_partition(const dht::decorated_key& key) {
    // call lower_bound so we have a hint for the insert, just in case.
    auto i = partitions.lower_bound(key);
    if (i == partitions.end() || !key.equal(*_schema, i->first)) {
        i = partitions.emplace_hint(i, std::make_pair(std::move(key), mutation_partition(_schema)));
    }
    return i->second;
}

const memtable::partitions_type&
memtable::all_partitions() const {
    return partitions;
}

struct column_family::merge_comparator {
    schema_ptr _schema;
    using ptr = boost::iterator_range<memtable::partitions_type::const_iterator>*;
    merge_comparator(schema_ptr schema) : _schema(std::move(schema)) {}
    bool operator()(ptr x, ptr y) const {
        return y->front().first.less_compare(*_schema, x->front().first);
    }
};

boost::iterator_range<memtable::partitions_type::const_iterator>
memtable::slice(const query::partition_range& range) const {
    if (range.is_singular()) {
        const query::ring_position& pos = range.start_value();

        if (!pos.has_key()) {
            fail(unimplemented::cause::RANGE_QUERIES);
        }

        auto i = partitions.find(pos.as_decorated_key());
        if (i != partitions.end()) {
            return boost::make_iterator_range(i, std::next(i));
        } else {
            return boost::make_iterator_range(i, i);
        }
    } else {
        if (!range.is_full()) {
            fail(unimplemented::cause::RANGE_QUERIES);
        }

        return boost::make_iterator_range(partitions.begin(), partitions.end());
    }
}

mutation_reader
memtable::make_reader(const query::partition_range& range) const {
    auto r = slice(range);
    return [begin = r.begin(), end = r.end(), self = shared_from_this()] () mutable {
        if (begin != end) {
            auto m = mutation(self->_schema, begin->first, begin->second);
            ++begin;
            return make_ready_future<mutation_opt>(std::experimental::make_optional(std::move(m)));
        } else {
            return make_ready_future<mutation_opt>();
        }
    };
}

mutation_reader
column_family::make_reader(const query::partition_range& range) const {
    std::vector<mutation_reader> readers;
    readers.reserve(_memtables->size() + _sstables->size());

    for (auto&& mt : *_memtables) {
        readers.emplace_back(mt->make_reader(range));
    }

    if (_config.enable_cache) {
        readers.emplace_back(_cache.make_reader(range));
    } else {
        readers.emplace_back(make_sstable_reader(range));
    }

    return make_combined_reader(std::move(readers));
}

template <typename Func>
future<bool>
column_family::for_all_partitions(Func&& func) const {
    static_assert(std::is_same<bool, std::result_of_t<Func(const dht::decorated_key&, const mutation_partition&)>>::value,
                  "bad Func signature");

    struct iteration_state {
        mutation_reader reader;
        Func func;
        bool ok = true;
        bool empty = false;
    public:
        bool done() const { return !ok || empty; }
        iteration_state(const column_family& cf, Func&& func)
            : reader(cf.make_reader())
            , func(std::move(func))
        { }
    };

    return do_with(iteration_state(*this, std::move(func)), [] (iteration_state& is) {
        return do_until([&is] { return is.done(); }, [&is] {
            return is.reader().then([&is](mutation_opt&& mo) {
                if (!mo) {
                    is.empty = true;
                } else {
                    is.ok = is.func(mo->decorated_key(), mo->partition());
                }
            });
        }).then([&is] {
            return is.ok;
        });
    });
}

future<bool>
column_family::for_all_partitions_slow(std::function<bool (const dht::decorated_key&, const mutation_partition&)> func) const {
    return for_all_partitions(std::move(func));
}


row&
memtable::find_or_create_row_slow(const partition_key& partition_key, const clustering_key& clustering_key) {
    mutation_partition& p = find_or_create_partition_slow(partition_key);
    return p.clustered_row(clustering_key).cells();
}

class lister {
    file _f;
    std::function<future<> (directory_entry de)> _walker;
    directory_entry_type _expected_type;
    subscription<directory_entry> _listing;

public:
    lister(file f, directory_entry_type type, std::function<future<> (directory_entry)> walker)
            : _f(std::move(f))
            , _walker(std::move(walker))
            , _expected_type(type)
            , _listing(_f.list_directory([this] (directory_entry de) { return _visit(de); })) {
    }

    static future<> scan_dir(sstring name, directory_entry_type type, std::function<future<> (directory_entry)> walker);
protected:
    future<> _visit(directory_entry de) {

        // FIXME: stat and try to recover
        if (!de.type) {
            dblog.error("database found file with unknown type {}", de.name);
            return make_ready_future<>();
        }

        // Hide all synthetic directories and hidden files.
        if ((de.type != _expected_type) || (de.name[0] == '.')) {
            return make_ready_future<>();
        }
        return _walker(de);
    }
    future<> done() { return _listing.done(); }
};


future<> lister::scan_dir(sstring name, directory_entry_type type, std::function<future<> (directory_entry)> walker) {

    return engine().open_directory(name).then([type, walker = std::move(walker)] (file f) {
        auto l = make_lw_shared<lister>(std::move(f), type, walker);
        return l->done().then([l] { });
    });
}

static std::vector<sstring> parse_fname(sstring filename) {
    std::vector<sstring> comps;
    boost::split(comps , filename ,boost::is_any_of(".-"));
    return comps;
}

future<> column_family::probe_file(sstring sstdir, sstring fname) {

    using namespace sstables;

    auto comps = parse_fname(fname);
    if (comps.size() != 5) {
        dblog.error("Ignoring malformed file {}", fname);
        return make_ready_future<>();
    }

    // Every table will have a TOC. Using a specific file as a criteria, as
    // opposed to, say verifying _sstables.count() to be zero is more robust
    // against parallel loading of the directory contents.
    if (comps[3] != "TOC") {
        return make_ready_future<>();
    }

    sstable::version_types version;
    sstable::format_types  format;

    try {
        version = sstable::version_from_sstring(comps[0]);
    } catch (std::out_of_range) {
        dblog.error("Uknown version found: {}", comps[0]);
        return make_ready_future<>();
    }

    auto generation = boost::lexical_cast<unsigned long>(comps[1]);

    try {
        format = sstable::format_from_sstring(comps[2]);
    } catch (std::out_of_range) {
        dblog.error("Uknown format found: {}", comps[2]);
        return make_ready_future<>();
    }

    assert(_sstables->count(generation) == 0);

    try {
        auto sst = std::make_unique<sstables::sstable>(sstdir, generation, version, format);
        auto fut = sst->load();
        return std::move(fut).then([this, generation, sst = std::move(sst)] () mutable {
            add_sstable(std::move(*sst));
            return make_ready_future<>();
        });
    } catch (malformed_sstable_exception& e) {
        dblog.error("Skipping malformed sstable: {}", e.what());
        return make_ready_future<>();
    }

    return make_ready_future<>();
}

void column_family::add_sstable(sstables::sstable&& sstable) {
    auto generation = sstable.generation();
    // allow in-progress reads to continue using old list
    _sstables = make_lw_shared<sstable_list>(*_sstables);
    _sstables->emplace(generation, make_lw_shared(std::move(sstable)));
}

void column_family::add_memtable() {
    // allow in-progress reads to continue using old list
    _memtables = make_lw_shared(memtable_list(*_memtables));
    _memtables->emplace_back(make_lw_shared<memtable>(_schema));
}

future<>
column_family::update_cache(memtable& m) {
    // TODO: add option to disable populating of the cache.
    // TODO: move data into cache instead of copying
    return _cache.update(m.make_reader());
}

void
column_family::seal_active_memtable(database* db) {
    auto old = _memtables->back();
    if (old->empty()) {
        return;
    }
    add_memtable();
    assert(_highest_flushed_rp < old->replay_position()
    || (_highest_flushed_rp == db::replay_position() && old->replay_position() == db::replay_position())
    );
    _highest_flushed_rp = old->replay_position();
    // FIXME: better way of ensuring we don't attemt to
    //        overwrite an existing table.
    auto gen = _sstable_generation++ * smp::count + engine().cpu_id();
    sstring name = sprint("%s/%s-%s-%d-Data.db",
            _config.datadir,
            _schema->ks_name(), _schema->cf_name(),
            gen);
    // FIXME: this does not clear CL. Should it?
    if (!_config.enable_disk_writes) {
        return;
    }

    sstables::sstable newtab = sstables::sstable(_config.datadir, gen,
        sstables::sstable::version_types::la,
        sstables::sstable::format_types::big);

    _in_flight_seals.enter();
    do_with(std::move(newtab), [old, name, this, db] (sstables::sstable& newtab) {
        // FIXME: write all components
        return newtab.write_components(*old).then([name, this, &newtab, old] {
            return newtab.load();
        }).then([this, old] {
            return update_cache(*old);
        }).then_wrapped([name, this, &newtab, old, db] (future<> ret) {
            try {
                ret.get();
                add_sstable(std::move(newtab));

                // FIXME: until the surrounding function returns a future and
                // caller ensures ordering (i.e. finish flushing one or more sequential tables before
                // doing the discard), this below is _not_ correct, since the use of replay_position
                // depends on us reporting the factual highest position we've actually flushed,
                // _and_ all positions (for a given UUID) below having been dealt with.
                //
                // Note that the whole scheme is also dependent on memtables being "allocated" in order,
                // i.e. we may not flush a younger memtable before and older, and we need to use the
                // highest rp.
                auto cl = db ? db->commitlog() : nullptr;
                if (cl != nullptr) {
                    cl->discard_completed_segments(_schema->id(), old->replay_position());
                }
                _memtables->erase(boost::range::find(*_memtables, old));
            } catch (std::exception& e) {
                dblog.error("failed to write sstable: {}", e.what());
            } catch (...) {
                dblog.error("failed to write sstable: unknown error");
            }

            _in_flight_seals.leave();
        });
    });
    // FIXME: release commit log
    // FIXME: provide back-pressure to upper layers
}

future<> column_family::populate(sstring sstdir) {

    return lister::scan_dir(sstdir, directory_entry_type::regular, [this, sstdir] (directory_entry de) {
        // FIXME: The secondary indexes are in this level, but with a directory type, (starting with ".")
        return probe_file(sstdir, de.name);
    });
}

database::database() : database(db::config())
{}

database::database(const db::config& cfg) : _cfg(std::make_unique<db::config>(cfg))
{
    bool durable = cfg.data_file_directories().size() > 0;
    db::system_keyspace::make(*this, durable);
}

database::~database() {
}

future<> database::populate_keyspace(sstring datadir, sstring ks_name) {
    auto ksdir = datadir + "/" + ks_name;
    auto i = _keyspaces.find(ks_name);
    if (i == _keyspaces.end()) {
        dblog.warn("Skipping undefined keyspace: {}", ks_name);
    } else {
        dblog.warn("Populating Keyspace {}", ks_name);
        return lister::scan_dir(ksdir, directory_entry_type::directory, [this, ksdir, ks_name] (directory_entry de) {
            auto comps = parse_fname(de.name);
            if (comps.size() < 2) {
                dblog.error("Keyspace {}: Skipping malformed CF {} ", ksdir, de.name);
                return make_ready_future<>();
            }
            sstring cfname = comps[0];

            auto sstdir = ksdir + "/" + de.name;

            try {
                auto& cf = find_column_family(ks_name, cfname);
                dblog.info("Keyspace {}: Reading CF {} ", ksdir, cfname);
                // FIXME: Increase parallelism.
                return cf.populate(sstdir);
            } catch (no_such_column_family&) {
                dblog.warn("{}, CF {}: schema not loaded!", ksdir, comps[0]);
                return make_ready_future<>();
            }
        });
    }
    return make_ready_future<>();
}

future<> database::populate(sstring datadir) {
    return lister::scan_dir(datadir, directory_entry_type::directory, [this, datadir] (directory_entry de) {
        auto& ks_name = de.name;
        if (ks_name == "system") {
            return make_ready_future<>();
        }
        return populate_keyspace(datadir, ks_name);
    });
}

future<>
database::init_from_data_directory(distributed<service::storage_proxy>& proxy) {
    // FIXME support multiple directories
    return touch_directory(_cfg->data_file_directories()[0] + "/" + db::system_keyspace::NAME).then([this] {
        return populate_keyspace(_cfg->data_file_directories()[0], db::system_keyspace::NAME).then([this]() {
            return populate(_cfg->data_file_directories()[0]);
        }).then([this] {
            return init_commitlog();
        });
    });
}

future<>
database::init_commitlog() {
    auto logdir = _cfg->commitlog_directory() + "/work" + std::to_string(engine().cpu_id());

    return engine().file_type(logdir).then([this, logdir](auto type) {
        if (type && type.value() != directory_entry_type::directory) {
            throw std::runtime_error("Not a directory " + logdir);
        }
        if (!type && ::mkdir(logdir.c_str(), S_IRWXU) != 0) {
            throw std::runtime_error("Could not create directory " + logdir);
        }

        db::commitlog::config cfg(*_cfg);
        cfg.commit_log_location = logdir;

        return db::commitlog::create_commitlog(cfg).then([this](db::commitlog&& log) {
            _commitlog = std::make_unique<db::commitlog>(std::move(log));
        });
    });
}

unsigned
database::shard_of(const dht::token& t) {
    if (t._data.size() < 2) {
        return 0;
    }
    uint16_t v = uint8_t(t._data[t._data.size() - 1])
            | (uint8_t(t._data[t._data.size() - 2]) << 8);
    return v % smp::count;
}

unsigned
database::shard_of(const mutation& m) {
    return shard_of(m.token());
}

unsigned
database::shard_of(const frozen_mutation& m) {
    // FIXME: This lookup wouldn't be necessary if we
    // sent the partition key in legacy form or together
    // with token.
    schema_ptr schema = find_schema(m.column_family_id());
    return shard_of(dht::global_partitioner().get_token(*schema, m.key(*schema)));
}

void database::add_keyspace(sstring name, keyspace k) {
    if (_keyspaces.count(name) != 0) {
        throw std::invalid_argument("Keyspace " + name + " already exists");
    }
    _keyspaces.emplace(std::move(name), std::move(k));
}

future<>
create_keyspace(distributed<database>& db, const lw_shared_ptr<keyspace_metadata>& ksm) {
    // FIXME support multiple directories
    return db.invoke_on_all([ksm] (database& db) {
        return db.create_keyspace(ksm);
    });
    // FIXME: rollback on error, or keyspace directory remains on disk, poisoning
    // everything.
    // FIXME: sync parent directory?
}

void database::update_keyspace(const sstring& name) {
    throw std::runtime_error("not implemented");
}

void database::drop_keyspace(const sstring& name) {
    throw std::runtime_error("not implemented");
}

void database::add_column_family(schema_ptr schema, column_family::config cfg) {
    auto uuid = schema->id();
    auto cf = make_lw_shared<column_family>(schema, std::move(cfg));
    auto ks = _keyspaces.find(schema->ks_name());
    if (ks == _keyspaces.end()) {
        throw std::invalid_argument("Keyspace " + schema->ks_name() + " not defined");
    }
    if (_column_families.count(uuid) != 0) {
        throw std::invalid_argument("UUID " + uuid.to_sstring() + " already mapped");
    }
    auto kscf = std::make_pair(schema->ks_name(), schema->cf_name());
    if (_ks_cf_to_uuid.count(kscf) != 0) {
        throw std::invalid_argument("Column family " + schema->cf_name() + " exists");
    }
    ks->second.add_column_family(schema);
    _column_families.emplace(uuid, std::move(cf));
    _ks_cf_to_uuid.emplace(std::move(kscf), uuid);
}

void database::update_column_family(const sstring& ks_name, const sstring& cf_name) {
    throw std::runtime_error("not implemented");
}

void database::drop_column_family(const sstring& ks_name, const sstring& cf_name) {
    throw std::runtime_error("not implemented");
}

const utils::UUID& database::find_uuid(const sstring& ks, const sstring& cf) const throw (std::out_of_range) {
    return _ks_cf_to_uuid.at(std::make_pair(ks, cf));
}

const utils::UUID& database::find_uuid(const schema_ptr& schema) const throw (std::out_of_range) {
    return find_uuid(schema->ks_name(), schema->cf_name());
}

keyspace& database::find_keyspace(const sstring& name) throw (no_such_keyspace) {
    try {
        return _keyspaces.at(name);
    } catch (...) {
        std::throw_with_nested(no_such_keyspace(name));
    }
}

const keyspace& database::find_keyspace(const sstring& name) const throw (no_such_keyspace) {
    try {
        return _keyspaces.at(name);
    } catch (...) {
        std::throw_with_nested(no_such_keyspace(name));
    }
}

bool database::has_keyspace(const sstring& name) const {
    return _keyspaces.count(name) != 0;
}

column_family& database::find_column_family(const sstring& ks_name, const sstring& cf_name) throw (no_such_column_family) {
    try {
        return find_column_family(find_uuid(ks_name, cf_name));
    } catch (...) {
        std::throw_with_nested(no_such_column_family(ks_name + ":" + cf_name));
    }
}

const column_family& database::find_column_family(const sstring& ks_name, const sstring& cf_name) const throw (no_such_column_family) {
    try {
        return find_column_family(find_uuid(ks_name, cf_name));
    } catch (...) {
        std::throw_with_nested(no_such_column_family(ks_name + ":" + cf_name));
    }
}

column_family& database::find_column_family(const utils::UUID& uuid) throw (no_such_column_family) {
    try {
        return *_column_families.at(uuid);
    } catch (...) {
        std::throw_with_nested(no_such_column_family(uuid.to_sstring()));
    }
}

const column_family& database::find_column_family(const utils::UUID& uuid) const throw (no_such_column_family) {
    try {
        return *_column_families.at(uuid);
    } catch (...) {
        std::throw_with_nested(no_such_column_family(uuid.to_sstring()));
    }
}

void
keyspace::create_replication_strategy(const std::map<sstring, sstring>& options) {
    using namespace locator;

    auto& ss = service::get_local_storage_service();
    _replication_strategy =
            abstract_replication_strategy::create_replication_strategy(
                _metadata->name(), _metadata->strategy_name(),
                ss.get_token_metadata(), options);
}

locator::abstract_replication_strategy&
keyspace::get_replication_strategy() {
    return *_replication_strategy;
}

void
keyspace::set_replication_strategy(std::unique_ptr<locator::abstract_replication_strategy> replication_strategy) {
    _replication_strategy = std::move(replication_strategy);
}

column_family::config
keyspace::make_column_family_config(const schema& s) const {
    column_family::config cfg;
    cfg.datadir = column_family_directory(s.cf_name(), s.id());
    cfg.enable_disk_reads = _config.enable_disk_reads;
    cfg.enable_disk_writes = _config.enable_disk_writes;
    return cfg;
}

sstring
keyspace::column_family_directory(const sstring& name, utils::UUID uuid) const {
    return sprint("%s/%s-%s", _config.datadir, name, uuid);
}

future<>
keyspace::make_directory_for_column_family(const sstring& name, utils::UUID uuid) {
    return make_directory(column_family_directory(name, uuid));
}

column_family& database::find_column_family(const schema_ptr& schema) throw (no_such_column_family) {
    return find_column_family(schema->id());
}

const column_family& database::find_column_family(const schema_ptr& schema) const throw (no_such_column_family) {
    return find_column_family(schema->id());
}

schema_ptr database::find_schema(const sstring& ks_name, const sstring& cf_name) const throw (no_such_column_family) {
    return find_schema(find_uuid(ks_name, cf_name));
}

schema_ptr database::find_schema(const utils::UUID& uuid) const throw (no_such_column_family) {
    return find_column_family(uuid).schema();
}

bool database::has_schema(const sstring& ks_name, const sstring& cf_name) const {
    return _ks_cf_to_uuid.count(std::make_pair(ks_name, cf_name)) > 0;
}


void database::create_in_memory_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm) {
    keyspace ks(ksm, std::move(make_keyspace_config(*ksm)));
    ks.create_replication_strategy(ksm->strategy_options());
    _keyspaces.emplace(ksm->name(), std::move(ks));
}

future<>
database::create_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm) {
    auto i = _keyspaces.find(ksm->name());
    if (i != _keyspaces.end()) {
        return make_ready_future<>();
    }

    create_in_memory_keyspace(ksm);
    return touch_directory(_cfg->data_file_directories()[0] + "/" + ksm->name());
}

std::set<sstring>
database::existing_index_names(const sstring& cf_to_exclude) const {
    std::set<sstring> names;
    for (auto& p : _column_families) {
        auto& cf = *p.second;
        if (!cf_to_exclude.empty() && cf.schema()->cf_name() == cf_to_exclude) {
            continue;
        }
        for (auto& cd : cf.schema()->all_columns_in_select_order()) {
            if (cd.idx_info.index_name) {
                names.emplace(*cd.idx_info.index_name);
            }
        }
    }
    return names;
}

void
memtable::update(const db::replay_position& rp) {
    if (_replay_position < rp) {
        _replay_position = rp;
    }
}

void
memtable::apply(const mutation& m, const db::replay_position& rp) {
    mutation_partition& p = find_or_create_partition(m.decorated_key());
    p.apply(*_schema, m.partition());
    update(rp);
}

void
memtable::apply(const frozen_mutation& m, const db::replay_position& rp) {
    mutation_partition& p = find_or_create_partition_slow(m.key(*_schema));
    p.apply(*_schema, m.partition());
    update(rp);
}

// Based on:
//  - org.apache.cassandra.db.AbstractCell#reconcile()
//  - org.apache.cassandra.db.BufferExpiringCell#reconcile()
//  - org.apache.cassandra.db.BufferDeletedCell#reconcile()
int
compare_atomic_cell_for_merge(atomic_cell_view left, atomic_cell_view right) {
    if (left.timestamp() != right.timestamp()) {
        return left.timestamp() > right.timestamp() ? 1 : -1;
    }
    if (left.is_live() != right.is_live()) {
        return left.is_live() ? -1 : 1;
    }
    if (left.is_live()) {
        auto c = compare_unsigned(left.value(), right.value());
        if (c != 0) {
            return c;
        }
        if (left.is_live_and_has_ttl()
            && right.is_live_and_has_ttl()
            && left.expiry() != right.expiry())
        {
            return left.expiry() < right.expiry() ? -1 : 1;
        }
    } else {
        // Both are deleted
        if (left.deletion_time() != right.deletion_time()) {
            // Origin compares big-endian serialized deletion time. That's because it
            // delegates to AbstractCell.reconcile() which compares values after
            // comparing timestamps, which in case of deleted cells will hold
            // serialized expiry.
            return (uint32_t) left.deletion_time().time_since_epoch().count()
                   < (uint32_t) right.deletion_time().time_since_epoch().count() ? -1 : 1;
        }
    }
    return 0;
}

struct query_state {
    explicit query_state(const query::read_command& cmd, const std::vector<query::partition_range>& ranges)
            : cmd(cmd)
            , builder(cmd.slice)
            , limit(cmd.row_limit)
            , current_partition_range(ranges.begin())
            , range_end(ranges.end()){
    }
    const query::read_command& cmd;
    query::result::builder builder;
    uint32_t limit;
    bool range_empty;
    std::vector<query::partition_range>::const_iterator current_partition_range;
    std::vector<query::partition_range>::const_iterator range_end;
    mutation_reader reader;
    bool done() const {
        return !limit || current_partition_range == range_end;
    }
};

future<lw_shared_ptr<query::result>>
column_family::query(const query::read_command& cmd, const std::vector<query::partition_range>& partition_ranges) const {
    return do_with(query_state(cmd, partition_ranges), [this] (query_state& qs) {
        return do_until(std::bind(&query_state::done, &qs), [this, &qs] {
            auto&& range = *qs.current_partition_range++;
            qs.reader = make_reader(range);
            qs.range_empty = false;
            return do_until([&qs] { return !qs.limit || qs.range_empty; }, [this, &qs] {
                return qs.reader().then([this, &qs](mutation_opt mo) {
                    if (mo) {
                        auto p_builder = qs.builder.add_partition(mo->key());
                        mo->partition().query(*_schema, qs.cmd.slice, qs.limit, p_builder);
                        p_builder.finish();
                        qs.limit -= p_builder.row_count();
                    } else {
                        qs.range_empty = true;
                    }
                });
            });
        }).then([&qs] {
            return make_ready_future<lw_shared_ptr<query::result>>(
                    make_lw_shared<query::result>(qs.builder.build()));
        });
    });
}

future<lw_shared_ptr<query::result>>
database::query(const query::read_command& cmd, const std::vector<query::partition_range>& ranges) {
    static auto make_empty = [] {
        return make_ready_future<lw_shared_ptr<query::result>>(make_lw_shared(query::result()));
    };

    try {
        column_family& cf = find_column_family(cmd.cf_id);
        return cf.query(cmd, ranges);
    } catch (const no_such_column_family&) {
        // FIXME: load from sstables
        return make_empty();
    }
}

std::ostream& operator<<(std::ostream& out, const atomic_cell_or_collection& c) {
    return out << to_hex(c._data);
}

std::ostream& operator<<(std::ostream& os, const mutation& m) {
    fprint(os, "{mutation: schema %p key %s data ", m.schema().get(), m.decorated_key());
    os << m.partition() << "}";
    return os;
}

std::ostream& operator<<(std::ostream& out, const column_family& cf) {
    return fprint(out, "{column_family: %s/%s}", cf._schema->ks_name(), cf._schema->cf_name());
}

std::ostream& operator<<(std::ostream& out, const database& db) {
    out << "{\n";
    for (auto&& e : db._column_families) {
        auto&& cf = *e.second;
        out << "(" << e.first.to_sstring() << ", " << cf.schema()->cf_name() << ", " << cf.schema()->ks_name() << "): " << cf << "\n";
    }
    out << "}";
    return out;
}

future<> database::apply_in_memory(const frozen_mutation& m, const db::replay_position& rp) {
    try {
        auto& cf = find_column_family(m.column_family_id());
        cf.apply(m, rp, this);
    } catch (no_such_column_family&) {
        // TODO: log a warning
        // FIXME: load keyspace meta-data from storage
    }
    return make_ready_future<>();
}

future<> database::apply(const frozen_mutation& m) {
    // I'm doing a nullcheck here since the init code path for db etc
    // is a little in flux and commitlog is created only when db is
    // initied from datadir.
    if (_commitlog != nullptr) {
        auto uuid = m.column_family_id();
        bytes_view repr = m.representation();
        auto write_repr = [repr] (data_output& out) { out.write(repr.begin(), repr.end()); };
        return _commitlog->add_mutation(uuid, repr.size(), write_repr).then([&m, this](auto rp) {
            try {
                return this->apply_in_memory(m, rp);
            } catch (replay_position_reordered_exception&) {
                // expensive, but we're assuming this is super rare.
                // if we failed to apply the mutation due to future re-ordering
                // (which should be the ever only reason for rp mismatch in CF)
                // let's just try again, add the mutation to the CL once more,
                // and assume success in inevitable eventually.
                dblog.warn("replay_position reordering detected");
                return this->apply(m);
            }
        });
    }
    return apply_in_memory(m, db::replay_position());
}

keyspace::config
database::make_keyspace_config(const keyspace_metadata& ksm) const {
    // FIXME support multiple directories
    keyspace::config cfg;
    if (_cfg->data_file_directories().size() > 0) {
        cfg.datadir = sprint("%s/%s", _cfg->data_file_directories()[0], ksm.name());
        cfg.enable_disk_writes = ksm.durable_writes();
    } else {
        cfg.datadir = "";
        cfg.enable_disk_writes = false;
    }
    return cfg;
}

namespace db {

std::ostream& operator<<(std::ostream& os, db::consistency_level cl) {
    switch (cl) {
    case db::consistency_level::ANY: return os << "ANY";
    case db::consistency_level::ONE: return os << "ONE";
    case db::consistency_level::TWO: return os << "TWO";
    case db::consistency_level::THREE: return os << "THREE";
    case db::consistency_level::QUORUM: return os << "QUORUM";
    case db::consistency_level::ALL: return os << "ALL";
    case db::consistency_level::LOCAL_QUORUM: return os << "LOCAL_QUORUM";
    case db::consistency_level::EACH_QUORUM: return os << "EACH_QUORUM";
    case db::consistency_level::SERIAL: return os << "SERIAL";
    case db::consistency_level::LOCAL_SERIAL: return os << "LOCAL_SERIAL";
    case db::consistency_level::LOCAL_ONE: return os << "LOCAL";
    default: abort();
    }
}

}

std::ostream&
operator<<(std::ostream& os, const exploded_clustering_prefix& ecp) {
    // Can't pass to_hex() to transformed(), since it is overloaded, so wrap:
    auto enhex = [] (auto&& x) { return to_hex(x); };
    return fprint(os, "prefix{%s}", ::join(":", ecp._v | boost::adaptors::transformed(enhex)));
}

std::ostream&
operator<<(std::ostream& os, const atomic_cell_view& acv) {
    if (acv.is_live()) {
        return fprint(os, "atomic_cell{%s;ts=%d;expiry=%d,ttl=%d}",
            to_hex(acv.value()),
            acv.timestamp(),
            acv.is_live_and_has_ttl() ? acv.expiry().time_since_epoch().count() : -1,
            acv.is_live_and_has_ttl() ? acv.ttl().count() : 0);
    } else {
        return fprint(os, "atomic_cell{DEAD;ts=%d;deletion_time=%d}",
            acv.timestamp(), acv.deletion_time().time_since_epoch().count());
    }
}

std::ostream&
operator<<(std::ostream& os, const atomic_cell& ac) {
    return os << atomic_cell_view(ac);
}

future<>
database::stop() {
    return parallel_for_each(_column_families, [this] (auto& val_pair) {
        return val_pair.second->stop(this);
    });
}

const sstring& database::get_snitch_name() const {
    return _cfg->endpoint_snitch();
}
