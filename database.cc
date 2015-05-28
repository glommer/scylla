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
#include "frozen_mutation.hh"
#include "mutation_partition_applier.hh"
#include "core/do_with.hh"

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
{
    add_memtable();
}

// define in .cc, since sstable is forward-declared in .hh
column_family::column_family(column_family&& x) = default;

// define in .cc, since sstable is forward-declared in .hh
column_family::~column_family() {
}

memtable::const_mutation_partition_ptr
memtable::find_partition(const dht::decorated_key& key) const {
    auto i = partitions.find(key);
    // FIXME: remove copy if only one data source
    return i == partitions.end() ? const_mutation_partition_ptr() : std::make_unique<const mutation_partition>(i->second);
}

future<column_family::const_mutation_partition_ptr>
column_family::find_partition(const dht::decorated_key& key) const {
    // FIXME: optimize for 0 or 1 entries found case
    struct find_state {
        sstables::key key;
        mutation_partition ret;
        bool any = false;
        lw_shared_ptr<sstable_list> sstables; // protect from concurrent sstable removal
        find_state(const column_family& cf, const dht::decorated_key& key)
                : key(sstables::key::from_partition_key(*cf._schema, key._key))
                , ret(cf._schema)
                , sstables(cf._sstables) {
        }
    };
    find_state fs(*this, key);
    auto& ret = fs.ret;
    bool& any = fs.any;
    for (auto&& mtp : *_memtables) {
        auto mp = mtp->find_partition(key);
        if (mp) {
            ret.apply(*_schema, *mp);
            any = true;
        }
    }
    return do_with(std::move(fs), [this] (find_state& fs) {
        return parallel_for_each(*fs.sstables | boost::adaptors::map_values, [this, &fs] (lw_shared_ptr<sstables::sstable> sstable) {
            return sstable->read_row(_schema, fs.key).then([&fs] (mutation_opt mo) {
                if (mo) {
                    fs.ret.apply(*mo->schema(), mo->partition());
                    fs.any = true;
                }
            });
        }).then([&fs] {
            if (fs.any) {
                return make_ready_future<const_mutation_partition_ptr>(std::make_unique<mutation_partition>(std::move(fs.ret)));
            } else {
                return make_ready_future<const_mutation_partition_ptr>();
            }
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

// To be able to merge subscription<mutation> objects, we need
// the next reads mutation to be available for comparision, so
// we need a single-item queue.
//
// This class implements an internal loop for fetching the next
// mutation, controlled by the merge algorithm (which merges
// mutations from multiple loops).
class mutation_cursor {
private:
    queue<mutation_opt> _q{1};
    // data fed from data source
    subscription<mutation> _sub;
public:
    mutation_cursor(subscription<mutation>&& sub)
            : _sub(std::move(sub)) {
        _sub.start([this] (mutation_opt m) {
            return _q.push_eventually(std::move(m));
        });
        _sub.done().then_wrapped([this] (future<> ret) {
            _q.push_eventually(std::experimental::nullopt); // result ignored
            // FIXME: if we have an error here, how do we propagate it to the caller?
            // need to make _q hold expected<mutation_opt, std::exception_ptr>
        });
    }
    // Can't copy or move since the subscription's consume function
    // is bound to this.
    mutation_cursor(const mutation_cursor&) = delete;
    mutation_cursor(mutation_cursor&&) = delete;
    void operator=(const mutation_cursor&) = delete;
    void operator=(mutation_cursor&&) = delete;
    // Fetch the next mutation. Returns a disengaged optional on eof.
    future<mutation_opt> next() {
        return _q.pop_eventually();
    }
};

// Convert a memtable to a subscription<mutation>, which is what's expected by
// mutation_cursor (and provided by sstables).
subscription<mutation>
make_memtable_subscription(const memtable& mt) {
    auto st = make_lw_shared<stream<mutation>>();
    st->started().then([st, &mt] {
       return do_for_each(mt.all_partitions(), [&mt, st] (const memtable::partitions_type::value_type& v) mutable {
           mutation m(mt.schema(), v.first, v.second);
           return st->produce(std::move(m));
       });
    }).finally([st] {
        st->close();
    });
    return st->listen();
}

template <typename Func>
future<bool>
column_family::for_all_partitions(Func&& func) const {
    static_assert(std::is_same<bool, std::result_of_t<Func(const dht::decorated_key&, const mutation_partition&)>>::value,
                  "bad Func signature");
    // The plan here is to use a heap structure to sort incoming
    // mutations from many mutation_queues, grab them in turn, and
    // either merge them (if the keys are the same), or pass them
    // to func (if not).
    struct iteration_state {
        std::deque<mutation_cursor> tables;
        struct cursor_and_mutation {
            mutation m;
            mutation_cursor* mc;
        };
        std::vector<cursor_and_mutation> ptables;
        // comparison function for std::make_heap()/std::push_heap()
        static bool heap_compare(const cursor_and_mutation& a, const cursor_and_mutation& b) {
            auto&& s = a.m.schema();
            // order of comparison is inverted, because heaps produce greatest value first
            return b.m.decorated_key().less_compare(*s, a.m.decorated_key());
        }
        // mutation being merged from ptables
        std::experimental::optional<mutation> current;
        lw_shared_ptr<memtable_list> memtables;
        Func func;
        bool ok = true;
        bool done() const { return !ok || ptables.empty(); }
        iteration_state(const column_family& cf, Func&& func)
                : memtables(cf._memtables), func(std::move(func)) {
        }
    };
    iteration_state is(*this, std::move(func));
    // Can't use memtable::partitions_type::value_type due do constness
    return do_with(std::move(is), [this] (iteration_state& is) {
        for (auto mtp : *is.memtables) {
            if (!mtp->empty()) {
                is.tables.emplace_back(make_memtable_subscription(*mtp));
            }
        }
        // Get first element from mutation_cursor, if any, and set up ptables
        return parallel_for_each(is.tables, [this, &is] (mutation_cursor& mc) {
            return mc.next().then([this, &is, &mc] (mutation_opt&& m) {
                if (m) {
                    is.ptables.push_back({std::move(*m), &mc});
                }
            });
        }).then([&is, this] {
            boost::range::make_heap(is.ptables, &iteration_state::heap_compare);
            return do_until(std::bind(&iteration_state::done, &is), [&is, this] {
                if (!is.ptables.empty()) {
                    boost::range::pop_heap(is.ptables, &iteration_state::heap_compare);
                    auto& candidate_queue = is.ptables.back();
                    // Note: heap is now in invalid state, waiting for pop_back or push_heap,
                    //       see below.
                    mutation& m = candidate_queue.m;
                    // FIXME: handle different schemas
                    if (is.current && !is.current->decorated_key().equal(*m.schema(), m.decorated_key())) {
                        // key has changed, so emit accumukated mutation
                        is.func(is.current->decorated_key(), is.current->partition());
                        is.current = std::experimental::nullopt;
                    }
                    if (!is.current) {
                        is.current = std::move(m);
                    } else {
                        is.current->partition().apply(*m.schema(), m.partition());
                    }
                    return candidate_queue.mc->next().then([&is] (mutation_opt&& more) {
                        // Restore heap to valid state
                        if (!more) {
                            is.ptables.pop_back();
                        } else {
                            is.ptables.back().m = std::move(*more);
                            boost::range::push_heap(is.ptables, &iteration_state::heap_compare);
                        }
                    });
                } else {
                    return make_ready_future<>();
                }
            });
        }).then([this, &is] {
            auto& ok = is.ok;
            auto& current = is.current;
            auto& func = is.func;
            if (ok && current) {
                ok = func(std::move(current->decorated_key()), std::move(current->partition()));
                current = std::experimental::nullopt;
            }
            return make_ready_future<bool>(ok);
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

void
column_family::seal_active_memtable() {
    auto old = _memtables->back();
    if (old->empty()) {
        return;
    }
    add_memtable();
    // FIXME: better way of ensuring we don't attemt to
    //        overwrite an existing table.
    auto gen = _sstable_generation++ * smp::count + engine().cpu_id();
    sstring name = sprint("%s/%s-%s-%d-Data.db",
            _config.datadir,
            _schema->ks_name(), _schema->cf_name(),
            gen);
    if (!_config.enable_disk_writes) {
        return;
    }

    sstables::sstable newtab = sstables::sstable(_config.datadir, gen,
        sstables::sstable::version_types::la,
        sstables::sstable::format_types::big);

    do_with(std::move(newtab), [&old, name, this] (sstables::sstable& newtab) {
        // FIXME: write all components
        return newtab.write_components(*old).then([&newtab] {
            return newtab.load();
        }).then_wrapped([name, this, &newtab] (future<> ret) {
            try {
                ret.get();
                add_sstable(std::move(newtab));
                // FIXME: drop memtable
            } catch (std::exception& e) {
                dblog.error("failed to write sstable: {}", e.what());
            } catch (...) {
                dblog.error("failed to write sstable: unknown error");
            }
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
    db::system_keyspace::make(*this);
}

database::~database() {
}

future<> database::populate(sstring datadir) {
    return lister::scan_dir(datadir, directory_entry_type::directory, [this, datadir] (directory_entry de) {
        auto& ks_name = de.name;
        auto ksdir = datadir + "/" + de.name;

        auto i = _keyspaces.find(ks_name);
        if (i == _keyspaces.end()) {
            dblog.warn("Skipping undefined keyspace: {}", ks_name);
        } else {
            dblog.warn("Populating Keyspace {}", ks_name);
            return lister::scan_dir(ksdir, directory_entry_type::directory, [this, ksdir, ks_name] (directory_entry de) {
                auto comps = parse_fname(de.name);
                if (comps.size() != 2) {
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
    });
}

future<>
database::init_from_data_directory() {
    return populate(_cfg->data_file_directories()).then([this]() {
        return init_commitlog();
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
        // TODO: real config. Real logging.
        // Right now we just set this up to use a single segment
        // and discard everything left on disk (not filling it)
        // with no hope of actually retrieving stuff...
        cfg.commitlog_total_space_in_mb = 1;

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
    return make_directory(db.local()._cfg->data_file_directories() + "/" + ksm->name()).then([ksm, &db] {
        return db.invoke_on_all([ksm] (database& db) {
            auto cfg = db.make_keyspace_config(*ksm);
            keyspace ks(ksm, cfg);
            ks.create_replication_strategy();
            db.add_keyspace(ksm->name(), std::move(ks));
        });
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

void database::add_column_family(const utils::UUID& uuid, column_family&& cf) {
    if (_keyspaces.count(cf.schema()->ks_name()) == 0) {
        throw std::invalid_argument("Keyspace " + cf.schema()->ks_name() + " not defined");
    }
    if (_column_families.count(uuid) != 0) {
        throw std::invalid_argument("UUID " + uuid.to_sstring() + " already mapped");
    }
    auto kscf = std::make_pair(cf.schema()->ks_name(), cf.schema()->cf_name());
    if (_ks_cf_to_uuid.count(kscf) != 0) {
        throw std::invalid_argument("Column family " + cf.schema()->cf_name() + " exists");
    }
    _column_families.emplace(uuid, std::move(cf));
    _ks_cf_to_uuid.emplace(std::move(kscf), uuid);
}

void database::add_column_family(column_family&& cf) {
    auto id = cf.schema()->id();
    add_column_family(id, std::move(cf));
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
        return _column_families.at(uuid);
    } catch (...) {
        std::throw_with_nested(no_such_column_family(uuid.to_sstring()));
    }
}

const column_family& database::find_column_family(const utils::UUID& uuid) const throw (no_such_column_family) {
    try {
        return _column_families.at(uuid);
    } catch (...) {
        std::throw_with_nested(no_such_column_family(uuid.to_sstring()));
    }
}

void
keyspace::create_replication_strategy() {
    static thread_local locator::token_metadata tm;
    static locator::simple_snitch snitch;
    static std::unordered_map<sstring, sstring> options = {{"replication_factor", "3"}};
    auto d2t = [](double d) {
        unsigned long l = net::hton(static_cast<unsigned long>(d*(std::numeric_limits<unsigned long>::max())));
        std::array<int8_t, 8> a;
        memcpy(a.data(), &l, 8);
        return a;
    };
    tm.update_normal_token({dht::token::kind::key, {d2t(0).data(), 8}}, to_sstring("127.0.0.1"));
    tm.update_normal_token({dht::token::kind::key, {d2t(1.0/4).data(), 8}}, to_sstring("127.0.0.2"));
    tm.update_normal_token({dht::token::kind::key, {d2t(2.0/4).data(), 8}}, to_sstring("127.0.0.3"));
    tm.update_normal_token({dht::token::kind::key, {d2t(3.0/4).data(), 8}}, to_sstring("127.0.0.4"));
    _replication_strategy = locator::abstract_replication_strategy::create_replication_strategy(_metadata->name(), _metadata->strategy_name(), tm, snitch, options);
}

locator::abstract_replication_strategy&
keyspace::get_replication_strategy() {
    return *_replication_strategy;
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

keyspace&
database::find_or_create_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm) {
    auto i = _keyspaces.find(ksm->name());
    if (i != _keyspaces.end()) {
        return i->second;
    }
    keyspace ks(ksm, std::move(make_keyspace_config(*ksm)));
    ks.create_replication_strategy();
    return _keyspaces.emplace(ksm->name(), std::move(ks)).first->second;
}

void
memtable::apply(const mutation& m) {
    mutation_partition& p = find_or_create_partition(m.decorated_key());
    p.apply(*_schema, m.partition());
}

void
memtable::apply(const frozen_mutation& m) {
    mutation_partition& p = find_or_create_partition_slow(m.key(*_schema));
    p.apply(*_schema, m.partition());
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
    explicit query_state(const query::read_command& cmd)
            : cmd(cmd)
            , builder(cmd.slice)
            , limit(cmd.row_limit)
            , current_partition_range(cmd.partition_ranges.begin()) {
    }
    const query::read_command& cmd;
    query::result::builder builder;
    uint32_t limit;
    std::vector<query::partition_range>::const_iterator current_partition_range;
    bool done() const {
        return !limit || current_partition_range == cmd.partition_ranges.end();
    }
};

future<lw_shared_ptr<query::result>>
column_family::query(const query::read_command& cmd) const {
    return do_with(query_state(cmd), [this] (query_state& qs) {
        return do_until(std::bind(&query_state::done, &qs), [this, &qs] {
            auto& cmd = qs.cmd;
            auto& builder = qs.builder;
            auto& limit = qs.limit;
            auto&& range = *qs.current_partition_range++;
            if (range.is_singular()) {
                auto& key = range.start_value();
                return find_partition_slow(key).then([this, &qs, &key] (auto partition) {
                    auto& cmd = qs.cmd;
                    auto& builder = qs.builder;
                    auto& limit = qs.limit;
                    if (!partition) {
                        return;
                    }
                    auto p_builder = builder.add_partition(key);
                    partition->query(*_schema, cmd.slice, limit, p_builder);
                    p_builder.finish();
                    limit -= p_builder.row_count();
                });
            } else if (range.is_full()) {
                return for_all_partitions([&] (const dht::decorated_key& dk, const mutation_partition& partition) {
                    auto p_builder = builder.add_partition(dk._key);
                    partition.query(*_schema, cmd.slice, limit, p_builder);
                    p_builder.finish();
                    limit -= p_builder.row_count();
                    if (limit == 0) {
                        return false;
                    }
                    return true;
                }).discard_result();
            } else {
                fail(unimplemented::cause::RANGE_QUERIES);
            }
            return make_ready_future<>();
        }).then([&qs] {
            return make_ready_future<lw_shared_ptr<query::result>>(
                    make_lw_shared<query::result>(qs.builder.build()));
        });
    });
}

future<lw_shared_ptr<query::result>>
database::query(const query::read_command& cmd) {
    static auto make_empty = [] {
        return make_ready_future<lw_shared_ptr<query::result>>(make_lw_shared(query::result()));
    };

    try {
        column_family& cf = find_column_family(cmd.cf_id);
        return cf.query(cmd);
    } catch (...) {
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
        auto&& cf = e.second;
        out << "(" << e.first.to_sstring() << ", " << cf.schema()->cf_name() << ", " << cf.schema()->ks_name() << "): " << cf << "\n";
    }
    out << "}";
    return out;
}

future<> database::apply_in_memory(const frozen_mutation& m) {
    try {
        auto& cf = find_column_family(m.column_family_id());
        cf.apply(m);
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
            return this->apply_in_memory(m);
        });
    }
    return apply_in_memory(m);
}

keyspace::config
database::make_keyspace_config(const keyspace_metadata& ksm) const {
    keyspace::config cfg;
    cfg.datadir = sprint("%s/%s", _cfg->data_file_directories(), ksm.name());
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
    return make_ready_future<>();
}
