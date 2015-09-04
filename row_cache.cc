/*
 * Copyright 2015 Cloudius Systems
 */

#include "row_cache.hh"
#include "core/memory.hh"
#include "core/do_with.hh"
#include "core/future-util.hh"
#include <seastar/core/scollectd.hh>
#include "memtable.hh"

static logging::logger logger("cache");

cache_tracker& global_cache_tracker() {
    static thread_local cache_tracker instance;
    return instance;
}

cache_tracker::cache_tracker() {
    setup_collectd();

    _region.make_evictable([this] {
        with_allocator(_region.allocator(), [this] {
            assert(!_lru.empty());
            _lru.pop_back_and_dispose(current_deleter<cache_entry>());
        });
    });
}

cache_tracker::~cache_tracker() {
    clear();
}

void
cache_tracker::setup_collectd() {
    _collectd_registrations = std::make_unique<scollectd::registrations>(scollectd::registrations({
        scollectd::add_polled_metric(scollectd::type_instance_id("cache"
                , scollectd::per_cpu_plugin_instance
                , "bytes", "used")
                , scollectd::make_typed(scollectd::data_type::GAUGE, [this] { return _region.occupancy().used_space(); })
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id("cache"
                , scollectd::per_cpu_plugin_instance
                , "bytes", "total")
                , scollectd::make_typed(scollectd::data_type::GAUGE, [this] { return _region.occupancy().total_space(); })
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id("cache"
                , scollectd::per_cpu_plugin_instance
                , "total_operations", "hits")
                , scollectd::make_typed(scollectd::data_type::DERIVE, _hits)
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id("cache"
                , scollectd::per_cpu_plugin_instance
                , "total_operations", "misses")
                , scollectd::make_typed(scollectd::data_type::DERIVE, _misses)
        ),
    }));
}

void cache_tracker::clear() {
    with_allocator(_region.allocator(), [this] {
        _lru.clear_and_dispose(current_deleter<cache_entry>());
    });
}

void cache_tracker::touch(cache_entry& e) {
    ++_hits;
    _lru.erase(_lru.iterator_to(e));
    _lru.push_front(e);
}

void cache_tracker::insert(cache_entry& entry) {
    ++_misses;
    _lru.push_front(entry);
}

allocation_strategy& cache_tracker::allocator() {
    return _region.allocator();
}

logalloc::region& cache_tracker::region() {
    return _region;
}

const logalloc::region& cache_tracker::region() const {
    return _region;
}

// Reader which populates the cache using data from the delegate.
class populating_reader final : public mutation_reader::impl {
    row_cache& _cache;
    mutation_reader _delegate;
public:
    populating_reader(row_cache& cache, mutation_reader delegate)
        : _cache(cache)
        , _delegate(std::move(delegate))
    { }

    virtual future<mutation_opt> operator()() override {
        return _delegate().then([this] (mutation_opt&& mo) {
            if (mo) {
                _cache.populate(*mo);
            }
            return std::move(mo);
        });
    }
};

mutation_reader
row_cache::make_reader(const query::partition_range& range) {
    if (range.is_singular()) {
        const query::ring_position& pos = range.start()->value();

        if (!pos.has_key()) {
            warn(unimplemented::cause::RANGE_QUERIES);
            return make_mutation_reader<populating_reader>(*this, _underlying(range));
        }

        const dht::decorated_key& dk = pos.as_decorated_key();
        auto i = _partitions.find(dk, cache_entry::compare(_schema));
        if (i != _partitions.end()) {
            cache_entry& e = *i;
            _tracker.touch(e);
            ++_stats.hits;
            logalloc::reclaim_lock lock(_tracker.region());
            return make_reader_returning(mutation(_schema, dk, e.partition()));
        } else {
            ++_stats.misses;
            return make_mutation_reader<populating_reader>(*this, _underlying(range));
        }
    }

    warn(unimplemented::cause::RANGE_QUERIES);
    return make_mutation_reader<populating_reader>(*this, _underlying(range));
}

row_cache::~row_cache() {
    with_allocator(_tracker.allocator(), [this] {
        _partitions.clear_and_dispose(current_deleter<cache_entry>());
    });
}

void row_cache::populate(const mutation& m) {
    with_allocator(_tracker.allocator(), [this, &m] {
        logalloc::reclaim_lock _(_tracker.region());
        auto i = _partitions.lower_bound(m.decorated_key(), cache_entry::compare(_schema));
        if (i == _partitions.end() || !i->key().equal(*_schema, m.decorated_key())) {
            cache_entry* entry = current_allocator().construct<cache_entry>(m.decorated_key(), m.partition());
            _tracker.insert(*entry);
            _partitions.insert(i, *entry);
        } else {
            _tracker.touch(*i);
            // We cache whole partitions right now, so if cache already has this partition,
            // it must be complete, so do nothing.
        }
    });
}

future<> row_cache::update(memtable& m, partition_presence_checker presence_checker) {
    _tracker.region().merge(m._region); // Now all data in memtable belongs to cache
    return repeat([this, &m, presence_checker = std::move(presence_checker)] () mutable {
        return with_allocator(_tracker.allocator(), [this, &m, &presence_checker] () {
            logalloc::reclaim_lock _(_tracker.region());
            unsigned quota = 30;
            auto i = m.partitions.begin();
            const schema& s = *m.schema();
            while (i != m.partitions.end() && quota-- != 0) {
                partition_entry& mem_e = *i;
                // FIXME: Optimize knowing we lookup in-order.
                auto cache_i = _partitions.lower_bound(mem_e.key(), cache_entry::compare(_schema));
                // If cache doesn't contain the entry we cannot insert it because the mutation may be incomplete.
                // FIXME: keep a bitmap indicating which sstables we do cover, so we don't have to
                //        search it.
                if (cache_i != _partitions.end() && cache_i->key().equal(s, mem_e.key())) {
                    cache_entry& entry = *cache_i;
                    _tracker.touch(entry);
                    entry.partition().apply(s, std::move(mem_e.partition()));
                } else if (presence_checker(mem_e.key().key()) == partition_presence_checker_result::definitely_doesnt_exist) {
                    cache_entry* entry = current_allocator().construct<cache_entry>(
                        std::move(mem_e.key()), std::move(mem_e.partition()));
                    _tracker.insert(*entry);
                    _partitions.insert(cache_i, *entry);
                }
                i = m.partitions.erase(i);
                current_allocator().destroy(&mem_e);
            }
            return make_ready_future<stop_iteration>(m.partitions.empty() ? stop_iteration::yes : stop_iteration::no);
        });
    }).finally([&m, this] {
        with_allocator(_tracker.allocator(), [&m] () {
            m.partitions.clear_and_dispose(current_deleter<partition_entry>());
        });
    });
}

void row_cache::touch(const dht::decorated_key& dk) {
    auto i = _partitions.find(dk, cache_entry::compare(_schema));
    if (i != _partitions.end()) {
        _tracker.touch(*i);
    }
}

row_cache::row_cache(schema_ptr s, mutation_source fallback_factory, cache_tracker& tracker)
    : _tracker(tracker)
    , _schema(std::move(s))
    , _partitions(cache_entry::compare(_schema))
    , _underlying(std::move(fallback_factory))
{ }

cache_entry::cache_entry(cache_entry&& o) noexcept
    : _key(std::move(o._key))
    , _p(std::move(o._p))
    , _lru_link()
    , _cache_link()
{
    {
        auto prev = o._lru_link.prev_;
        o._lru_link.unlink();
        cache_tracker::lru_type::node_algorithms::link_after(prev, _lru_link.this_ptr());
    }

    {
        using container_type = row_cache::partitions_type;
        container_type::node_algorithms::replace_node(o._cache_link.this_ptr(), _cache_link.this_ptr());
        container_type::node_algorithms::init(o._cache_link.this_ptr());
    }
}
