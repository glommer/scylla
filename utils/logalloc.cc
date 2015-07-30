/*
 * Copyright 2015 Cloudius Systems
 */

#include <boost/range/algorithm/heap_algorithm.hpp>
#include <seastar/core/print.hh>

#include "utils/logalloc.hh"
#include "log.hh"

namespace logalloc {

logging::logger logger("lsa");

thread_local segment_pool shard_segment_pool;
static thread_local std::vector<compactible_region*> _compactibles;

#ifndef DEFAULT_ALLOCATOR

bool is_large(size_t object_size) {
    return object_size > segment::size/2;
}

segment_descriptor&
segment_pool::descriptor(const segment* seg) {
    static_assert(alignof(segment) == segment::size, "Segment must be aligned to its size");

    uintptr_t seg_addr = reinterpret_cast<uintptr_t>(seg);
    assert((seg_addr & ~((uintptr_t) segment::size - 1)) == seg_addr); // Relying on segment alignment

    uintptr_t index = (seg_addr - _segments_base) >> segment::size_shift;
    return _segments[index];
}

bool
segment_pool::is_lsa_managed(segment* seg) const {
    return shard_segment_pool.descriptor(seg)._lsa_managed;
}

segment*
segment_pool::new_segment() {
    auto seg = new (with_alignment(segment::size)) segment();

    // segment_pool::descriptor() is relying on segment alignment
    auto seg_addr = reinterpret_cast<uintptr_t>(seg);
    assert((seg_addr & ~((uintptr_t)segment::size - 1)) == seg_addr);

    ++_segments_in_use;
    segment_descriptor& desc = shard_segment_pool.descriptor(seg);
    desc._lsa_managed = true;
    desc._free_space = segment::size;
    return seg;
}

void segment_pool::free_segment(segment* seg) {
    logger.debug("Releasing segment {}", seg);
    descriptor(seg)._lsa_managed = false;
    delete seg;
    --_segments_in_use;
}

bool segment::is_empty() const {
    return shard_segment_pool.descriptor(this).is_empty();
}

#else

// LSA currently works only with seastar's allocator. When built with
// the default allocator, pass-through all allocations to it.

bool is_large(size_t object_size) {
    return true;
}

segment_descriptor&
segment_pool::descriptor(const segment* seg) {
    assert(0);
}

bool
segment_pool::is_lsa_managed(segment* seg) const {
    return false;
}

segment*
segment_pool::new_segment() {
    return new (with_alignment(segment::size)) segment();
}

void segment_pool::free_segment(segment* seg) {
    delete seg;
}

bool segment::is_empty() const {
    return true;
}

#endif

segment_pool::segment_pool()
    : _layout_listener([this] (memory::memory_layout layout) {
        if (!_segments.empty()) {
            assert(layout.start == _layout.start); // We don't handle changes of start address.
            assert(layout.end >= _layout.end); // We don't handle shrinking.
        }
        // We're relying here on the fact that segments are aligned to their size.
        _segments_base = align_up(layout.start, (uintptr_t) segment::size);
        _segments.resize((layout.end - _segments_base) / segment::size);
        _layout = layout;
    })
{ }

void segment::record_alloc(segment::size_type size) {
    shard_segment_pool.descriptor(this)._free_space -= size;
}

void segment::record_free(segment::size_type size) {
    shard_segment_pool.descriptor(this)._free_space += size;
}

occupancy_stats
segment::occupancy() const {
    return {
        ._free_space  = shard_segment_pool.descriptor(this)._free_space,
        ._total_space = segment::size
    };
}

void
segment::set_heap_handle(segment_heap::handle_type handle) {
    shard_segment_pool.descriptor(this)._heap_handle = handle;
}

const segment_heap::handle_type&
segment::heap_handle() {
    return shard_segment_pool.descriptor(this)._heap_handle;
}

uint64_t reclaim(uint64_t bytes) {
    auto segments_to_release = (bytes + segment::size - 1) >> segment::size_shift;

    auto cmp = [] (compactible_region* c1, compactible_region* c2) {
        return c1->occupancy() < c2->occupancy();
    };

    uint64_t in_use = shard_segment_pool.segments_in_use();

    auto target = in_use >= segments_to_release
                  ? (in_use - segments_to_release)
                  : in_use;

    logger.debug("Compacting, {} segments in use ({} B), trying to release {} ({} B).",
        in_use, in_use * segment::size, segments_to_release, segments_to_release * segment::size);

    boost::range::make_heap(_compactibles, cmp);
    
    if (logger.is_enabled(logging::log_level::debug)) {
        logger.debug("Pool occupancy:");
        for (compactible_region* pool : _compactibles) {
            logger.debug(" - {}", pool->occupancy());
        }
    }

    while (shard_segment_pool.segments_in_use() > target) {
        boost::range::pop_heap(_compactibles, cmp);
        compactible_region* pool = _compactibles.back();

        logger.debug("Compacting pool: {}", pool->occupancy());

        auto before = shard_segment_pool.segments_in_use();
        pool->compact();
        auto after = shard_segment_pool.segments_in_use();

        boost::range::push_heap(_compactibles, cmp);

        if (before == after) {
            logger.warn("Unable to release more segments, segments in use = {}", after);
            break;
        }
    }

    uint64_t nr_released = in_use - shard_segment_pool.segments_in_use();
    logger.debug("Released {} segments.", nr_released);

    return nr_released * segment::size;
}

uint64_t segment_pool::segments_in_use() const {
    return _segments_in_use;
}

void register_compactible_region(compactible_region& c) {
    _compactibles.emplace_back(&c);
}

void unregister_compactible_region(compactible_region& c) {
    _compactibles.erase(std::remove(_compactibles.begin(), _compactibles.end(), &c));
}

std::ostream& operator<<(std::ostream& out, const occupancy_stats& stats) {
    return out << sprint("%.2f%%, %d / %d [B]",
        stats.used_fraction() * 100, stats.used_space(), stats.total_space());
}

}
