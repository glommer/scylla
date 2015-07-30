/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include <bits/unique_ptr.h>
#include <boost/heap/binomial_heap.hpp>
#include <seastar/core/memory.hh>
#include <seastar/core/align.hh>

#include "utils/typeset.hh"
#include "log.hh"

namespace logalloc {

struct segment;
struct segment_occupancy_descending_less_compare;
struct occupancy_stats;
class compactible_region;

void register_compactible_region(compactible_region&);
void unregister_compactible_region(compactible_region&);

//
// Tries to reclaim given amount of bytes from all compatible and evictable
// regions. Returns the number of bytes actually reclaimed. That value may be
// smaller than the requested amount when evictable pools are empty and
// compactible pools can't compact any more.
//
// Invalidates references to objects in all compactible and evictable regions.
//
uint64_t reclaim(uint64_t bytes);

// Returns aggregate statistics for all pools.
occupancy_stats occupancy();

bool is_large(size_t object_size);

extern logging::logger logger;

constexpr float max_occupancy_for_compaction = 0.9; // FIXME: make configurable

// RegionManaged must be movable. It can be moved by log allocator across deferring points.

struct segment_occupancy_descending_less_compare {
    inline bool operator()(segment* s1, segment* s2) const;
};

using segment_heap = boost::heap::binomial_heap<
    segment*, boost::heap::compare<segment_occupancy_descending_less_compare>>;

struct segment {
    using size_type = uint16_t;
    static constexpr int size_shift = 15; // 32K
    static constexpr size_type size = 1 << size_shift;

    uint8_t data[size];

    template<typename T>
    const T* at(size_t offset) const {
        return reinterpret_cast<const T*>(data + offset);
    }

    template<typename T>
    T* at(size_t offset) {
        return reinterpret_cast<T*>(data + offset);
    }

    bool is_empty() const;
    void record_alloc(size_type size);
    void record_free(size_type size);
    occupancy_stats occupancy() const;

    void set_heap_handle(segment_heap::handle_type);
    const segment_heap::handle_type& heap_handle();
} __attribute__((__aligned__(segment::size)));

// Monoid representing pool occupancy statistics.
// Naturally ordered so that sparser pools come fist.
// All sizes in bytes.
struct occupancy_stats {
    size_t _free_space;
    size_t _total_space;

    bool operator<(const occupancy_stats& other) const {
        return used_fraction() < other.used_fraction();
    }

    occupancy_stats& operator+=(const occupancy_stats& other) {
        _total_space += other._total_space;
        _free_space += other._free_space;
        return *this;
    }

    occupancy_stats& operator-=(const occupancy_stats& other) {
        _total_space -= other._total_space;
        _free_space -= other._free_space;
        return *this;
    }

    size_t used_space() const {
        return _total_space - _free_space;
    }

    size_t free_space() const {
        return _free_space;
    }

    size_t total_space() const {
        return _total_space;
    }

    float used_fraction() const {
        return _total_space ? float(used_space()) / total_space() : 0;
    }

    friend std::ostream& operator<<(std::ostream&, const occupancy_stats&);
};

inline bool
segment_occupancy_descending_less_compare::operator()(segment* s1, segment* s2) const {
    return s2->occupancy() < s1->occupancy();
}

struct segment_descriptor {
    bool _lsa_managed;
    segment::size_type _free_space;
    segment_heap::handle_type _heap_handle;

    segment_descriptor()
        : _lsa_managed(false)
    { }

    bool is_empty() const {
        return _free_space == segment::size;
    }
};

class segment_pool {
    std::vector<segment_descriptor> _segments;
    uintptr_t _segments_base; // The address of the first segment
    uint64_t _segments_in_use{};
    memory::memory_layout _layout;
    memory::memory_layout_change_listener _layout_listener;
public:
    segment_pool();
    segment* new_segment();
    segment_descriptor& descriptor(const segment*);
    void free_segment(segment*);
    uint64_t segments_in_use() const;
    bool is_lsa_managed(segment*) const;
};

extern thread_local segment_pool shard_segment_pool;

// 10xxxxxx if padding present, and bits 0-5 hold the amount of padding bytes (not including this one)
// 11xxxxxx if padding present, and the count of padding bytes is encoded in the next field of type size_type
// 00000000 if the object is dead
// 00000001 if the object is live
// 00000010 for end-of-segment marker
struct obj_flags {
    static constexpr uint8_t live = 0x01;
    static constexpr uint8_t eos = 0x02;
    static constexpr uint8_t padding = 0x80;

    uint8_t _value;

    obj_flags(uint8_t value)
        : _value(value)
    { }

    static obj_flags make_end_of_segment() {
        return { eos };
    }

    static obj_flags make_live() {
        return { live };
    }

    static obj_flags make_dead() {
        return { 0 };
    }

    bool is_padding() const {
        return _value & padding;
    }

    bool is_live() const {
        return _value & live;
    }

    bool is_end_of_segment() const {
        return _value == eos;
    }
} __attribute__((packed));

template<typename TypeSet>
class object_descriptor {
private:
    obj_flags _flags;
    typename TypeSet::id_type _type;
    segment::size_type _size;
public:
    object_descriptor(typename TypeSet::id_type type, segment::size_type size)
        : _flags(obj_flags::make_live())
        , _type(type)
        , _size(size)
    { }

    void mark_dead() {
        _flags = obj_flags::make_dead();
    }

    typename TypeSet::id_type type() const {
        return _type;
    }

    segment::size_type size() const {
        return _size;
    }

    obj_flags flags() const {
        return _flags;
    }

    bool is_live() const {
        return _flags.is_live();
    }

    bool is_end_of_segment() const {
        return _flags.is_end_of_segment();
    }

    friend std::ostream& operator<<(std::ostream& out, const object_descriptor& desc) {
        return out << sprint("{flags = %x, type=%d, size=%d}", (int)desc._flags._value, (int)desc._type, desc._size);
    }
} __attribute__((packed));

struct padding_descriptor {
    obj_flags _flags;
    segment::size_type padding;

    void set(segment::size_type padding_) {
        if (padding < 0x40) {
            _flags._value = (uint8_t)padding_ | 0x80;
        } else {
            _flags._value = 0xc0;
            padding = padding_;
        }
    }

    segment::size_type get() const {
        if ((_flags._value & 0xc0) == 0xc0) {
            return padding;
        }
        return _flags._value & 0x3f;
    }
} __attribute__((packed));

class compactible_region {
public:
    virtual ~compactible_region() {}

    virtual const occupancy_stats& occupancy() = 0;

    // Performs incremental compaction inside the pool, trying to release one segment
    // into the shared pool. May return without achieving the goal, which indicates
    // that this region can't be compacted any more.
    virtual void compact() = 0;
};

//
// Region is used to allocate objects from given TypeSet.
//
// Objects allocated using this region are said to be owned by this region.
// Objects must be freed only using the region which owns them. Ownership can
// be transferred across regions using the merge() method. Region must be live
// as long as it owns any objects.
//
// Each region has separate memory accounting and can be compacted
// independently from other regions. To reclaim memory from all regions use
// logalloc::reclaim(). Region is automatically added to the set of
// compactible regions when constructed.
//
template<typename TypeSet>
class region final {
    using object_descriptor_type = object_descriptor<TypeSet>;
private:
    struct compactible_adapter : public compactible_region {
        region<TypeSet>& _self;
        compactible_adapter(region<TypeSet>& self) : _self(self) {}
        virtual const occupancy_stats& occupancy() override { return _self._occupancy; }
        virtual void compact() override { _self.compact(); }
    };
    struct mover {
        region<TypeSet>& self;
        object_descriptor_type* desc;

        template <typename T>
        void operator()(T* obj) {
            static_assert(std::is_nothrow_move_constructible<T>::value, "Throwing move constructor not supported");
            new (self.alloc_small<T>(desc->size())) T (std::move(*obj));
            obj->~T();
        }
    };
private:
    // Active segment is used to satisfy current allocation. It's not present in the _segments heap and
    // is not included in pool's occupancy statistics.
    segment* _active;
    size_t _active_offset;

    std::unique_ptr<compactible_adapter> _compactible_adapter;
    segment_heap _segments;
    occupancy_stats _occupancy{};
private:
    template <typename T>
    T* alloc_large(size_t size) {
        return static_cast<T*>(::aligned_alloc(alignof(T), size));
    }

    template <typename T>
    void free_large(T* obj) {
        return ::free(obj);
    }

    template <typename T>
    T* alloc_small(segment::size_type size) {
        static_assert(segment::size > alignof(T), "Alignment must be smaller than segment size");
        static_assert(std::is_move_constructible<T>::value, "T must be move-constructible for compaction.");

        size_t obj_offset = align_up(_active_offset + sizeof(object_descriptor_type), alignof(T));
        if (obj_offset + size > segment::size) {
            close_and_open();
            return alloc_small<T>(size);
        }

        auto descriptor_offset = obj_offset - sizeof(object_descriptor_type);
        auto padding = descriptor_offset - _active_offset;

        if (padding) {
            _active->at<padding_descriptor>(_active_offset)->set(padding);
            //std::cout << "** padding at " << _active << " off " << _active_offset << "\n";
        }

        object_descriptor_type* desc = _active->at<object_descriptor_type>(descriptor_offset);
        new (desc) object_descriptor_type(TypeSet::template id<T>(), size);

        T* obj = _active->at<T>(obj_offset);
        _active_offset = obj_offset + size;

        //std::cout << "alloc " << obj << " desc=" << *desc << "\n";

        // FIXME: We don't count padding, because we don't have
        // an easy way to determine it in free().
        _active->record_alloc(size + sizeof(object_descriptor_type));
        return obj;
    }

    template<typename Func>
    void for_each_live(segment* seg, Func&& func) {
        static_assert(std::is_same<void, std::result_of_t<Func(object_descriptor_type*, void*)>>::value, "bad Func signature");

        size_t offset = 0;
        //std::cout << "iterating " << seg << "\n";
        while (offset < segment::size) {
            //std::cout << "off " << offset << "\n";
            object_descriptor_type* desc = seg->at<object_descriptor_type>(offset);
            if (desc->flags().is_padding()) {
                segment::size_type pad = seg->at<padding_descriptor>(offset)->get();
                //std::cout << "padding " << pad << "\n";
                offset += pad;
                desc = seg->at<object_descriptor_type>(offset);
            }
            //std::cout << "off " << offset << ", desc = " << *desc << "\n";
            if (desc->is_end_of_segment()) {
                break;
            }
            if (desc->is_live()) {
                func(desc, reinterpret_cast<void*>(seg->data + offset + sizeof(object_descriptor_type)));
            }
            offset += sizeof(object_descriptor_type) + desc->size();
        }
        //std::cout << "done\n";
    }

    void close_active() {
        if (_active_offset < segment::size) {
            *_active->at<obj_flags>(_active_offset) = obj_flags::make_end_of_segment();
        }
        logger.debug("Closing segment {}, used={}, waste={}B", _active, _active->occupancy(), segment::size - _active_offset);
        _occupancy += _active->occupancy();
        auto handle = _segments.push(_active);
        _active->set_heap_handle(handle);
        _active = nullptr;
    }

    bool is_compactible() {
        if (_occupancy.free_space() < 2 * segment::size) {
            return false;
        }
        return _occupancy.used_fraction() < max_occupancy_for_compaction;
    }

    void compact(segment* seg) {
        for_each_live(seg, [this] (object_descriptor_type* desc, void* obj) {
            TypeSet::template dispatch(desc->type(), mover{*this, desc}, obj);
        });

        shard_segment_pool.free_segment(seg);
    }

    void compact() {
        if (!is_compactible()) {
            return;
        }

        auto in_use = shard_segment_pool.segments_in_use();

        // FIXME: is this really guaranteed to stop?
        while (shard_segment_pool.segments_in_use() >= in_use) {
            segment* seg = _segments.top();
            logger.debug("Compacting segment {}, {}", seg, seg->occupancy());
            _segments.pop();
            _occupancy -= seg->occupancy();
            compact(seg);
        }
    }

    void close_and_open() {
        segment* new_active = shard_segment_pool.new_segment();
        close_active();
        _active = new_active;
        _active_offset = 0;
    }
public:
    region()
        : _active{shard_segment_pool.new_segment()}
        , _active_offset{0}
        , _compactible_adapter{std::make_unique<compactible_adapter>(*this)}
    {
        register_compactible_region(*_compactible_adapter);
    }

    region(region&& other)
        : _active(other._active)
        , _active_offset(other._active_offset)
        , _compactible_adapter(std::move(other._compactible_adapter))
    {
        other._active = nullptr;
        _compactible_adapter->_self = *this;
    }

    ~region() {
        unregister_compactible_region(*_compactible_adapter);
        assert(_segments.empty());
        if (_active) {
            assert(_active->is_empty());
            shard_segment_pool.free_segment(_active);
        }
    }

    const occupancy_stats& occupancy() const {
        return _occupancy;
    }

    // Like alloc() but also constructs the object.
    template <typename ManagedObject, typename... Args>
    ManagedObject* construct(Args&&... args) {
        return new (alloc<ManagedObject>(sizeof(ManagedObject)))
            ManagedObject(std::forward<Args>(args)...);
    }

    //
    // Allocates space for a new ManagedObject. The caller must construct the
    // object before compaction runs. "size" is the amount of space to reserve
    // in bytes. It can be larger than MangedObjects's size.
    //
    // Due to compaction, ManagedObject must be a movable type. Compaction is
    // triggered asynchronously in a separate fiber. Because of that, the
    // pointer returned by alloc() is guaranteed to be valid only until the
    // next deferring point. It maybe invalidated sooner only if compaction is
    // explicitly invoked synchronously form user's code. See ::make_managed()
    // on how to make a reference which is permanently valid.
    //
    // Note: When object is used as an element inside intrusive containers,
    // typically no extra measures need to be taken for reference tracking, if
    // the link member is movable. When object is moved, the member hook will
    // be moved too and it should take care of updating any back-references.
    // The user must be aware though that any iterators into such container
    // may be invalidated across deferring points.
    //
    // Doesn't invalidate references to allocated objects.
    //
    template <typename ManagedObject>
    ManagedObject* alloc(size_t size) {
        assert(size >= sizeof(ManagedObject));
        if (is_large(size)) {
            return alloc_large<ManagedObject>(size);
        } else {
            return alloc_small<ManagedObject>(size);
        }
    }

    // Releases storage allocated for given object.
    // The object must be owned by this region.
    // Doesn't invalidate references to allocated objects.
    template <typename ManagedObject>
    void free(ManagedObject* obj) {
        auto obj_addr = reinterpret_cast<uintptr_t>(obj);
        auto segment_addr = align_down(obj_addr, (uintptr_t)segment::size);
        segment* seg = reinterpret_cast<segment*>(segment_addr);

        if (!shard_segment_pool.is_lsa_managed(seg)) {
            free_large(obj);
            return;
        }

        auto desc = reinterpret_cast<object_descriptor_type*>(obj_addr - sizeof(object_descriptor_type));
        desc->mark_dead();

        //std::cout << "free " << obj << " desc=" << *desc << "\n";

        if (seg != _active) {
            _occupancy -= seg->occupancy();
        }

        seg->record_free(desc->size() + sizeof(object_descriptor_type));

        if (seg != _active) {
            if (seg->is_empty()) {
                _segments.erase(seg->heap_handle());
                shard_segment_pool.free_segment(seg);
            } else {
                _occupancy += seg->occupancy();
                _segments.decrease(seg->heap_handle());
            }
        }
    }

    // Destroys given object and releases it's storage.
    // The object must be owned by this region.
    // Doesn't invalidate references to allocated objects.
    template <typename ManagedObject>
    void destroy(ManagedObject* obj) {
        obj->~ManagedObject();
        free(obj);
    }

    // Merges another region into this region. The other region is left empty.
    // Doesn't invalidate references to allocated objects.
    void merge(region& other) {
        if (_active->is_empty()) {
            _active = other._active;
            other._active = nullptr;
            _active_offset = other._active_offset;
        } else {
            other.close_active();
        }

        _segments.merge(other._segments);

        _occupancy += other._occupancy;
        other._occupancy = {};
    }

    // Compacts everything. Mainly for testing.
    // Invalidates references to allocated objects.
    void full_compaction() {
        logger.debug("Performing full compaction, used={}, active={}", _occupancy, _active->occupancy());
        close_and_open();
        segment_heap all;
        std::swap(all, _segments);
        _occupancy = {};
        while (!all.empty()) {
            segment* seg = all.top();
            all.pop();
            compact(seg);
        }
        logger.debug("Done, used={}, active={}", _occupancy, _active->occupancy());
    }
};

//
// region_allocator<> implements AllocationStrategy concept, serving
// allocations from the current LSA region of given type. As a stateless
// allocator it relies on the target region being set in the thread-local
// context using with_region(). The region must be set whenever
// region_allocator<> strategy is invoked, which includes both object
// construction and destruction.
//
// Example:
//
//    using region_type = region<typeset<blob_storage>>;
//    using allocator = region_allocator<region_type>;
//
//    region_type reg;
//
//    with_region(reg, [] {
//        blob<allocator> b(bytes("asd"));
//    });
//

template<typename Region>
inline
Region*& current_region() {
    static __thread Region* current = nullptr;
    return current;
}

template<typename Region>
class lock {
public:
    lock(Region& alloc) {
        assert(!current_region<Region>());
        current_region<Region>() = &alloc;
    }

    ~lock() {
        current_region<Region>() = nullptr;
    }
};

// AllocationStrategy which allocates from an LSA region
template<typename Region>
struct region_allocator {
    using lock_type = lock<Region>;

    template <typename T>
    T* alloc(size_t size) {
        return current_region<Region>()->template alloc<T>(size);
    }

    template <typename T>
    T* alloc() {
        return alloc<T>(sizeof(T));
    }

    template <typename T>
    void free(T* p) noexcept {
        current_region<Region>()->template free(p);
    }

    template <typename T, typename... Args>
    T* construct(Args&&... args) {
        return new (alloc<T>()) T(std::forward<Args>(args)...);
    }

    template <typename T>
    void destroy(T* p) noexcept {
        current_region<Region>()->template destroy(p);
    }
};

template<typename Region, typename Func>
inline
auto with_region(Region& alloc, Func&& func) {
    lock<Region> lock(alloc);
    return func();
}

}
