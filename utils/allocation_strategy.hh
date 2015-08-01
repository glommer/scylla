/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <cstdlib>

//
// AllocationStrategy concept abstracts a stateless memory allocator which is
// capable of managing allocated objects.
//
// Allocated objects may be moved at deferring points, if for example a
// compacting allocator is used under the hood. It differs from the C++
// Allocator concept, which must not move storage and can be used to allocate
// opaque memory.
//
// Conforming types must provide methods as presented in the
// AllocationStrategy struct below.
//

// TODO: Use trait class for derived methods.
struct AllocationStrategy {
    // Reserves uninitialized storage for T of given size. The object must get
    // initialized by the caller immediately or the storage must be freed
    // using free().
    template<typename T>
    T* alloc(size_t);

    // Equivalent to alloc(sizeof(T)).
    template<typename T>
    T* alloc();

    // Releases storage for the object. Doesn't invoke object's destructor.
    template<typename T>
    void free(T*);

    // Allocates and constructs T.
    template<typename T, typename... Args>
    T* construct(Args&&... args);

    // Destroys T and releases its storage.
    template<typename T>
    void destroy(T*);
};

// AllocationStrategy using standard allocator
struct standard_allocation_strategy {
    template<typename T>
    T* alloc(size_t size) {
        return static_cast<T*>(std::malloc(size));
    }

    template<typename T>
    T* alloc() {
        return alloc<T>(sizeof(T));
    }

    template<typename T>
    void free(T* p) {
        std::free(p);
    }

    template<typename T, typename... Args>
    T* construct(Args&& ... args) {
        return new(alloc<T>()) T(std::forward<Args>(args)...);
    }

    template<typename T>
    void destroy(T* p) {
        p->~T();
        free(p);
    }
};
