#pragma once

#include <stdint.h>
#include <memory>
#include "bytes.hh"
#include "utils/allocation_strategy.hh"

struct blob_storage {
    using size_type = uint32_t;

    blob_storage** backref;
    size_type size;
    bytes_view::value_type data[];

    blob_storage(blob_storage** backref, size_type size) noexcept
        : backref(backref)
        , size(size)
    {
        *backref = this;
    }

    blob_storage(blob_storage&& o) noexcept
        : backref(o.backref)
        , size(o.size)
    {
        *backref = this;
        memcpy(data, o.data, size);
    }
} __attribute__((packed));

// A variable-length contiguous sequence of bytes suitable
// to be allocated with LSA.
template<typename AllocationStrategy = standard_allocation_strategy>
class blob {
    blob_storage* _ptr;
public:
    using size_type = blob_storage::size_type;

    blob(bytes_view v) {
        new (AllocationStrategy().template alloc<blob_storage>(
            sizeof(blob_storage) + v.size())) blob_storage(&_ptr, v.size());
        memcpy(_ptr->data, v.data(), v.size());
    }

    blob(const blob& o) : blob(static_cast<bytes_view>(o)) {}

    blob(blob&& o) noexcept
        : _ptr(o._ptr)
    {
        o._ptr = nullptr;
        if (_ptr) {
            _ptr->backref = &_ptr;
        }
    }

    ~blob() {
        if (_ptr) {
            AllocationStrategy().template destroy(_ptr);
        }
    }

    explicit operator bytes_view() const {
        return { _ptr->data, _ptr->size };
    }
};
