#pragma once

#include "core/sstring.hh"

namespace version {
inline const sstring& current() {
    static sstring v = "3";
    return v;
}

inline const sstring& release() {
    static sstring v = "SeastarDB v0.1";
    return v;
}
}
