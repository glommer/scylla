/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */
#pragma once

#include "types.hh"
#include "core/sstring.hh"
#include "schema.hh"

namespace cell_comparator {
sstring to_sstring(const schema& s);
bool check_compound(sstring comparator);
}
