/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */
#include "comparator.hh"

static constexpr auto _composite_str = "org.apache.cassandra.db.marshal.CompositeType";
static constexpr auto _collection_str = "org.apache.cassandra.db.marshal.ColumnToCollectionType";

namespace cell_comparator {

static sstring collection_name(const collection_type& t) {
    sstring collection_str(_collection_str);
    collection_str += "(00000000:" + t->name() + ")";
    return collection_str;
}

static sstring compound_name(const schema& s) {
    sstring compound(_composite_str);

    compound += "(";
    if (s.clustering_key_size()) {
        for (auto &t : s.clustering_key_columns()) {
            compound += t.type->name() + ",";
        }
    } else {
        compound += s.regular_column_name_type()->name() + ",";
    }

    if (s.has_collections()) {
        for (auto& t: s.regular_columns()) {
            if (t.type->is_collection()) {
                auto ct = static_pointer_cast<const collection_type_impl>(t.type);
                compound += collection_name(ct) + ",";
            }
        }
    }
    // last one will be a ',', just replace it.
    compound.back() = ')';
    return compound;
}

sstring to_sstring(const schema& s) {
    if (!s.is_compound()) {
        return s.regular_column_name_type()->name();
    } else {
        return compound_name(s);
    }
}

bool check_compound(sstring comparator) {
    static sstring compound(_composite_str);
    return comparator.compare(0, compound.size(), compound) == 0;
}
}
