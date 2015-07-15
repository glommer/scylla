/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */
#pragma once

#include "types.hh"
#include "core/shared_ptr.hh"
#include "core/sstring.hh"

#include <memory>

enum class is_compound { no, yes };
enum class is_sparse { no, yes };

struct i_cell_name {
    virtual sstring to_sstring() const = 0;
    virtual bool dense() const = 0;
    virtual bool compound() const = 0;
};
using cell_name_type_ptr = std::shared_ptr<i_cell_name>;

static constexpr auto _composite_str = "org.apache.cassandra.db.marshal.CompositeType";
static constexpr auto _collection_str = "org.apache.cassandra.db.marshal.ColumnToCollectionType";

template <is_compound Compound, is_sparse Sparse>
class cell_name_type: public i_cell_name {
private:
    std::vector<data_type> _types;
    std::vector<collection_type> _collection_types;
public:
    cell_name_type(std::vector<data_type>&& t, std::vector<collection_type>&& ct = {}) : _types(std::move(t)), _collection_types(std::move(ct)) {}
    cell_name_type(const std::vector<data_type>& t, const std::vector<collection_type>& ct = {}) : _types(t), _collection_types(ct) {}

    virtual sstring to_sstring() const {
        if (Compound == is_compound::no) {
            return _types[0]->name();
        } else {
            return compound_name();
        }
    }

    virtual bool dense() const {
        return Sparse != is_sparse::yes;
    };

    virtual bool compound() const {
        return Compound == is_compound::yes;
    };
private:
    sstring collection_name(const collection_type& t) const {
        sstring collection_str(_collection_str);
        collection_str += "(" + t->hashed_name() + ")";
        return collection_str;
    }

    sstring compound_name() const {
        sstring compound(_composite_str);

        compound += "(";
        for (auto &t : _types) {
            compound += t->name() + ",";
        }

        for (auto& t: _collection_types) {
            compound += collection_name(t) + ",";
        }
        // last one will be a ',', just replace it.
        compound.back() = ')';
        return compound;
    }
};

inline std::vector<data_type> parse_compound(sstring name) {
    static const sstring composite_str(_composite_str);

    std::vector<data_type> t;
    if (name.compare(0, composite_str.size(), composite_str) == 0) {
        size_t pos = composite_str.size();
        assert(name[name.size()] == ')');
        assert(name[pos] == '(');

        while (pos++ < name.size()) {
            auto next = name.find(',');
            sstring comp_name = name.substr(pos, next);
            t.push_back(abstract_type::parse_type(comp_name));
            pos = next;
        }
    } else {
        t.push_back(abstract_type::parse_type(name));
    }

    return t;
}

using compound_sparse_cell_name_type = cell_name_type<is_compound::yes, is_sparse::yes>;
using simple_sparse_cell_name_type = cell_name_type<is_compound::no, is_sparse::yes>;
using compound_dense_cell_name_type = cell_name_type<is_compound::yes, is_sparse::no>;
using simple_dense_cell_name_type = cell_name_type<is_compound::no, is_sparse::no>;
using compound_data_type = std::experimental::optional<std::vector<data_type>>;

inline compound_data_type make_raw_abstract_type(compound_data_type comparator, compound_data_type sub_comparator) {
    if (!sub_comparator) {
        return std::move(comparator);
    }

    compound_data_type::value_type ret;
    for (auto&& t: *comparator) {
        ret.push_back(std::move(t));
    }

    for (auto&& t: *sub_comparator) {
        ret.push_back(std::move(t));
    }
    return ret;
}
