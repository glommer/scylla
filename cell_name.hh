/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */
#pragma once

#include "types.hh"
#include "core/shared_ptr.hh"
#include "core/sstring.hh"

class cell_name_type;
using cell_name_type_ptr = ::shared_ptr<cell_name_type>;

static constexpr auto _composite_str = "org.apache.cassandra.db.marshal.CompositeType";
static constexpr auto _collection_str = "org.apache.cassandra.db.marshal.ColumnToCollectionType";

class cell_name_type : public enable_shared_from_this<cell_name_type> {
private:
    bool _is_compound;
    bool _is_dense;
    std::vector<data_type> _types;
    std::vector<collection_type> _collection_types;

protected:
    cell_name_type(bool composite, bool dense, std::vector<data_type>&& t, std::vector<collection_type>&& ct = {}) : _types(std::move(t)), _collection_types(std::move(ct)) {}
    cell_name_type(bool composite, bool dense, const std::vector<data_type>& t, const std::vector<collection_type>& ct = {}) : _types(t), _collection_types(ct) {}
public:
    sstring to_sstring() const {
        if (!_is_compound) {
            return _types[0]->name();
        } else {
            return compound_name();
        }
    }

    bool is_dense() const {
        return _is_dense;
    }

    bool is_compound() const {
        return _is_compound;
    }

    cell_name_type_ptr accumulate(cell_name_type_ptr&& c2_ptr) {
        if (!c2_ptr) {
            return shared_from_this();
        }

        // If it was not composite, it should now become
        _is_compound = true;
        for (auto&& t: c2_ptr->_types) {
            _types.push_back(std::move(t));
        }

        for (auto&& t: c2_ptr->_collection_types) {
            _collection_types.push_back(std::move(t));
        }
        return shared_from_this();
    }

    cell_name_type_ptr update_dense(bool dense) {
        _is_dense = dense;
        return shared_from_this();
    }

private:
    sstring collection_name(const collection_type& t) const {
        sstring collection_str(_collection_str);
        collection_str += "(00000000:" + t->name() + ")";
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

struct compound_sparse_cell_name_type : public cell_name_type {
    compound_sparse_cell_name_type(std::vector<data_type>&& t, std::vector<collection_type>&& ct = {}) : cell_name_type(true, false, std::move(t), std::move(ct)) {}
    compound_sparse_cell_name_type(const std::vector<data_type>& t, const std::vector<collection_type>& ct = {}) : cell_name_type(true, false, t, ct) {}
};

struct simple_sparse_cell_name_type : public cell_name_type {
    simple_sparse_cell_name_type(std::vector<data_type>&& t, std::vector<collection_type>&& ct = {}) : cell_name_type(false, false, std::move(t), std::move(ct)) {}
    simple_sparse_cell_name_type(const std::vector<data_type>& t, const std::vector<collection_type>& ct = {}) : cell_name_type(false, false, t, ct) {}
};

struct compound_dense_cell_name_type: public cell_name_type {
    compound_dense_cell_name_type(std::vector<data_type>&& t, std::vector<collection_type>&& ct = {}) : cell_name_type(true, true, std::move(t), std::move(ct)) {}
    compound_dense_cell_name_type(const std::vector<data_type>& t, const std::vector<collection_type>& ct = {}) : cell_name_type(true, true, t, ct) {}
};

struct simple_dense_cell_name_type: public cell_name_type {
    simple_dense_cell_name_type(std::vector<data_type>&& t, std::vector<collection_type>&& ct = {}) : cell_name_type(false, true, std::move(t), std::move(ct)) {}
    simple_dense_cell_name_type(const std::vector<data_type>& t, const std::vector<collection_type>& ct = {}) : cell_name_type(false, true, t, ct) {}
};
