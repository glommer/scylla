/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "schema.hh"
#include "database_fwd.hh"

struct schema_builder {
    schema::raw_schema _raw;

    schema_builder(const schema::raw_schema&);
public:
    schema_builder(const sstring& ks_name, const sstring& cf_name,
            std::experimental::optional<utils::UUID> = { },
            data_type regular_column_name_type = utf8_type);
    schema_builder(const schema_ptr);

    void set_uuid(const utils::UUID& id) {
        _raw._id = id;
    }
    const utils::UUID& uuid() const {
        return _raw._id;
    }
    void set_regular_column_name_type(const data_type& t) {
        _raw._regular_column_name_type = t;
    }
    const data_type& regular_column_name_type() const {
        return _raw._regular_column_name_type;
    }
    const sstring& ks_name() const {
        return _raw._ks_name;
    }
    const sstring& cf_name() const {
        return _raw._cf_name;
    }
    void set_comment(const sstring& s) {
        _raw._comment = s;
    }
    const sstring& comment() const {
        return _raw._comment;
    }
    void set_default_time_to_live(gc_clock::duration t) {
        _raw._default_time_to_live = t;
    }
    gc_clock::duration default_time_to_live() const {
        return _raw._default_time_to_live;
    }

    void set_default_validator(const data_type& validator) {
        _raw._default_validator = validator;
    }
    const data_type& get_default_validator() const {
        return _raw._default_validator;
    }

    void set_bloom_filter_fp_chance(double fp) {
        _raw._bloom_filter_fp_chance = fp;
    }
    double get_bloom_filter_fp_chance() const {
        return _raw._bloom_filter_fp_chance;
    }
    void set_compressor_params(const compression_parameters& cp) {
        _raw._compressor_params = cp;
    }
    void set_is_dense(bool is_dense) {
        _raw._is_dense = is_dense;
    }
    column_definition& find_column(const cql3::column_identifier&);
    schema_builder& with_column(const column_definition& c);
    schema_builder& with_column(bytes name, data_type type, column_kind kind = column_kind::regular_column);
    schema_builder& with_column(bytes name, data_type type, index_info info, column_kind kind = column_kind::regular_column);

    void add_default_index_names(database&);

    schema_ptr build();
};
