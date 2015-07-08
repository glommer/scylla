/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <unordered_map>
#include <boost/range/iterator_range.hpp>
#include <boost/range/join.hpp>
#include <boost/range/algorithm/transform.hpp>

#include "cql3/column_specification.hh"
#include "core/shared_ptr.hh"
#include "types.hh"
#include "compound.hh"
#include "gc_clock.hh"
#include "unimplemented.hh"
#include "utils/UUID.hh"
#include "compress.hh"

using column_id = uint32_t;

// make sure these match the order we like columns back from schema
enum class column_kind { partition_key, clustering_key, static_column, regular_column,  };

// CMH this is also manually defined in thrift gen file.
enum class index_type {
    keys,
    custom,
    composites,
    none, // cwi: added none to avoid "optional" bs.
};

typedef std::unordered_map<sstring, sstring> index_options_map;

class schema;

struct index_info {
    index_info(::index_type = ::index_type::none
            , std::experimental::optional<sstring> index_name = std::experimental::optional<sstring>()
            , std::experimental::optional<index_options_map> = std::experimental::optional<index_options_map>());

    enum index_type index_type = ::index_type::none;
    std::experimental::optional<sstring> index_name;
    std::experimental::optional<index_options_map> index_options;
};

class column_definition final {
public:
    template<typename ColumnRange>
    static std::vector<const column_definition*> vectorize(ColumnRange&& columns) {
        std::vector<const column_definition*> r;
        boost::transform(std::forward<ColumnRange>(columns), std::back_inserter(r), [] (auto& def) { return &def; });
        return r;
    }
private:
    bytes _name;

    struct thrift_bits {
        thrift_bits()
            : is_on_all_components(0)
        {}
        uint8_t is_on_all_components : 1;
        // more...?
    };

    thrift_bits _thrift_bits;
    friend class schema;
public:
    column_definition(bytes name, data_type type, column_kind kind, index_info = index_info());

    data_type type;

    // Unique within (kind, schema instance).
    // schema::position() and component_index() depend on the fact that for PK columns this is
    // equivalent to component index.
    // Note: set by schema::build()
    column_id id = 0;

    column_kind kind;
    ::shared_ptr<cql3::column_specification> column_specification;
    index_info idx_info;

    bool is_static() const { return kind == column_kind::static_column; }
    bool is_regular() const { return kind == column_kind::regular_column; }
    bool is_partition_key() const { return kind == column_kind::partition_key; }
    bool is_clustering_key() const { return kind == column_kind::clustering_key; }
    bool is_primary_key() const { return kind == column_kind::partition_key || kind == column_kind::clustering_key; }
    bool is_atomic() const { return !type->is_multi_cell(); }
    bool is_compact_value() const;
    const sstring& name_as_text() const;
    const bytes& name() const;
    friend std::ostream& operator<<(std::ostream& os, const column_definition& cd) {
        return os << cd.name_as_text();
    }
    bool has_component_index() const {
        return is_primary_key();
    }
    uint32_t component_index() const {
        assert(has_component_index());
        return id;
    }
    uint32_t position() const {
        if (has_component_index()) {
            return component_index();
        }
        return 0;
    }
    bool is_on_all_components() const;
    friend bool operator==(const column_definition&, const column_definition&);
};

/*
 * Sub-schema for thrift aspects, i.e. not currently supported stuff.
 * But might be, and should be kept isolated (and starved)
 */
class thrift_schema {
public:
    bool has_compound_comparator() const;
};

bool operator==(const column_definition&, const column_definition&);

class schema_builder;

/*
 * Effectively immutable.
 * Not safe to access across cores because of shared_ptr's.
 */
class schema final {
private:
    // More complex fields are derived from these inside rebuild().
    // Contains only fields which can be safely default-copied.
    struct raw_schema {
        raw_schema(utils::UUID id);
        utils::UUID _id;
        sstring _ks_name;
        sstring _cf_name;
        // regular columns are sorted by name
        // static columns are sorted by name, but present only when there's any clustering column
        std::vector<column_definition> _columns;
        sstring _comment;
        gc_clock::duration _default_time_to_live = gc_clock::duration::zero();
        sstring _default_validator = bytes_type->name();
        data_type _regular_column_name_type;
        double _bloom_filter_fp_chance = 0.01;
        compression_parameters _compressor_params;
        bool _is_dense = false;
    };
    raw_schema _raw;
    thrift_schema _thrift;

    const std::array<size_t, 3> _offsets;

    inline size_t column_offset(column_kind k) const {
        return k == column_kind::partition_key ? 0 : _offsets[size_t(k) - 1];
    }

    std::unordered_map<bytes, const column_definition*> _columns_by_name;
    std::map<bytes, const column_definition*, serialized_compare> _regular_columns_by_name;
    lw_shared_ptr<compound_type<allow_prefixes::no>> _partition_key_type;
    lw_shared_ptr<compound_type<allow_prefixes::no>> _clustering_key_type;
    lw_shared_ptr<compound_type<allow_prefixes::yes>> _clustering_key_prefix_type;

    friend class schema_builder;
public:
    typedef std::vector<column_definition> columns_type;
    typedef typename columns_type::iterator iterator;
    typedef typename columns_type::const_iterator const_iterator;
    typedef boost::iterator_range<iterator> iterator_range_type;
    typedef boost::iterator_range<const_iterator> const_iterator_range_type;

    static constexpr int32_t NAME_LENGTH = 48;

    static const std::experimental::optional<sstring> DEFAULT_COMPRESSOR;

    struct column {
        bytes name;
        data_type type;
        index_info idx_info;
    };
private:
    ::shared_ptr<cql3::column_specification> make_column_specification(const column_definition& def);
    void rebuild();
    schema(const raw_schema&);
public:
    schema(std::experimental::optional<utils::UUID> id,
        sstring ks_name,
        sstring cf_name,
        std::vector<column> partition_key,
        std::vector<column> clustering_key,
        std::vector<column> regular_columns,
        std::vector<column> static_columns,
        data_type regular_column_name_type,
        sstring comment = {});
    schema(const schema&);
    double bloom_filter_fp_chance() const {
        return _raw._bloom_filter_fp_chance;
    }
    schema& set_bloom_filter_fp_chance(double fp) {
        _raw._bloom_filter_fp_chance = fp;
        return *this;
    }
    sstring thrift_key_validator() const;
    void set_compressor_params(compression_parameters c) {
        _raw._compressor_params = c;
    }
    const compression_parameters& get_compressor_params() const {
        return _raw._compressor_params;
    }
    bool is_dense() const {
        return _raw._is_dense;
    }
    thrift_schema& thrift() {
        return _thrift;
    }
    const thrift_schema& thrift() const {
        return _thrift;
    }
    const utils::UUID& id() const {
        return _raw._id;
    }
    const sstring& comment() const {
        return _raw._comment;
    }
    void set_comment(const sstring& comment) {
        _raw._comment = comment;
    }
    void set_id(utils::UUID new_id) {
        _raw._id = new_id;
    }
    bool is_counter() const {
        return false;
    }
    const column_definition* get_column_definition(const bytes& name) const;
    const_iterator regular_begin() const {
        return regular_columns().begin();
    }
    const_iterator regular_end() const {
        return regular_columns().end();
    }
    const_iterator regular_lower_bound(const bytes& name) const {
        // TODO: use regular_columns and a version of std::lower_bound() with heterogeneous comparator
        auto i = _regular_columns_by_name.lower_bound(name);
        if (i == _regular_columns_by_name.end()) {
            return regular_end();
        } else {
            return regular_begin() + i->second->id;
        }
    }
    const_iterator regular_upper_bound(const bytes& name) const {
        // TODO: use regular_columns and a version of std::upper_bound() with heterogeneous comparator
        auto i = _regular_columns_by_name.upper_bound(name);
        if (i == _regular_columns_by_name.end()) {
            return regular_end();
        } else {
            return regular_begin() + i->second->id;
        }
    }
    data_type column_name_type(const column_definition& def) const {
        return def.kind == column_kind::regular_column ? _raw._regular_column_name_type : utf8_type;
    }
    const column_definition& regular_column_at(column_id id) const {
        if (id > regular_columns_count()) {
            throw std::out_of_range("column_id");
        }
        return _raw._columns.at(column_offset(column_kind::regular_column) + id);
    }
    const column_definition& static_column_at(column_id id) const {
        if (id > static_columns_count()) {
            throw std::out_of_range("column_id");
        }
        return _raw._columns.at(column_offset(column_kind::static_column) + id);
    }
    bool is_last_partition_key(const column_definition& def) const {
        return &_raw._columns.at(partition_key_size() - 1) == &def;
    }
    bool has_collections() const ;
    bool has_static_columns() const {
        return !static_columns().empty();
    }
    size_t partition_key_size() const {
        return column_offset(column_kind::clustering_key);
    }
    size_t clustering_key_size() const {
        return column_offset(column_kind::static_column) - column_offset(column_kind::clustering_key);
    }
    size_t static_columns_count() const {
        return column_offset(column_kind::regular_column) - column_offset(column_kind::static_column);
    }
    size_t regular_columns_count() const {
        return _raw._columns.size() - column_offset(column_kind::static_column);
    }
    // Returns a range of column definitions
    const_iterator_range_type partition_key_columns() const {
        return boost::make_iterator_range(_raw._columns.begin() + column_offset(column_kind::partition_key)
                , _raw._columns.begin() + column_offset(column_kind::clustering_key));
    }
    // Returns a range of column definitions
    const_iterator_range_type clustering_key_columns() const {
        return boost::make_iterator_range(_raw._columns.begin() + column_offset(column_kind::clustering_key)
                , _raw._columns.begin() + column_offset(column_kind::static_column));
    }
    // Returns a range of column definitions
    const_iterator_range_type static_columns() const {
        return boost::make_iterator_range(_raw._columns.begin() + column_offset(column_kind::static_column)
                , _raw._columns.begin() + column_offset(column_kind::regular_column));
    }
    // Returns a range of column definitions
    const_iterator_range_type regular_columns() const {
        return boost::make_iterator_range(_raw._columns.begin() + column_offset(column_kind::regular_column)
                , _raw._columns.end());
    }
    // Returns a range of column definitions
    const columns_type& all_columns_in_select_order() const {
        return _raw._columns;
    }
    uint32_t position(const column_definition& column) const {
        if (column.is_primary_key()) {
            return column.id;
        }
        return clustering_key_size();
    }
    gc_clock::duration default_time_to_live() const {
        return _raw._default_time_to_live;
    }

    const sstring& default_validator() const {
        return _raw._default_validator;
    }

    const sstring& ks_name() const {
        return _raw._ks_name;
    }
    const sstring& cf_name() const {
        return _raw._cf_name;
    }
    const lw_shared_ptr<compound_type<allow_prefixes::no>>& partition_key_type() const {
        return _partition_key_type;
    }
    const lw_shared_ptr<compound_type<allow_prefixes::no>>& clustering_key_type() const {
        return _clustering_key_type;
    }
    const lw_shared_ptr<compound_type<allow_prefixes::yes>>& clustering_key_prefix_type() const {
        return _clustering_key_prefix_type;
    }
    const data_type& regular_column_name_type() const {
        return _raw._regular_column_name_type;
    }
    friend bool operator==(const schema&, const schema&);
};

bool operator==(const schema&, const schema&);

using schema_ptr = lw_shared_ptr<const schema>;

utils::UUID generate_legacy_id(const sstring& ks_name, const sstring& cf_name);
