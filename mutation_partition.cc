/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include <boost/range/adaptor/reversed.hpp>
#include "mutation_partition.hh"
#include "mutation_partition_applier.hh"

mutation_partition::mutation_partition(const mutation_partition& x)
        : _tombstone(x._tombstone)
        , _static_row(x._static_row)
        , _rows(x._rows.value_comp())
        , _row_tombstones(x._row_tombstones.value_comp()) {
    auto cloner = [] (const auto& x) {
        return current_allocator().construct<std::remove_const_t<std::remove_reference_t<decltype(x)>>>(x);
    };
    _rows.clone_from(x._rows, cloner, current_deleter<rows_entry>());
    try {
        _row_tombstones.clone_from(x._row_tombstones, cloner, current_deleter<row_tombstones_entry>());
    } catch (...) {
        _rows.clear_and_dispose(current_deleter<rows_entry>());
        throw;
    }
}

mutation_partition::~mutation_partition() {
    _rows.clear_and_dispose(current_deleter<rows_entry>());
    _row_tombstones.clear_and_dispose(current_deleter<row_tombstones_entry>());
}

mutation_partition&
mutation_partition::operator=(const mutation_partition& x) {
    mutation_partition n(x);
    std::swap(*this, n);
    return *this;
}

void
mutation_partition::apply(const schema& schema, const mutation_partition& p) {
    _tombstone.apply(p._tombstone);

    for (auto&& e : p._row_tombstones) {
        apply_row_tombstone(schema, e.prefix(), e.t());
    }

    _static_row.merge(schema, column_kind::static_column, p._static_row);

    for (auto&& entry : p._rows) {
        auto i = _rows.find(entry);
        if (i == _rows.end()) {
            auto e = current_allocator().construct<rows_entry>(entry);
            _rows.insert(i, *e);
        } else {
            i->row().apply(entry.row().deleted_at());
            i->row().apply(entry.row().marker());
            i->row().cells().merge(schema, column_kind::regular_column, entry.row().cells());
        }
    }
}

void
mutation_partition::apply(const schema& s, mutation_partition&& p) {
    _tombstone.apply(p._tombstone);

    p._row_tombstones.clear_and_dispose([this, &s] (row_tombstones_entry* e) {
        apply_row_tombstone(s, e);
    });

    _static_row.merge(s, column_kind::static_column, std::move(p._static_row));

    auto p_i = p._rows.begin();
    auto p_end = p._rows.end();
    while (p_i != p_end) {
        rows_entry& entry = *p_i;
        auto i = _rows.find(entry);
        if (i == _rows.end()) {
            p_i = p._rows.erase(p_i);
            _rows.insert(i, entry);
        } else {
            i->row().apply(entry.row().deleted_at());
            i->row().apply(entry.row().marker());
            i->row().cells().merge(s, column_kind::regular_column, std::move(entry.row().cells()));
            p_i = _rows.erase_and_dispose(p_i, current_deleter<rows_entry>());
        }
    }
}

void
mutation_partition::apply(const schema& schema, mutation_partition_view p) {
    mutation_partition_applier applier(schema, *this);
    p.accept(schema, applier);
}

tombstone
mutation_partition::range_tombstone_for_row(const schema& schema, const clustering_key& key) const {
    tombstone t = _tombstone;

    if (_row_tombstones.empty()) {
        return t;
    }

    auto c = row_tombstones_entry::key_comparator(
        clustering_key::prefix_view_type::less_compare_with_prefix(schema));

    // _row_tombstones contains only strict prefixes
    for (unsigned prefix_len = 1; prefix_len < schema.clustering_key_size(); ++prefix_len) {
        auto i = _row_tombstones.find(key.prefix_view(schema, prefix_len), c);
        if (i != _row_tombstones.end()) {
            t.apply(i->t());
        }
    }

    return t;
}

tombstone
mutation_partition::tombstone_for_row(const schema& schema, const clustering_key& key) const {
    tombstone t = range_tombstone_for_row(schema, key);

    auto j = _rows.find(key, rows_entry::compare(schema));
    if (j != _rows.end()) {
        t.apply(j->row().deleted_at());
    }

    return t;
}

tombstone
mutation_partition::tombstone_for_row(const schema& schema, const rows_entry& e) const {
    tombstone t = range_tombstone_for_row(schema, e.key());
    t.apply(e.row().deleted_at());
    return t;
}

void
mutation_partition::apply_row_tombstone(const schema& schema, clustering_key_prefix prefix, tombstone t) {
    assert(!prefix.is_full(schema));
    auto i = _row_tombstones.lower_bound(prefix, row_tombstones_entry::compare(schema));
    if (i == _row_tombstones.end() || !prefix.equal(schema, i->prefix())) {
        auto e = current_allocator().construct<row_tombstones_entry>(std::move(prefix), t);
        _row_tombstones.insert(i, *e);
    } else {
        i->apply(t);
    }
}

void
mutation_partition::apply_row_tombstone(const schema& s, row_tombstones_entry* e) noexcept {
    auto i = _row_tombstones.lower_bound(*e);
    if (i == _row_tombstones.end() || !e->prefix().equal(s, i->prefix())) {
        _row_tombstones.insert(i, *e);
    } else {
        i->apply(e->t());
        current_allocator().destroy(e);
    }
}

void
mutation_partition::apply_delete(const schema& schema, const exploded_clustering_prefix& prefix, tombstone t) {
    if (!prefix) {
        apply(t);
    } else if (prefix.is_full(schema)) {
        apply_delete(schema, clustering_key::from_clustering_prefix(schema, prefix), t);
    } else {
        apply_row_tombstone(schema, clustering_key_prefix::from_clustering_prefix(schema, prefix), t);
    }
}

void
mutation_partition::apply_delete(const schema& schema, clustering_key&& key, tombstone t) {
    clustered_row(schema, std::move(key)).apply(t);
}

void
mutation_partition::apply_delete(const schema& schema, clustering_key_view key, tombstone t) {
    clustered_row(schema, key).apply(t);
}

void
mutation_partition::apply_insert(const schema& s, clustering_key_view key, api::timestamp_type created_at) {
    clustered_row(s, key).apply(created_at);
}

const rows_entry*
mutation_partition::find_entry(const schema& schema, const clustering_key_prefix& key) const {
    auto i = _rows.find(key, rows_entry::key_comparator(clustering_key::less_compare_with_prefix(schema)));
    if (i == _rows.end()) {
        return nullptr;
    }
    return &*i;
}

const row*
mutation_partition::find_row(const clustering_key& key) const {
    auto i = _rows.find(key);
    if (i == _rows.end()) {
        return nullptr;
    }
    return &i->row().cells();
}

deletable_row&
mutation_partition::clustered_row(clustering_key&& key) {
    auto i = _rows.find(key);
    if (i == _rows.end()) {
        auto e = current_allocator().construct<rows_entry>(std::move(key));
        _rows.insert(i, *e);
        return e->row();
    }
    return i->row();
}

deletable_row&
mutation_partition::clustered_row(const clustering_key& key) {
    auto i = _rows.find(key);
    if (i == _rows.end()) {
        auto e = current_allocator().construct<rows_entry>(key);
        _rows.insert(i, *e);
        return e->row();
    }
    return i->row();
}

deletable_row&
mutation_partition::clustered_row(const schema& s, const clustering_key_view& key) {
    auto i = _rows.find(key, rows_entry::compare(s));
    if (i == _rows.end()) {
        auto e = current_allocator().construct<rows_entry>(key);
        _rows.insert(i, *e);
        return e->row();
    }
    return i->row();
}

boost::iterator_range<mutation_partition::rows_type::const_iterator>
mutation_partition::range(const schema& schema, const query::range<clustering_key_prefix>& r) const {
    auto cmp = rows_entry::key_comparator(clustering_key::prefix_equality_less_compare(schema));
    auto i1 = r.start() ? (r.start()->is_inclusive()
            ? _rows.lower_bound(r.start()->value(), cmp)
            : _rows.upper_bound(r.start()->value(), cmp)) : _rows.cbegin();
    auto i2 = r.end() ? (r.end()->is_inclusive()
            ? _rows.upper_bound(r.end()->value(), cmp)
            : _rows.lower_bound(r.end()->value(), cmp)) : _rows.cend();
    return boost::make_iterator_range(i1, i2);
}


template<typename Func>
void mutation_partition::for_each_row(const schema& schema, const query::range<clustering_key_prefix>& row_range, bool reversed, Func&& func) const
{
    auto r = range(schema, row_range);
    if (!reversed) {
        for (const auto& e : r) {
            if (func(e) == stop_iteration::yes) {
                break;
            }
        }
    } else {
        for (const auto& e : r | boost::adaptors::reversed) {
            if (func(e) == stop_iteration::yes) {
                break;
            }
        }
    }
}

static void get_row_slice(const schema& s,
    column_kind kind,
    const row& cells,
    const std::vector<column_id>& columns,
    tombstone tomb,
    gc_clock::time_point now,
    query::result::row_writer& writer)
{
    for (auto id : columns) {
        const atomic_cell_or_collection* cell = cells.find_cell(id);
        if (!cell) {
            writer.add_empty();
        } else {
            auto&& def = s.column_at(kind, id);
            if (def.is_atomic()) {
                auto c = cell->as_atomic_cell();
                if (!c.is_live(tomb, now)) {
                    writer.add_empty();
                } else {
                    writer.add(cell->as_atomic_cell());
                }
            } else {
                auto&& mut = cell->as_collection_mutation();
                auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
                auto m_view = ctype->deserialize_mutation_form(mut);
                m_view.tomb.apply(tomb);
                auto m_ser = ctype->serialize_mutation_form_only_live(m_view, now);
                if (ctype->is_empty(m_ser)) {
                    writer.add_empty();
                } else {
                    writer.add(m_ser);
                }
            }
        }
    }
}

bool has_any_live_data(const schema& s, column_kind kind, const row& cells, tombstone tomb, gc_clock::time_point now) {
    bool any_live = false;
    cells.for_each_cell_until([&] (column_id id, const atomic_cell_or_collection& cell_or_collection) {
        const column_definition& def = s.column_at(kind, id);
        if (def.is_atomic()) {
            auto&& c = cell_or_collection.as_atomic_cell();
            if (c.is_live(tomb, now)) {
                any_live = true;
                return stop_iteration::yes;
            }
        } else {
            auto&& cell = cell_or_collection.as_collection_mutation();
            auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
            if (ctype->is_any_live(cell, tomb, now)) {
                any_live = true;
                return stop_iteration::yes;
            }
        }
        return stop_iteration::no;
    });
    return any_live;
}

void
mutation_partition::query(query::result::partition_writer& pw,
    const schema& s,
    gc_clock::time_point now,
    uint32_t limit) const
{
    const query::partition_slice& slice = pw.slice();

    // To avoid retraction of the partition entry in case of limit == 0.
    assert(limit > 0);

    bool any_live = has_any_live_data(s, column_kind::static_column, static_row(), _tombstone, now);

    if (!slice.static_columns.empty()) {
        auto row_builder = pw.add_static_row();
        get_row_slice(s, column_kind::static_column, static_row(), slice.static_columns, partition_tombstone(), now, row_builder);
        row_builder.finish();
    }

    auto is_reversed = slice.options.contains(query::partition_slice::option::reversed);
    for (auto&& row_range : slice.row_ranges) {
        if (limit == 0) {
            break;
        }

        // FIXME: Optimize for a full-tuple singular range. mutation_partition::range()
        // does two lookups to form a range, even for singular range. We need
        // only one lookup for a full-tuple singular range though.
        for_each_row(s, row_range, is_reversed, [&] (const rows_entry& e) {
            auto& row = e.row();
            auto row_tombstone = tombstone_for_row(s, e);

            if (row.is_live(s, row_tombstone, now)) {
                any_live = true;
                auto row_builder = pw.add_row(e.key());
                get_row_slice(s, column_kind::regular_column, row.cells(), slice.regular_columns, row_tombstone, now, row_builder);
                row_builder.finish();
                if (--limit == 0) {
                    return stop_iteration::yes;
                }
            }
            return stop_iteration::no;
        });
    }

    if (!any_live) {
        pw.retract();
    } else {
        pw.finish();
    }
}

std::ostream&
operator<<(std::ostream& os, const std::pair<column_id, const atomic_cell_or_collection&>& c) {
    return fprint(os, "{column: %s %s}", c.first, c.second);
}

std::ostream&
operator<<(std::ostream& os, const row& r) {
    sstring cells;
    switch (r._type) {
    case row::storage_type::set:
        cells = ::join(", ", r.get_range_set());
        break;
    case row::storage_type::vector:
        cells = ::join(", ", r.get_range_vector());
        break;
    }
    return fprint(os, "{row: %s}", cells);
}

std::ostream&
operator<<(std::ostream& os, const row_marker& rm) {
    if (rm.is_missing()) {
        return fprint(os, "{missing row_marker}");
    } else if (rm._ttl == row_marker::dead) {
        return fprint(os, "{dead row_marker %s %s}", rm._timestamp, rm._expiry.time_since_epoch().count());
    } else {
        return fprint(os, "{row_marker %s %s %s}", rm._timestamp, rm._ttl.count(),
            rm._ttl != row_marker::no_ttl ? rm._expiry.time_since_epoch().count() : 0);
    }
}

std::ostream&
operator<<(std::ostream& os, const deletable_row& dr) {
    return fprint(os, "{deletable_row: %s %s %s}", dr._marker, dr._deleted_at, dr._cells);
}

std::ostream&
operator<<(std::ostream& os, const rows_entry& re) {
    return fprint(os, "{rows_entry: %s %s}", re._key, re._row);
}

std::ostream&
operator<<(std::ostream& os, const row_tombstones_entry& rte) {
    return fprint(os, "{row_tombstone_entry: %s %s}", rte._prefix, rte._t);
}

std::ostream&
operator<<(std::ostream& os, const mutation_partition& mp) {
    return fprint(os, "{mutation_partition: %s (%s) static %s clustered %s}",
                  mp._tombstone, ::join(", ", mp._row_tombstones), mp._static_row,
                  ::join(", ", mp._rows));
}

constexpr gc_clock::duration row_marker::no_ttl;
constexpr gc_clock::duration row_marker::dead;

bool
deletable_row::equal(const schema& s, const deletable_row& other) const {
    if (_deleted_at != other._deleted_at || _marker != other._marker) {
        return false;
    }
    return _cells == other._cells;
}

bool
rows_entry::equal(const schema& s, const rows_entry& other) const {
    return key().equal(s, other.key()) && row().equal(s, other.row());
}

bool
row_tombstones_entry::equal(const schema& s, const row_tombstones_entry& other) const {
    return prefix().equal(s, other.prefix()) && t() == other.t();
}

bool mutation_partition::equal(const schema& s, const mutation_partition& p) const {
    if (_tombstone != p._tombstone) {
        return false;
    }

    if (!std::equal(_rows.begin(), _rows.end(), p._rows.begin(), p._rows.end(),
        [&s] (const rows_entry& e1, const rows_entry& e2) { return e1.equal(s, e2); }
    )) {
        return false;
    }

    if (!std::equal(_row_tombstones.begin(), _row_tombstones.end(),
        p._row_tombstones.begin(), p._row_tombstones.end(),
        [&s] (const row_tombstones_entry& e1, const row_tombstones_entry& e2) { return e1.equal(s, e2); }
    )) {
        return false;
    }

    return _static_row == p._static_row;
}

void
merge_column(const column_definition& def,
             atomic_cell_or_collection& old,
             atomic_cell_or_collection&& neww) {
    if (def.is_atomic()) {
        if (compare_atomic_cell_for_merge(old.as_atomic_cell(), neww.as_atomic_cell()) < 0) {
            old = std::move(neww);
        }
    } else {
        auto ct = static_pointer_cast<const collection_type_impl>(def.type);
        old = ct->merge(old.as_collection_mutation(), neww.as_collection_mutation());
    }
}

void
row::apply(const column_definition& column, const atomic_cell_or_collection& value) {
    // FIXME: Optimize
    atomic_cell_or_collection tmp(value);
    apply(column, std::move(tmp));
}

void
row::apply(const column_definition& column, atomic_cell_or_collection&& value) {
    // our mutations are not yet immutable
    auto id = column.id;
    if (_type == storage_type::vector && id < max_vector_size) {
        if (id >= _storage.vector.size()) {
            _storage.vector.resize(id);
            _storage.vector.emplace_back(std::move(value));
            _size++;
        } else if (!bool(_storage.vector[id])) {
            _storage.vector[id] = std::move(value);
            _size++;
        } else {
            merge_column(column, _storage.vector[id], std::move(value));
        }
    } else {
        if (_type == storage_type::vector) {
            vector_to_set();
        }
        auto i = _storage.set.lower_bound(id, cell_entry::compare());
        if (i == _storage.set.end() || i->id() != id) {
            auto e = current_allocator().construct<cell_entry>(id, std::move(value));
            _storage.set.insert(i, *e);
            _size++;
        } else {
            merge_column(column, i->cell(), std::move(value));
        }
    }
}

void
row::append_cell(column_id id, atomic_cell_or_collection value) {
    if (_type == storage_type::vector && id < max_vector_size) {
        _storage.vector.resize(id);
        _storage.vector.emplace_back(std::move(value));
    } else {
        if (_type == storage_type::vector) {
            vector_to_set();
        }
        auto e = current_allocator().construct<cell_entry>(id, std::move(value));
        _storage.set.insert(_storage.set.end(), *e);
    }
    _size++;
}

const atomic_cell_or_collection*
row::find_cell(column_id id) const {
    if (_type == storage_type::vector) {
        if (id >= _storage.vector.size() || !bool(_storage.vector[id])) {
            return nullptr;
        }
        return &_storage.vector[id];
    } else {
        auto i = _storage.set.find(id, cell_entry::compare());
        if (i == _storage.set.end()) {
            return nullptr;
        }
        return &i->cell();
    }
}

uint32_t
mutation_partition::compact_for_query(
    const schema& s,
    gc_clock::time_point query_time,
    const std::vector<query::clustering_range>& row_ranges,
    uint32_t row_limit)
{
    assert(row_limit > 0);
    bool stop = false;

    // FIXME: drop GC'able tombstones

    bool static_row_live = _static_row.compact_and_expire(s, column_kind::static_column, _tombstone, query_time);

    uint32_t row_count = 0;

    auto last = _rows.begin();
    for (auto&& row_range : row_ranges) {
        if (stop) {
            break;
        }

        auto it_range = range(s, row_range);
        last = _rows.erase_and_dispose(last, it_range.begin(), current_deleter<rows_entry>());

        while (last != it_range.end()) {
            rows_entry& e = *last;
            deletable_row& row = e.row();

            tombstone tomb = tombstone_for_row(s, e);

            bool is_live = row.cells().compact_and_expire(s, column_kind::regular_column, tomb, query_time);
            is_live |= row.marker().compact_and_expire(tomb, query_time);

            // when row_limit is reached, do not exit immediately,
            // iterate to the next live_row instead to include trailing
            // tombstones in the mutation. This is how Origin deals with
            // https://issues.apache.org/jira/browse/CASSANDRA-8933
            if (is_live) {
                if (row_count == row_limit) {
                    stop = true;
                    break;
                }
                ++row_count;
            }
            ++last;
        }
    }

    if (row_count == 0 && static_row_live) {
        ++row_count;
    }

    _rows.erase_and_dispose(last, _rows.end(), current_deleter<rows_entry>());

    // FIXME: purge unneeded prefix tombstones based on row_ranges

    return row_count;
}

bool
deletable_row::is_live(const schema& s, tombstone base_tombstone, gc_clock::time_point query_time = gc_clock::time_point::min()) const {
    // _created_at corresponds to the row marker cell, present for rows
    // created with the 'insert' statement. If row marker is live, we know the
    // row is live. Otherwise, a row is considered live if it has any cell
    // which is live.
    base_tombstone.apply(_deleted_at);
    return _marker.is_live(base_tombstone, query_time)
           || has_any_live_data(s, column_kind::regular_column, _cells, base_tombstone, query_time);
}

bool
mutation_partition::is_static_row_live(const schema& s, gc_clock::time_point query_time) const {
    return has_any_live_data(s, column_kind::static_column, static_row(), _tombstone, query_time);
}

size_t
mutation_partition::live_row_count(const schema& s, gc_clock::time_point query_time) const {
    size_t count = 0;

    for (const rows_entry& e : _rows) {
        tombstone base_tombstone = range_tombstone_for_row(s, e.key());
        if (e.row().is_live(s, base_tombstone, query_time)) {
            ++count;
        }
    }

    if (count == 0 && is_static_row_live(s, query_time)) {
        return 1;
    }

    return count;
}

rows_entry::rows_entry(rows_entry&& o) noexcept
    : _link(std::move(o._link))
    , _key(std::move(o._key))
    , _row(std::move(o._row))
{
    using container_type = mutation_partition::rows_type;
    container_type::node_algorithms::replace_node(o._link.this_ptr(), _link.this_ptr());
    container_type::node_algorithms::init(o._link.this_ptr());
}

row_tombstones_entry::row_tombstones_entry(row_tombstones_entry&& o) noexcept
    : _link()
    , _prefix(std::move(o._prefix))
    , _t(std::move(o._t))
{
    using container_type = mutation_partition::row_tombstones_type;
    container_type::node_algorithms::replace_node(o._link.this_ptr(), _link.this_ptr());
    container_type::node_algorithms::init(o._link.this_ptr());
}

row::row(const row& o)
    : _type(o._type)
    , _size(o._size)
{
    if (_type == storage_type::vector) {
        new (&_storage.vector) vector_type(o._storage.vector);
    } else {
        auto cloner = [] (const auto& x) {
            return current_allocator().construct<std::remove_const_t<std::remove_reference_t<decltype(x)>>>(x);
        };
        new (&_storage.set) map_type;
        try {
            _storage.set.clone_from(o._storage.set, cloner, current_deleter<cell_entry>());
        } catch (...) {
            _storage.set.~map_type();
            throw;
        }
    }
}

row::~row() {
    if (_type == storage_type::vector) {
        _storage.vector.~vector_type();
    } else {
        _storage.set.clear_and_dispose(current_deleter<cell_entry>());
        _storage.set.~map_type();
    }
}

row::cell_entry::cell_entry(const cell_entry& o) noexcept
    : _id(o._id)
    , _cell(o._cell)
{ }

row::cell_entry::cell_entry(cell_entry&& o) noexcept
    : _link()
    , _id(o._id)
    , _cell(std::move(o._cell))
{
    using container_type = row::map_type;
    container_type::node_algorithms::replace_node(o._link.this_ptr(), _link.this_ptr());
    container_type::node_algorithms::init(o._link.this_ptr());
}

const atomic_cell_or_collection& row::cell_at(column_id id) const {
    auto&& cell = find_cell(id);
    if (!cell) {
        throw std::out_of_range(sprint("Column not found for id = %d", id));
    }
    return *cell;
}

void row::vector_to_set()
{
    assert(_type == storage_type::vector);
    map_type set;
    for (unsigned i = 0; i < _storage.vector.size(); i++) {
        auto& c = _storage.vector[i];
        if (!bool(c)) {
            continue;
        }
        auto e = current_allocator().construct<cell_entry>(i, std::move(c));
        set.insert(set.end(), *e);
    }
    _storage.vector.~vector_type();
    new (&_storage.set) map_type(std::move(set));
    _type = storage_type::set;
}

void row::reserve(column_id last_column)
{
    if (_type == storage_type::vector && last_column >= internal_count) {
        if (last_column >= max_vector_size) {
            vector_to_set();
        } else {
            _storage.vector.reserve(last_column);
        }
    }
}

bool row::operator==(const row& other) const {
    if (size() != other.size()) {
        return false;
    }

    auto cells_equal = [] (std::pair<column_id, const atomic_cell_or_collection&> c1, std::pair<column_id, const atomic_cell_or_collection&> c2) {
        return c1.first == c2.first && c1.second == c2.second;
    };
    if (_type == storage_type::vector) {
        if (other._type == storage_type::vector) {
            return boost::equal(get_range_vector(), other.get_range_vector(), cells_equal);
        } else {
            return boost::equal(get_range_vector(), other.get_range_set(), cells_equal);
        }
    } else {
        if (other._type == storage_type::vector) {
            return boost::equal(get_range_set(), other.get_range_vector(), cells_equal);
        } else {
            return boost::equal(get_range_set(), other.get_range_set(), cells_equal);
        }
    }
}

row::row() {
    new (&_storage.vector) vector_type;
}

row::row(row&& other)
    : _type(other._type), _size(other._size) {
    if (_type == storage_type::vector) {
        new (&_storage.vector) vector_type(std::move(other._storage.vector));
    } else {
        new (&_storage.set) map_type(std::move(other._storage.set));
    }
}

row& row::operator=(row&& other) {
    if (this != &other) {
        this->~row();
        new (this) row(std::move(other));
    }
    return *this;
}

void row::merge(const schema& s, column_kind kind, const row& other) {
    if (other._type == storage_type::vector) {
        reserve(other._storage.vector.size() - 1);
    } else {
        reserve(other._storage.set.rbegin()->id());
    }
    other.for_each_cell([&] (column_id id, const atomic_cell_or_collection& cell) {
        apply(s.column_at(kind, id), cell);
    });
}

void row::merge(const schema& s, column_kind kind, row&& other) {
    if (other._type == storage_type::vector) {
        reserve(other._storage.vector.size() - 1);
    } else {
        reserve(other._storage.set.rbegin()->id());
    }
    // FIXME: Optimize when 'other' is a set. We could move whole entries, not only cells.
    other.for_each_cell_until([&] (column_id id, atomic_cell_or_collection& cell) {
        apply(s.column_at(kind, id), std::move(cell));
        return stop_iteration::no;
    });
}

bool row::compact_and_expire(const schema& s, column_kind kind, tombstone tomb, gc_clock::time_point query_time) {
    bool any_live = false;
    remove_if([&] (column_id id, atomic_cell_or_collection& c) {
        bool erase = false;
        const column_definition& def = s.column_at(kind, id);
        if (def.is_atomic()) {
            atomic_cell_view cell = c.as_atomic_cell();
            if (cell.is_covered_by(tomb)) {
                erase = true;
            } else if (cell.has_expired(query_time)) {
                c = atomic_cell::make_dead(cell.timestamp(), cell.deletion_time());
            } else {
                any_live |= cell.is_live();
            }
        } else {
            auto&& cell = c.as_collection_mutation();
            auto&& ctype = static_pointer_cast<const collection_type_impl>(def.type);
            auto m_view = ctype->deserialize_mutation_form(cell);
            collection_type_impl::mutation m = m_view.materialize();
            any_live |= m.compact_and_expire(tomb, query_time);
            if (m.cells.empty() && m.tomb <= tomb) {
                erase = true;
            } else {
                c = ctype->serialize_mutation_form(m);
            }
        }
        return erase;
    });
    return any_live;
}
