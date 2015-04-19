/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "selectable.hh"
#include "selectable_with_field_selection.hh"
#include "field_selector.hh"
#include "writetime_or_ttl.hh"
#include "selector_factories.hh"
#include "cql3/functions/functions.hh"
#include "abstract_function_selector.hh"
#include "writetime_or_ttl_selector.hh"

namespace cql3 {

namespace selection {

shared_ptr<selector::factory>
selectable::writetime_or_ttl::new_selector_factory(database& db, schema_ptr s, std::vector<const column_definition*>& defs) {
    auto&& def = s->get_column_definition(_id->name());
    if (!def) {
        throw exceptions::invalid_request_exception(sprint("Undefined name %s in selection clause", _id));
    }
    if (def->is_primary_key()) {
        throw exceptions::invalid_request_exception(
                sprint("Cannot use selection function %s on PRIMARY KEY part %s",
                              _is_writetime ? "writeTime" : "ttl",
                              def->name()));
    }
    if (def->type->is_collection()) {
        throw exceptions::invalid_request_exception(sprint("Cannot use selection function %s on collections",
                                                        _is_writetime ? "writeTime" : "ttl"));
    }

    return writetime_or_ttl_selector::new_factory(def->name_as_text(), add_and_get_index(*def, defs), _is_writetime);
}

shared_ptr<selectable>
selectable::writetime_or_ttl::raw::prepare(schema_ptr s) {
    return make_shared<writetime_or_ttl>(_id->prepare_column_identifier(s), _is_writetime);
}

bool
selectable::writetime_or_ttl::raw::processes_selection() const {
    return true;
}

shared_ptr<selector::factory>
selectable::with_function::new_selector_factory(database& db, schema_ptr s, std::vector<const column_definition*>& defs) {
    auto&& factories = selector_factories::create_factories_and_collect_column_definitions(_args, db, s, defs);

    // resolve built-in functions before user defined functions
    auto&& fun = functions::functions::get(db, s->ks_name, _function_name, factories->new_instances(), s->ks_name, s->cf_name);
    if (!fun) {
        throw exceptions::invalid_request_exception(sprint("Unknown function '%s'", _function_name));
    }
    if (!fun->return_type()) {
        throw exceptions::invalid_request_exception(sprint("Unknown function %s called in selection clause", _function_name));
    }

    return abstract_function_selector::new_factory(std::move(fun), std::move(factories));
}

shared_ptr<selectable>
selectable::with_function::raw::prepare(schema_ptr s) {
        std::vector<shared_ptr<selectable>> prepared_args;
        prepared_args.reserve(_args.size());
        for (auto&& arg : _args) {
            prepared_args.push_back(arg->prepare(s));
        }
        return ::make_shared<with_function>(_function_name, std::move(prepared_args));
    }

bool
selectable::with_function::raw::processes_selection() const {
    return true;
}

shared_ptr<selector::factory>
selectable::with_field_selection::new_selector_factory(database& db, schema_ptr s, std::vector<const column_definition*>& defs) {
    auto&& factory = _selected->new_selector_factory(db, s, defs);
    auto&& type = factory->new_instance()->get_type();
    auto&& ut = dynamic_pointer_cast<user_type_impl>(std::move(type));
    if (!ut) {
        throw exceptions::invalid_request_exception(
                sprint("Invalid field selection: %s of type %s is not a user type",
                       "FIXME: selectable" /* FIMXME: _selected */, ut->as_cql3_type()));
    }
    for (size_t i = 0; i < ut->size(); ++i) {
        if (ut->field_name(i) != _field->bytes_) {
            continue;
        }
        return field_selector::new_factory(std::move(ut), i, std::move(factory));
    }
    throw exceptions::invalid_request_exception(sprint("%s of type %s has no field %s",
                                                       "FIXME: selectable" /* FIXME: _selected */, ut->as_cql3_type(), _field));
}

shared_ptr<selectable>
selectable::with_field_selection::raw::prepare(schema_ptr s) {
    // static_pointer_cast<> needed due to lack of covariant return type
    // support with smart pointers
    return make_shared<with_field_selection>(_selected->prepare(s),
            static_pointer_cast<column_identifier>(_field->prepare(s)));
}

bool
selectable::with_field_selection::raw::processes_selection() const {
    return true;
}

}

}
