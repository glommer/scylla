/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

#ifndef CQL3_OPERATION_HH
#define CQL3_OPERATION_HH

#include "core/shared_ptr.hh"
#include "exceptions/exceptions.hh"
#include "database.hh"
#include "term.hh"

#include <experimental/optional>

namespace cql3 {

class update_parameters;

#if 0
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
#endif

/**
 * An UPDATE or DELETE operation.
 *
 * For UPDATE this includes:
 *   - setting a constant
 *   - counter operations
 *   - collections operations
 * and for DELETE:
 *   - deleting a column
 *   - deleting an element of collection column
 *
 * Fine grained operation are obtained from their raw counterpart (Operation.Raw, which
 * correspond to a parsed, non-checked operation) by provided the receiver for the operation.
 */
class operation {
public:
    // the column the operation applies to
    // We can hold a reference because all operations have life bound to their statements and
    // statements pin the schema.
    const column_definition& column;

protected:
    // Term involved in the operation. In theory this should not be here since some operation
    // may require none of more than one term, but most need 1 so it simplify things a bit.
    const ::shared_ptr<term> _t;

public:
    operation(column_definition& column_, ::shared_ptr<term> t)
        : column{column_}
        , _t{t}
    { }

    virtual ~operation() {}

    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const {
        return _t && _t->uses_function(ks_name, function_name);
    }

    /**
    * @return whether the operation requires a read of the previous value to be executed
    * (only lists setterByIdx, discard and discardByIdx requires that).
    */
    virtual bool requires_read() {
        return false;
    }

    /**
     * Collects the column specification for the bind variables of this operation.
     *
     * @param bound_names the list of column specification where to collect the
     * bind variables of this term in.
     */
    virtual void collect_marker_specification(::shared_ptr<variable_specifications> bound_names) {
        if (_t) {
            _t->collect_marker_specification(bound_names);
        }
    }

    /**
     * Execute the operation.
     */
    virtual void execute(mutation& m, const clustering_prefix& row_key, const update_parameters& params) = 0;

    /**
     * A parsed raw UPDATE operation.
     *
     * This can be one of:
     *   - Setting a value: c = v
     *   - Setting an element of a collection: c[x] = v
     *   - An addition/subtraction to a variable: c = c +/- v (where v can be a collection literal)
     *   - An prepend operation: c = v + c
     */
    class raw_update {
    public:
        virtual ~raw_update() {}

        /**
         * This method validates the operation (i.e. validate it is well typed)
         * based on the specification of the receiver of the operation.
         *
         * It returns an Operation which can be though as post-preparation well-typed
         * Operation.
         *
         * @param receiver the "column" this operation applies to. Note that
         * contrarly to the method of same name in Term.Raw, the receiver should always
         * be a true column.
         * @return the prepared update operation.
         */
        virtual ::shared_ptr<operation> prepare(const sstring& keyspace, column_definition& receiver) = 0;

        /**
         * @return whether this operation can be applied alongside the {@code
         * other} update (in the same UPDATE statement for the same column).
         */
        virtual bool is_compatible_with(::shared_ptr<raw_update> other) = 0;
    };

    /**
     * A parsed raw DELETE operation.
     *
     * This can be one of:
     *   - Deleting a column
     *   - Deleting an element of a collection
     */
    class raw_deletion {
    public:
        ~raw_deletion() {}

        /**
         * The name of the column affected by this delete operation.
         */
        virtual ::shared_ptr<column_identifier::raw> affectedColumn() = 0;

        /**
         * This method validates the operation (i.e. validate it is well typed)
         * based on the specification of the column affected by the operation (i.e the
         * one returned by affectedColumn()).
         *
         * It returns an Operation which can be though as post-preparation well-typed
         * Operation.
         *
         * @param receiver the "column" this operation applies to.
         * @return the prepared delete operation.
         */
        virtual ::shared_ptr<operation> prepare(const sstring& keyspace, column_definition& receiver) = 0;
    };

    class set_value;

    class set_element : public raw_update {
        const shared_ptr<term::raw> _selector;
        const shared_ptr<term::raw> _value;
    public:
        set_element(shared_ptr<term::raw> selector, shared_ptr<term::raw> value)
            : _selector(std::move(selector)), _value(std::move(value)) {
        }

        virtual shared_ptr<operation> prepare(const sstring& keyspace, column_definition& receiver);
#if 0
        protected String toString(ColumnSpecification column)
        {
            return String.format("%s[%s] = %s", column.name, selector, value);
        }

#endif
        virtual bool is_compatible_with(shared_ptr<raw_update> other) override;
    };

    class addition : public raw_update {
        const shared_ptr<term::raw> _value;
    public:
        addition(shared_ptr<term::raw> value)
                : _value(value) {
        }

        virtual shared_ptr<operation> prepare(const sstring& keyspace, column_definition& receiver) override;

#if 0
        protected String toString(ColumnSpecification column)
        {
            return String.format("%s = %s + %s", column.name, column.name, value);
        }
#endif

        virtual bool is_compatible_with(shared_ptr<raw_update> other) override;
    };

    class subtraction : public raw_update {
        const shared_ptr<term::raw> _value;
    public:
        subtraction(shared_ptr<term::raw> value)
                : _value(value) {
        }

        virtual shared_ptr<operation> prepare(const sstring& keyspace, column_definition& receiver) override;

#if 0
        protected String toString(ColumnSpecification column)
        {
            return String.format("%s = %s - %s", column.name, column.name, value);
        }
#endif

        virtual bool is_compatible_with(shared_ptr<raw_update> other) override;
    };

    class prepend : public raw_update {
        shared_ptr<term::raw> _value;
    public:
        prepend(shared_ptr<term::raw> value)
                : _value(std::move(value)) {
        }

        virtual shared_ptr<operation> prepare(const sstring& keyspace, column_definition& receiver) override;

#if 0
        protected String toString(ColumnSpecification column)
        {
            return String.format("%s = %s - %s", column.name, value, column.name);
        }
#endif
        virtual bool is_compatible_with(shared_ptr<raw_update> other) override;
    };

#if 0
    public static class ColumnDeletion implements RawDeletion
    {
        private final ColumnIdentifier.Raw id;

        public ColumnDeletion(ColumnIdentifier.Raw id)
        {
            this.id = id;
        }

        public ColumnIdentifier.Raw affectedColumn()
        {
            return id;
        }

        public Operation prepare(String keyspace, ColumnDefinition receiver) throws InvalidRequestException
        {
            // No validation, deleting a column is always "well typed"
            return new Constants.Deleter(receiver);
        }
    }

    public static class ElementDeletion implements RawDeletion
    {
        private final ColumnIdentifier.Raw id;
        private final Term.Raw element;

        public ElementDeletion(ColumnIdentifier.Raw id, Term.Raw element)
        {
            this.id = id;
            this.element = element;
        }

        public ColumnIdentifier.Raw affectedColumn()
        {
            return id;
        }

        public Operation prepare(String keyspace, ColumnDefinition receiver) throws InvalidRequestException
        {
            if (!(receiver.type.isCollection()))
                throw new InvalidRequestException(String.format("Invalid deletion operation for non collection column %s", receiver.name));
            else if (!(receiver.type.isMultiCell()))
                throw new InvalidRequestException(String.format("Invalid deletion operation for frozen collection column %s", receiver.name));

            switch (((CollectionType)receiver.type).kind)
            {
                case LIST:
                    Term idx = element.prepare(keyspace, Lists.indexSpecOf(receiver));
                    return new Lists.DiscarderByIndex(receiver, idx);
                case SET:
                    Term elt = element.prepare(keyspace, Sets.valueSpecOf(receiver));
                    return new Sets.Discarder(receiver, elt);
                case MAP:
                    Term key = element.prepare(keyspace, Maps.keySpecOf(receiver));
                    return new Maps.DiscarderByKey(receiver, key);
            }
            throw new AssertionError();
        }
    }
#endif
};

}

#endif
