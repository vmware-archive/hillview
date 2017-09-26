/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.hillview.table;

import org.hillview.table.api.*;

/**
 * This is a simple table held entirely in RAM.
 */
public class Table extends BaseTable {
    private final Schema schema;
    private final IMembershipSet members;

    /**
     * Create an empty table with the specified schema.
     * @param schema schema of the empty table
     */
    public Table(final Schema schema) {
        super(schema);
        this.schema = schema;
        this.members = new FullMembership(0);
    }

    /**
     * Create a table from raw ingredients.
     * @param columns  Columns in the table.
     * @param members  Membership set (rows in the table).
     * @param schema   Schema; must match the set of columns.
     */
    protected <C extends IColumn> Table(
            final Iterable<C> columns, final IMembershipSet members, Schema schema) {
        super(columns);
        this.members = members;
        this.schema = schema;
    }

    public <C extends IColumn> Table(final Iterable<C> columns, final IMembershipSet members) {
        super(columns);
        final Schema s = new Schema();
        for (final IColumn c : columns)
            s.append(c.getDescription());
        this.schema = s;
        this.members = members;
    }

    @Override
    public ITable project(Schema schema) {
        Iterable<IColumn> cols = this.getColumns(schema);
        return new Table(cols, this.members);
    }

    @Override
    public Schema getSchema() {
        return this.schema;
    }

    @Override
    public IRowIterator getRowIterator() {
        return this.members.getIterator();
    }

    /**
     * Describes the set of rows that are really present in the table.
     */
    @Override
    public IMembershipSet getMembershipSet() { return this.members; }

    public <C extends IColumn> Table(final Iterable<C> columns) {
        this(columns, new FullMembership(columnSize(columns)));
    }

    @Override
    public int getNumOfRows() {
        return this.members.getSize();
    }

    @Override
    public ITable selectRowsFromFullTable(IMembershipSet set) {
        return new Table(this.getColumns(), set);
    }

    /**
     * Generates a table that contains all the columns, and only
     * the rows contained in IMembership Set members with consecutive numbering.
     */
    public SmallTable compress() {
        final ISubSchema subSchema = new FullSubSchema();
        return this.compress(subSchema, this.members);
    }
}
