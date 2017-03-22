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

package org.hiero.sketch.table;

import org.hiero.sketch.table.api.*;

import java.util.Arrays;

/**
 * This is a simple table held entirely in RAM.
 */
public class Table extends BaseTable {
    protected final Schema schema;
    protected final IMembershipSet members;

    /**
     * Create an empty table with the specified schema.
     * @param schema schema of the empty table
     */
    public Table(final Schema schema) {
        super(schema);
        this.schema = schema;
        this.members = new FullMembership(0);
    }

    public Table(final Iterable<IColumn> columns, final IMembershipSet members) {
        super(columns);
        final Schema s = new Schema();
        for (final IColumn c : columns)
            s.append(c.getDescription());
        this.schema = s;
        this.members = members;
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

    public Table(final Iterable<IColumn> columns) {
        this(columns, new FullMembership(columnSize(columns)));
    }

    public String toLongString(int rowsToDisplay) {
        return this.toLongString(0, rowsToDisplay);
    }

    @Override
    public int getNumOfRows() {
        return this.members.getSize();
    }

    @Override
    public ITable filter(IMembershipSet set) {
        return new Table(this.getColumns(), set);
    }

    /**
     * Can be used for testing.
     * @return A small table with some interesting contents.
     */
    public static Table testTable() {
        ColumnDescription c0 = new ColumnDescription("Name", ContentsKind.Category, false);
        ColumnDescription c1 = new ColumnDescription("Age", ContentsKind.Int, false);
        StringArrayColumn sac = new StringArrayColumn(c0,
                new String[] { "Mike", "John", "Tom", "Bill", "Bill", "Smith", "Donald", "Bruce",
                               "Bob", "Frank", "Richard", "Steve", "Dave" });
        IntArrayColumn iac = new IntArrayColumn(c1, new int[] { 20, 30, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
        return new Table(Arrays.asList(sac, iac));
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
