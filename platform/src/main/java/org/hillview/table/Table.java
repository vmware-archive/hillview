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
 */

package org.hillview.table;

import org.hillview.table.api.*;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a simple table held entirely in RAM.
 */
public class Table extends BaseTable {
    private final Schema schema;
    private final IMembershipSet members;
    @Nullable
    private final IColumnLoader columnLoader;

    /**
     * Create an empty table with the specified schema.
     * @param schema schema of the empty table
     */
    public Table(final Schema schema) {
        super(schema);
        this.schema = schema;
        this.columnLoader = null;
        this.members = new FullMembershipSet(0);
    }

    /**
     * Create a table from raw ingredients.
     * @param columns  Columns in the table.
     * @param members  Membership set (rows in the table).
     * @param schema   Schema; must match the set of columns.
     * @param loader   Loader that knows how to load column data.
     */
    protected <C extends IColumn> Table(
            final Iterable<C> columns, final IMembershipSet members,
            final Schema schema, @Nullable final IColumnLoader loader) {
        super(columns);
        this.members = members;
        this.schema = schema;
        this.columnLoader = loader;
    }

    public <C extends IColumn> Table(final Iterable<C> columns, final IMembershipSet members,
                                     @Nullable final IColumnLoader loader) {
        super(columns);
        final Schema s = new Schema();
        for (final IColumn c : columns)
            s.append(c.getDescription());
        this.schema = s;
        this.members = members;
        this.columnLoader = loader;
    }

    public <C extends IColumn> Table(final Iterable<C> columns,
                                     @Nullable final IColumnLoader loader) {
        this(columns, new FullMembershipSet(columnSize(columns)), loader);
    }

    public <C extends IColumn> Table(final C[] columns,
                                     @Nullable final IColumnLoader loader) {
        this(Utilities.arrayToIterable(columns), loader);
    }

    public <C extends IColumn> Table(final C[] columns, IMembershipSet set,
                                     @Nullable final IColumnLoader loader) {
        this(Utilities.arrayToIterable(columns), set, loader);
    }

    @Override
    public ITable project(Schema schema) {
        IColumn[] cols = this.getColumns(schema);
        return new Table(cols, this.members, this.columnLoader);
    }

    @Override
    public ITable replace(IColumn[] columns) {
        return new Table(columns, this.getMembershipSet(), this.columnLoader);
    }

    @Override
    synchronized public ColumnAndConverter[] getLoadedColumns(ColumnAndConverterDescription[] columns) {
        List<String> toLoad = new ArrayList<String>();
        ColumnAndConverter[] result = new ColumnAndConverter[columns.length];
        for (ColumnAndConverterDescription column : columns) {
            String name = column.columnName;
            IColumn col = this.columns.get(name);
            if (col == null)
                throw new RuntimeException("No column named " + name);
            if (!col.isLoaded())
                toLoad.add(name);
        }
        if (!toLoad.isEmpty()) {
            if (this.columnLoader == null)
                throw new RuntimeException("Cannot load columns dynamically");
            IColumn[] cols = this.columnLoader.loadColumns(toLoad);
            for (IColumn c: cols)
                this.columns.put(c.getName(), c);
        }
        for (int i=0; i < columns.length; i++) {
            String name = columns[i].columnName;
            IColumn col = this.columns.get(name);
            if (col == null)
                throw new RuntimeException("Cannot get column " + name);
            result[i] = new ColumnAndConverter(col, columns[i].getConverter());
        }
        return result;
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

    @Override
    public int getNumOfRows() {
        return this.members.getSize();
    }

    @Override
    public ITable selectRowsFromFullTable(IMembershipSet set) {
        return new Table(this.getColumns(), set, this.columnLoader);
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
