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
import org.hillview.table.columns.LazyColumn;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.utils.Linq;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This is a simple table held entirely in RAM.
 */
public class Table extends BaseTable {
    /**
     * The table schema: the set of columns.
     */
    private final Schema schema;
    /**
     * The list of rows that belongs to this table.
     */
    private final IMembershipSet members;
    /**
     * A loader that can be invoked to load columns that are lazily loaded.
     */
    @Nullable
    private final IColumnLoader columnLoader;
    /**
     * Path to the local (worker-resident) file that was used to load this table.
     */
    @Nullable
    private final String sourceFile;

    /**
     * Create an empty table with the specified schema.
     * @param schema schema of the empty table
     */
    public Table(final Schema schema) {
        super(schema);
        this.schema = schema;
        this.columnLoader = null;
        this.sourceFile = null;
        this.members = new FullMembershipSet(0);
    }

    /**
     * Create a table from raw ingredients.
     * @param columns  Columns in the table.
     * @param members  Membership set (rows in the table).
     * @param schema   Schema; must match the set of columns.
     * @param loader   Loader that knows how to load column data.
     * @param sourceFile  The file where the data is loaded from.
     */
    protected <C extends IColumn> Table(
            final List<C> columns, final IMembershipSet members, final Schema schema,
            @Nullable final String sourceFile,
            @Nullable final IColumnLoader loader) {
        super(columns);
        this.members = members;
        this.schema = schema;
        this.columnLoader = loader;
        this.sourceFile = sourceFile;
    }

    public <C extends IColumn> Table(final List<C> columns, final IMembershipSet members,
                                     @Nullable final String sourceFile,
                                     @Nullable final IColumnLoader loader) {
        super(columns);
        final Schema s = new Schema();
        for (final IColumn c : columns)
            s.append(c.getDescription());
        this.schema = s;
        this.sourceFile = sourceFile;
        this.members = members;
        this.columnLoader = loader;
    }

    public <C extends IColumn> Table(final C[] columns,
                                     @Nullable final String sourceFile,
                                     @Nullable final IColumnLoader loader) {
        this(Arrays.asList(columns),
                new FullMembershipSet(columnSize(Arrays.asList(columns))),
                sourceFile, loader);
    }

    public <C extends IColumn> Table(final List<C> columns,
                                     @Nullable final String sourceFile,
                                     @Nullable final IColumnLoader loader) {
        this(columns, new FullMembershipSet(columnSize(columns)), sourceFile, loader);
    }

    /**
     * Creates a table where all columns are lazy.
     * @param desc    A collection column descriptions.
     * @param loader  Loader that knows how to load a column.
     * @param rowCount Number of rows of the table.
     * @param sourceFile  File where this data was loaded from.
     */
    public static Table createLazyTable(List<ColumnDescription> desc, int rowCount,
                                        @Nullable String sourceFile, IColumnLoader loader) {
        List<LazyColumn> cols = Linq.map(desc, d -> new LazyColumn(d, rowCount, loader));
        return new Table(cols, sourceFile, loader);
    }


    /**
     * Creates a new table that only has the columns specified in the schema.
     * @param schema: Schema of the resulting table.  Must be a subset of the
     *              columns of the existing table.
     */
    @Override
    public ITable project(Schema schema) {
        List<IColumn> cols = this.getColumns(schema);
        return new Table(cols, this.members, this.sourceFile, this.columnLoader);
    }

    /**
     * Creates a new table that has a different list of columns, but the same
     * membership set and the same source file.
     * @param columns  Set of columns for new table.
     */
    @Override
    public <T extends IColumn> ITable replace(List<T> columns) {
        return new Table(columns, this.getMembershipSet(),
                this.sourceFile, this.columnLoader);
    }

    @Override
    synchronized public List<ColumnAndConverter> getLoadedColumns(
            List<ColumnAndConverterDescription> columns) {
        List<String> toLoad = new ArrayList<String>();
        List<ColumnAndConverter> result = new ArrayList<ColumnAndConverter>(columns.size());
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
            List<IColumn> cols = this.columnLoader.loadColumns(toLoad);
            for (IColumn c: cols)
                this.columns.put(c.getName(), c);
        }
        for (ColumnAndConverterDescription column : columns) {
            String name = column.columnName;
            IColumn col = this.columns.get(name);
            if (col == null)
                throw new RuntimeException("Cannot get column " + name);
            result.add(new ColumnAndConverter(col, column.getConverter()));
        }
        return result;
    }

    @Nullable
    @Override
    public String getSourceFile() {
        return this.sourceFile;
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

    /**
     * Creates a new table that has the same columns but a different set of rows.
     * @param set: Membership set of the resulting table.
     */
    @Override
    public ITable selectRowsFromFullTable(IMembershipSet set) {
        return new Table(this.getColumns(), set, this.sourceFile, this.columnLoader);
    }

    /**
     * Generates a table that contains all the columns, and only
     * the rows contained in IMembership Set members with consecutive numbering.
     */
    public SmallTable compress() {
        return this.compress(this.getSchema(), this.members);
    }
}
