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
import org.hillview.table.columns.BaseArrayColumn;
import org.hillview.table.rows.RowSnapshot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Base class for in-memory tables.
 */
public abstract class BaseTable implements ITable, Serializable {
    /**
     * Maps columns name to an IColumn.
     */
    final HashMap<String, IColumn> columns;

    /**
     * @return An iterator over the rows in the table.
     */
    @Override public IRowIterator getRowIterator() {
        return this.getMembershipSet().getIterator();
    }

    <C extends IColumn> BaseTable(Iterable<C> columns) {
        BaseTable.columnSize(columns);  // validate column sizes
        sealColumns(columns);
        this.columns = new HashMap<String, IColumn>();
        for (final IColumn c : columns)
            this.columns.put(c.getName(), c);
    }

    /**
     * Returns columns in the order they appear in the schema.
     */
    public Iterable<IColumn> getColumns(Schema schema) {
        List<IColumn> cols = new ArrayList<IColumn>();
        for (String col : schema.getColumnNames()) {
            IColumn myCol = this.getColumn(col);
            cols.add(myCol);
        }
        return cols;
    }

    /**
     * Returns columns in the order they appear in the schema.
     */
    public Iterable<IColumn> getColumns() {
        return this.getColumns(this.getSchema());
    }

    BaseTable(Schema schema) {
        this.columns = new HashMap<String, IColumn>();
        for (final String c : schema.getColumnNames()) {
            ColumnDescription cd = schema.getDescription(c);
            this.columns.put(c, BaseArrayColumn.create(cd));
        }
    }

    BaseTable() {
        this.columns = new HashMap<>();
    }

    @Override
    public String toString() {
        return "Table, " + this.getSchema().getColumnCount() + " columns, " +
                this.getNumOfRows() + " rows";
    }

    public String toLongString(int rowsToDisplay) {
        return this.toLongString(0, rowsToDisplay);
    }

    /**
     * Compute the size common to all these columns.
     * @param columns A set of columns.
     * @return The common size, or 0 if the set is empty.
     * Throws if the columns do not all have the same size.
     */
    static <C extends IColumn> int columnSize(Iterable<C> columns) {
        int size = -1;
        for (IColumn c : columns) {
            if (size < 0)
                size = c.sizeInRows();
            else if (size != c.sizeInRows())
                throw new IllegalArgumentException("Columns do not have the same size");
        }
        if (size < 0)
            size = 0;
        return size;
    }

    @Override public IColumn getColumn(final String colName) {
        return this.columns.get(colName);
    }

    /**
     * Compress generates a table that contains only the columns referred to by subSchema,
     * and only the rows contained in IMembership Set with consecutive numbering.
     * The order among the columns is preserved.
     */
    @Override public SmallTable compress(final ISubSchema subSchema,
                                         final IRowOrder rowOrder) {
        Schema newSchema = this.getSchema().project(subSchema);
        List<IColumn> compressedCols = new ArrayList<IColumn>(newSchema.getColumnCount());
        for (String s : newSchema.getColumnNames()) {
            IColumn c = this.columns.get(s).compress(rowOrder);
            compressedCols.add(c);
        }
        return new SmallTable(compressedCols, newSchema);
    }

    /**
     * Version of Compress that defaults subSchema to the entire Schema.
     * @param rowOrder Ordered set of rows to include in the compressed table.
     * @return A compressed table containing only the rows contained in rowOrder.
     */
    @Override public SmallTable compress(final IRowOrder rowOrder) {
        final ISubSchema subSchema = new FullSubSchema();
        return this.compress(subSchema, rowOrder);
    }

    private String toLongString(int startRow, int rowsToDisplay) {
        final StringBuilder builder = new StringBuilder();
        builder.append(this.toString());
        builder.append(System.getProperty("line.separator"));
        final IRowIterator rowIt = this.getRowIterator();
        int nextRow = rowIt.getNextRow();
        while ((nextRow < startRow) && (nextRow != -1))
            nextRow = rowIt.getNextRow();
        int count = 0;
        while ((nextRow >= 0) && (count < rowsToDisplay)) {
            RowSnapshot rs = new RowSnapshot(this, nextRow);
            builder.append(rs.toString());
            builder.append(System.getProperty("line.separator"));
            nextRow = rowIt.getNextRow();
            count++;
        }
        return builder.toString();
    }

    static <C extends IColumn> void sealColumns(Iterable<C> columns) {
        for (C c: columns) {
            if (c instanceof IMutableColumn) {
                ((IMutableColumn)c).seal();
            } else if (c instanceof IAppendableColumn) {
                ((IAppendableColumn)c).seal();
            }
        }
    }
}