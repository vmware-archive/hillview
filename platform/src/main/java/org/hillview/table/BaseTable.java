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
import org.hillview.utils.Utilities;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Base class for in-memory tables.
 * We make this serializable because otherwise the columns field is not serialized
 * when accessed by the SmallTable.
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


    <C extends IColumn> BaseTable(C[] columns) {
        this(Utilities.arrayToIterable(columns));
    }

    /**
     * Creates a BaseTable with empty columns.
     */
    BaseTable(Schema schema) {
        this.columns = new HashMap<String, IColumn>();
        for (final String c : schema.getColumnNames()) {
            ColumnDescription cd = schema.getDescription(c);
            this.columns.put(c, BaseArrayColumn.create(cd));
        }
    }

    BaseTable() {
        this.columns = new HashMap<String, IColumn>();
    }

    /**
     * Returns columns in the order they appear in the schema.
     */
    public IColumn[] getColumns(Schema schema) {
        IColumn[] cols = new IColumn[schema.getColumnCount()];
        String[] colNames = schema.getColumnNames();
        for (int i=0; i < colNames.length; i++) {
            String col = colNames[i];
            IColumn myCol = this.columns.get(col);
            cols[i] = myCol;
        }
        return cols;
    }

    /**
     * Returns columns in the order they appear in the schema.
     */
    public IColumn[] getColumns() {
        return this.getColumns(this.getSchema());
    }

    @Override
    public ITable append(IColumn[] columns) {
        IColumn[] cols = new IColumn[this.columns.size() + columns.length];
        IColumn[] old = this.getColumns();
        System.arraycopy(old, 0, cols, 0, old.length);
        System.arraycopy(columns, 0, cols, old.length, columns.length);
        return this.replace(cols);
    }

    @Override
    public String toString() {
        return "Table[" + this.getSchema().getColumnCount() +"x" + this.getNumOfRows() + "]"; }

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

    /**
     * Compress generates a table that contains only the columns referred to by subSchema,
     * and only the rows contained in IMembership Set with consecutive numbering.
     * The order among the columns is preserved.
     */
    @Override public SmallTable compress(final ISubSchema subSchema,
                                         final IRowOrder rowOrder) {
        Schema newSchema = this.getSchema().project(subSchema);
        String[] colNames = newSchema.getColumnNames();

        IColumn[] compressedCols = new IColumn[newSchema.getColumnCount()];
        for (int i=0; i < colNames.length; i++) {
            String s = colNames[i];
            IColumn c = this.columns.get(s).compress(rowOrder);
            compressedCols[i] = c;
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

    @Override
    public ITable insertColumn(IColumn column, int index) {
        IColumn[] result = new IColumn[this.columns.size() + 1];
        int insertIndex = 0;
        String[] colNames = this.getSchema().getColumnNames();
        for (int i = 0; i < colNames.length; i++) {
            if (i == index) {
                result[i] = column;
                insertIndex++;
            }
            result[insertIndex++] = this.columns.get(colNames[i]);
        }
        if (index == -1)
            result[insertIndex] = column;
        return this.replace(result);
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