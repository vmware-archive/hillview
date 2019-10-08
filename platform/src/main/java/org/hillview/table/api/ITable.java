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

package org.hillview.table.api;

import org.hillview.sketches.results.ColumnSortOrientation;
import org.hillview.table.RecordOrder;
import org.hillview.table.Schema;
import org.hillview.table.SmallTable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * An ITable object has a schema, a set of columns, and a MembershipSet.
 * All columns have the same size.
 */
public interface ITable {
    /**
     * The local name of the file where this table was loaded from.
     * This returns null if the table is not associated with a file.
     */
    @Nullable
    String getSourceFile();

    /**
     * The schema of the table, describing the set of columns.
     */
    Schema getSchema();

    /**
     * An iterator over the rows that are present in this table.
     */
    IRowIterator getRowIterator();

    /**
     * Describes the set of rows that are really present in the table.
     */
    IMembershipSet getMembershipSet();

    /**
     * The number of rows in the table.
     */
    int getNumOfRows();

    /**
     * Creates a small table by keeping only the rows in the IRowOrder and
     * the columns in the subSchema.
     * @param colNames Indicates columns to keep.
     * @param rowOrder  Indicates rows to keep.
     */
    SmallTable compress(String[] colNames, IRowOrder rowOrder);

    /**
     * Creates a small table by keeping only the rows in the IRowOrder and
     * the columns in the subSchema.
     * @param schema Indicates columns to keep.
     * @param rowOrder  Indicates rows to keep.
     */
    SmallTable compress(Schema schema, IRowOrder rowOrder);

    /**
     * Creates a small table by keeping only the rows in rowOrder.
     * @param rowOrder  Indicates rows to keep.
     */
    SmallTable compress(IRowOrder rowOrder);

    /**
     * Gets the columns in the order they appear in the schema.
     * The columns must have already been loaded.
     * @param schema  Schema indicating which columns to load.
     */
    List<IColumn> getColumns(Schema schema);

    /**
     * Creates a new table which has the same data with this one except the
     * provided membership set.  Note that the result can have more rows
     * than the original table - the original membership set is ignored.
     *
     * @param set: Membership set of the resulting table.
     */
    ITable selectRowsFromFullTable(IMembershipSet set);

    /**
     * Return a new table which only contains the specified columns.
     *
     * @param schema: Schema of the resulting table.
     */
    ITable project(Schema schema);

    /**
     * Create a new table based on this set of columns but
     * with the membershipSet of the original one.
     * @param columns  Set of columns for new table.
     */
    <T extends IColumn> ITable replace(List<T> columns);

    /**
     * Append a bunch of columns to this table.
     */
    <T extends IColumn> ITable append(List<T> columns);

    /**
     * Returns the specified columns.  Ensures that the columns are loaded.
     */
    List<IColumn> getLoadedColumns(List<String> columns);

    default List<IColumn> getLoadedColumns(String[] columns) {
        return this.getLoadedColumns(Arrays.asList(columns));
    }
    /**
     * Returns the specified column.  Ensures that the column is loaded.
     */
    default IColumn getLoadedColumn(String column) {
        List<String> cols = new ArrayList<String>(1);
        cols.add(column);
        List<IColumn> result = this.getLoadedColumns(cols);
        assert result.size() == 1;
        return result.get(0);
    }

    /**
     * Return a new table which has the exact same columns as the specified one plus one extra.
     * @param column  Column to insert.
     * @param index   Position where column is inserted.  If -1 the column is appended.
     * @return        A new table.
     */
    ITable insertColumn(IColumn column, int index);

    /**
     * Formats the first rows in the table as a long string.
     * @param rowsToDisplay How many rows to format.
     */
    String toLongString(int rowsToDisplay);

    /**
     * Create a new table where some columns have new names, as indicated
     * by the rename map.
     * @param renameMap  Map indicating how a column name should be renamed.
     */
    default ITable renameColumns(HashMap<String,String> renameMap) {
        List<IColumn> cols = new ArrayList<IColumn>(this.getSchema().getColumnCount());
        for (IColumn col: this.getColumns(this.getSchema())) {
            IColumn newCol = col;
            String name = col.getName();
            if (renameMap.containsKey(name))
                newCol = col.rename(renameMap.get(name));
            cols.add(newCol);
        }
        return this.replace(cols);
    }

    default RecordOrder getRecordOrder(boolean isAscending) {
        RecordOrder ro = new RecordOrder();
        for (String colName : this.getSchema().getColumnNames())
            ro.append(new ColumnSortOrientation(this.getSchema().getDescription(colName),
                    isAscending));
        return ro;
    }
}
