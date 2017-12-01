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

import org.hillview.table.Schema;
import org.hillview.table.SmallTable;

/**
 * An ITable object has a schema, a set of columns, and a MembershipSet.
 * All columns have the same size.
 */
public interface ITable {
    Schema getSchema();

    IRowIterator getRowIterator();

    /**
     * Describes the set of rows that are really present in the table.
     */
    IMembershipSet getMembershipSet();

    int getNumOfRows();

    /**
     * Creates a small table by keeping only the rows in the IRowOrder and
     * the columns in the subSchema.
     *
     * @param subSchema Indicates columns to keep.
     * @param rowOrder  Indicates rows to keep.
     */
    SmallTable compress(ISubSchema subSchema, IRowOrder rowOrder);

    SmallTable compress(IRowOrder rowOrder);

    /**
     * Gets the columns in the order they appear in the schema.
     * The columns must have already been loaded.
     * @param schema  Schema indicating which columns to load.
     */
    IColumn[] getColumns(Schema schema);

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
    ITable replace(IColumn[] columns);

    /**
     * Append a bunch of columns to this table.
     */
    ITable append(IColumn[] columns);

    /**
     * Returns the specified columns.  Ensures that the columns are loaded.
     */
    ColumnAndConverter[] getLoadedColumns(ColumnAndConverterDescription[] columns);

    /**
     * Returns the specified column.  Ensures that the column is loaded.
     */
    default ColumnAndConverter getLoadedColumn(ColumnAndConverterDescription column) {
        ColumnAndConverterDescription[] cols = new ColumnAndConverterDescription[] { column };
        ColumnAndConverter[] result = this.getLoadedColumns(cols);
        return result[0];
    }

    default ColumnAndConverter getLoadedColumn(String columnName) {
        ColumnAndConverterDescription desc = new ColumnAndConverterDescription(columnName);
        return this.getLoadedColumn(desc);
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
}