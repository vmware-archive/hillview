/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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

    IColumn getColumn(String colName);

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
     * Returns columns in the order they appear in the schema.
     */
    Iterable<IColumn> getColumns(Schema schema);

    /**
     * Returns columns in the order they appear in the schema.
     */
    Iterable<IColumn> getColumns();

    default ColumnAndConverter getColumn(ColumnNameAndConverter cc) {
        return new ColumnAndConverter(this.getColumn(cc.columnName), cc.converter);
    }

    /**
     * Formats the first rows in the table as a long string.
     */
    String toLongString(int rowsToDisplay);
}