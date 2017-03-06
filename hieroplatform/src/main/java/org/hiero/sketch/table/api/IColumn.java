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

package org.hiero.sketch.table.api;

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.ObjectArrayColumn;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Interface describing operations on a column.
 * A column is just a big vector of values.
 */
public interface IColumn {
    ColumnDescription getDescription();

    /* Only one of the following methods is supposed to work for a column */
    String getString(int rowIndex);
    double getDouble(int rowIndex);
    LocalDateTime getDate(int rowIndex);
    int getInt(int rowIndex);
    Duration getDuration(int rowIndex);
    /* This function is inefficient, it should be used sparingly. It
       will cast the value to an Object, boxing it if necessary. It returns null
       if the row is missing.
     */
    default Object getObject(final int rowIndex) {
        if (this.isMissing(rowIndex)) { return null; }
        switch (this.getDescription().kind) {
            case String:
                return this.getString(rowIndex);
            case Date:
                return this.getDate(rowIndex);
            case Int:
                return this.getInt(rowIndex);
            case Json:
                return this.getString(rowIndex);
            case Double:
                return this.getDouble(rowIndex);
            case Duration:
                return this.getDuration(rowIndex);
            default:
                throw new RuntimeException("Unexpected data type");
        }
    }

    /**
     * @param rowIndex Row to check
     * @return True if the data in the specified row is missing.
     */
    boolean isMissing(int rowIndex);

    /**
     * @return Number of rows in the column.
     */
    int sizeInRows();

    /**
     * Whatever the internal data type, return a double.
     */
    double asDouble(int rowIndex, @Nullable IStringConverter converter);

    String asString(int rowIndex);

    IndexComparator getComparator();

    /**
     * Compresses an IColumn to an ObjectArrayColumn, ordered according to the specified rowOrder
     * @param rowOrder specifies the set of rows and their order
     * @return An ObjectArrayColumn with the specified sequence of rows
     */
    default IColumn compress(final IRowOrder rowOrder) {
        final IRowIterator rowIt = rowOrder.getIterator();
        final ObjectArrayColumn result = new ObjectArrayColumn(
                this.getDescription(), rowOrder.getSize());
        int row = 0;
        while (true) {
            final int i = rowIt.getNextRow();
            if (i == -1) {
                break;
            }
            result.set(row, this.getObject(i));
            row++;
        }
        return result;
    }

    default String getName() {
        return getDescription().name;
    }
}