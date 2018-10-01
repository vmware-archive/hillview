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

import net.openhft.hashing.LongHashFunction;
import org.hillview.table.*;
import org.hillview.table.columns.*;
import org.hillview.utils.HillviewLogger;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;

/**
 * Interface describing operations on a column.
 * A column is just a big vector of values.
 */
public interface IColumn extends Serializable {
    ColumnDescription getDescription();

    /* Only one of the following methods is supposed to work for a column */
    double getDouble(int rowIndex);
    int getInt(int rowIndex);
    @Nullable
    String getString(int rowIndex);
    @Nullable
    Instant getDate(int rowIndex);
    @Nullable
    Duration getDuration(int rowIndex);
    /**
     * Number of exceptions that have occurred during parsing.
     */
    int getParsingExceptionCount();

    /* This function is inefficient, it should be used sparingly. It
       will cast the value to an Object, boxing it if necessary. It returns null
       if the row is missing.
     */
    @Nullable
    default Object getObject(final int rowIndex) {
        if (rowIndex < 0)
            throw new RuntimeException("Checking for negative row " + rowIndex);
        if (this.isMissing(rowIndex)) { return null; }
        switch (this.getDescription().kind) {
            case Json:
            case String:
                return this.getString(rowIndex);
            case Date:
                return this.getDate(rowIndex);
            case Integer:
                return this.getInt(rowIndex);
            case Double:
                return this.getDouble(rowIndex);
            case Duration:
                return this.getDuration(rowIndex);
            default:
                throw new RuntimeException("Unexpected data type");
        }
    }

    /**
     * True if the data for the column is already present in memory.
     */
    boolean isLoaded();

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
    double asDouble(int rowIndex);

    // Returns null only if the object is missing.
    @Nullable
    String asString(int rowIndex);

    /**
     * @return A comparator which compares two rowIndexes according to the contents
     * of these rowIndexes in the column.
     */
    IndexComparator getComparator();

    /**
     * Compresses an IColumn to an ObjectArrayColumn, ordered according to the specified rowOrder
     * @param rowOrder specifies the set of rows and their order.
     * @return An ObjectArrayColumn with the specified sequence of rows.
     */
    default ObjectArrayColumn compress(final IRowOrder rowOrder) {
        final IRowIterator rowIt = rowOrder.getIterator();
        HillviewLogger.instance.info("Compressing column",
                "{0} to {1}", this.sizeInRows(), rowOrder.getSize());
        final ObjectArrayColumn result = new ObjectArrayColumn(
                this.getDescription(), rowOrder.getSize());
        int row = 0;
        while (true) {
            final int i = rowIt.getNextRow();
            if (i < 0)
                break;
            result.set(row, this.getObject(i));
            row++;
        }
        return result;
    }

    /**
     * Create a new column which shares the same data but has a different name.
     * @param newName  New name to use for column.
     */
    IColumn rename(String newName);

    default IMutableColumn allocateConvertedColumn(
        ContentsKind kind, IMembershipSet set, String newColName) {
        ColumnDescription cd = new ColumnDescription(newColName, kind);
        if (set.useSparseColumn())
            return new SparseColumn(cd, this.sizeInRows());

        switch (kind) {
            case Json:
            case String:
                return new StringArrayColumn(cd, this.sizeInRows());
            case Integer:
                return new IntArrayColumn(cd, this.sizeInRows());
            case Double:
                return new DoubleArrayColumn(cd, this.sizeInRows());
            case Date:
                return new DateArrayColumn(cd, this.sizeInRows());
            case Duration:
                return new DurationArrayColumn(cd, this.sizeInRows());
            default:
                throw new RuntimeException("Unexpected column kind " + kind);
        }
    }

    default <T> void convert(IMutableColumn dest, IMembershipSet set,
                             Function<Integer, T> converter) {
        IRowIterator it = set.getIterator();
        int rowIndex = it.getNextRow();
        while (rowIndex >= 0) {
            if (this.isMissing(rowIndex)) {
                dest.setMissing(rowIndex);
            } else {
                T result = converter.apply(rowIndex);
                if (result == null)
                    dest.setMissing(rowIndex);
                else
                    dest.set(rowIndex, result);
            }
            rowIndex = it.getNextRow();
        }
    }

    /**
     * Returns a copy of this column, in the specified kind.
     * @param kind       The kind of the destination column.
     * @param newColName Name of the new column.
     * @param set        Set of elements that have to be converted.
     * @return An IColumn that is a copy of this column, converted to the specified kind.
     */
    IColumn convertKind(ContentsKind kind, String newColName, IMembershipSet set);

    default String getName() {
        return this.getDescription().name;
    }

    default ContentsKind getKind() { return this.getDescription().kind;}

    /**
     * @return A 64 bit hash code for the item in the rowIndex.
     * returns MISSING_HASH_VALUE if item is missing.
     */
    long hashCode64(int rowIndex, LongHashFunction hash);

    long MISSING_HASH_VALUE = 0;
}
