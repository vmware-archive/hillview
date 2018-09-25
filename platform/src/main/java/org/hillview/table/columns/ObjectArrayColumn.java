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

package org.hillview.table.columns;

import net.openhft.hashing.LongHashFunction;
import org.hillview.table.ColumnDescription;
import org.hillview.table.api.*;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.time.Duration;
import java.time.Instant;

/*
 * Column of objects of any type; only for moving data around. Size of column expected to be small.
 */
public final class ObjectArrayColumn extends BaseArrayColumn {
    private final Object[] data;

    public ObjectArrayColumn(final ColumnDescription description, final int size) {
        super(description, size);
        this.data = new Object[size];
    }

    private ObjectArrayColumn(final ColumnDescription description,
                             final Object[] data) {
        super(description, data.length);
        this.data = data;
    }

    public IColumn rename(String newName) {
        return new ObjectArrayColumn(this.description.rename(newName), this.data);
    }

    @Override
    public int sizeInRows() { return this.data.length; }

    @Override
    public double asDouble(final int rowIndex) {
        switch (ObjectArrayColumn.this.description.kind) {
            case Json:
            case String:
                String str = this.getString(rowIndex);
                return IStringColumn.stringToDouble(str);
            case Integer:
                return this.getInt(rowIndex);
            case Date:
            case Double:
            case Duration:
                return this.getDouble(rowIndex);
            default:
                throw new RuntimeException("Unexpected data type");
        }
    }

    @Override
    public String asString(final int rowIndex) {
        return this.data[rowIndex].toString();
    }

    @Override
    public IndexComparator getComparator() {
        return new IndexComparator() {
            @Override
            public int compare(final int i, final int j) {
                final boolean iMissing = ObjectArrayColumn.this.isMissing(i);
                final boolean jMissing = ObjectArrayColumn.this.isMissing(j);
                if (iMissing && jMissing) {
                    return 0;
                } else if (iMissing) {
                    return 1;
                } else if (jMissing) {
                    return -1;
                } else {
                    switch (ObjectArrayColumn.this.description.kind) {
                        case Json:
                        case String:
                            String si = ObjectArrayColumn.this.getString(i);
                            String sj = ObjectArrayColumn.this.getString(j);
                            assert si != null;
                            assert sj != null;
                            return si.compareTo(sj);
                        case Date:
                            Instant ii = ObjectArrayColumn.this.getDate(i);
                            Instant ij = ObjectArrayColumn.this.getDate(j);
                            assert ii != null;
                            assert ij != null;
                            return ii.compareTo(ij);
                        case Integer:
                            return Integer.compare(ObjectArrayColumn.this.getInt(i),
                                    ObjectArrayColumn.this.getInt(j));
                        case Double:
                            return Double.compare(ObjectArrayColumn.this.getDouble(i),
                                    ObjectArrayColumn.this.getDouble(j));
                        case Duration:
                            Duration di = ObjectArrayColumn.this.getDuration(i);
                            Duration dj = ObjectArrayColumn.this.getDuration(j);
                            assert di != null;
                            assert dj != null;
                            return di.compareTo(dj);
                        default:
                            throw new RuntimeException("Unexpected data type");
                    }
                }
            }
        };
    }

    @Override
    public IColumn convertKind(
            ContentsKind kind, String newColName, IMembershipSet set) {
        throw new UnsupportedOperationException("Converting object columns");
    }

    @Override
    public int getInt(final int rowIndex) {
        return (int)this.data[rowIndex];
    }

    @Override
    public double getDouble(final int rowIndex) {
        if (this.getKind() == ContentsKind.Date)
            return Converters.toDouble((Instant)this.data[rowIndex]);
        else if (this.getKind() == ContentsKind.Duration)
            return Converters.toDouble((Duration)this.data[rowIndex]);
        return (double)this.data[rowIndex];
    }

    @Override
    public Instant getDate(final int rowIndex) {
        return (Instant) this.data[rowIndex];
    }

    @Override
    public Duration getDuration(final int rowIndex) {
        return (Duration)this.data[rowIndex];
    }
    @Override
    public String getString(final int rowIndex) {
        return (String)this.data[rowIndex];
    }

    public void set(final int rowIndex, @Nullable final Object value) {
        this.data[rowIndex] = value;
    }

    @Override
    public boolean isMissing(final int rowIndex) { return this.data[rowIndex] == null; }

    @Override
    public void setMissing(final int rowIndex) { this.set(rowIndex, null);}

    /**
     * Given two Columns left and right, merge them to a single Column, using the Boolean
     * array mergeLeft which represents the order in which elements merge.
     * mergeLeft[i] = true means the i^th element comes from the left column.
     * @param left The left column
     * @param right The right column
     * @param mergeLeft The order in which to merge the two columns.
     * @return The merged column.
     */
    public static ObjectArrayColumn mergeColumns(final IColumn left, final IColumn right,
                                                 final boolean[] mergeLeft) {
        if (mergeLeft.length != (left.sizeInRows() + right.sizeInRows())) {
            throw new InvalidParameterException("Length of mergeOrder must equal " +
                    "sum of lengths of the columns");
        }
        final ObjectArrayColumn merged = new
                ObjectArrayColumn(left.getDescription(), mergeLeft.length);
        int i = 0, j = 0, k = 0;
        while (k < mergeLeft.length) {
            if (mergeLeft[k]) {
                merged.set(k, left.getObject(i));
                i++;
            } else {
                merged.set(k, right.getObject(j));
                j++;
            }
            k++;
        }
        return merged;
    }

    @Override
    public long hashCode64(int rowIndex, LongHashFunction hash) {
        if (this.isMissing(rowIndex))
            return MISSING_HASH_VALUE;
        switch (ObjectArrayColumn.this.description.kind) {
            case Json:
            case String:
                String str = this.getString(rowIndex);
                assert str != null;
                return hash.hashChars(str);
            case Date:
                Instant inst = this.getDate(rowIndex);
                assert inst != null;
                return hash.hashLong(Double.doubleToLongBits(Converters.toDouble(inst)));
            case Integer:
                return hash.hashInt(this.getInt(rowIndex));
            case Double:
                return hash.hashLong(Double.doubleToLongBits(this.getDouble(rowIndex)));
            case Duration:
                Duration d = this.getDuration(rowIndex);
                assert d != null;
                return hash.hashLong(Double.doubleToLongBits(Converters.toDouble(d)));
            default:
                throw new RuntimeException("Unexpected data type");
        }
    }
}
