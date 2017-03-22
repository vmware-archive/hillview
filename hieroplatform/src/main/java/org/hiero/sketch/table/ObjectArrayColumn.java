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

package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.IndexComparator;
import org.hiero.utils.Converters;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.LocalDateTime;

/*
 * Column of objects of any type; only for moving data around. Size of column expected to be small.
 */
public final class ObjectArrayColumn extends BaseArrayColumn {
    private final Object[] data;

    public ObjectArrayColumn(final ColumnDescription description, final int size) {
        super(description, size);
        this.data = new Object[size];
    }

    public ObjectArrayColumn(final ColumnDescription description,
                             final Object[] data) {
        super(description, data.length);
        this.data = data;
    }

    @Override
    public int sizeInRows() { return this.data.length; }

    @Override
    public double asDouble(final int rowIndex, @Nullable final IStringConverter converter) {
        switch (ObjectArrayColumn.this.description.kind) {
            case Category:
            case Json:
            case String:
                IStringConverter c = Converters.checkNull(converter);
                return c.asDouble(Converters.checkNull(this.getString(rowIndex)));
            case Date:
                return Converters.toDouble(Converters.checkNull(this.getDate(rowIndex)));
            case Int:
                return this.getInt(rowIndex);
            case Double:
                return this.getDouble(rowIndex);
            case Duration:
                return Converters.toDouble(Converters.checkNull(this.getDuration(rowIndex)));
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
            public int compare(final Integer i, final Integer j) {
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
                        case Category:
                        case String:
                            return Converters.checkNull(ObjectArrayColumn.this.getString(i)).compareTo(
                                    Converters.checkNull(ObjectArrayColumn.this.getString(j)));
                        case Date:
                            return Converters.checkNull(ObjectArrayColumn.this.getDate(i)).compareTo(
                                    Converters.checkNull(ObjectArrayColumn.this.getDate(j)));
                        case Int:
                            return Integer.compare(Converters.checkNull(ObjectArrayColumn.this.getInt(i)),
                                    Converters.checkNull(ObjectArrayColumn.this.getInt(j)));
                        case Double:
                            return Double.compare(Converters.checkNull(ObjectArrayColumn.this.getDouble(i)),
                                    Converters.checkNull(ObjectArrayColumn.this.getDouble(j)));
                        case Duration:
                            return Converters.checkNull(ObjectArrayColumn.this.getDuration(i)).
                                    compareTo(Converters.checkNull(ObjectArrayColumn.this.getDuration(j)));
                        default:
                            throw new RuntimeException("Unexpected data type");
                    }
                }
            }
        };
    }

    @Override
    public int getInt(final int rowIndex) {
        return (int)this.data[rowIndex];
    }

    @Override
    public double getDouble(final int rowIndex) {
        return (double)this.data[rowIndex];
    }

    @Override
    public LocalDateTime getDate(final int rowIndex) {
        return (LocalDateTime) this.data[rowIndex];
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
}
