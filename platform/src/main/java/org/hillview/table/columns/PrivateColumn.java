/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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
import org.hillview.table.api.*;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;

/**
 * A private column has an attached privacy policy and another (real) column.
 * The private column mediates access to the column data; when queried it
 * does not return the real values; each value is fit within a bucket of
 * the privacy policy and treated as if it is the lower bound of the bucket.
 */
public class PrivateColumn extends BaseColumn {
    private final IColumn data;
    private final ColumnPrivacyMetadata metadata;

    public PrivateColumn(IColumn data, ColumnPrivacyMetadata metadata) {
        super(data.getDescription());
        if (data.getKind().isString()) {
            if (!(metadata instanceof StringColumnPrivacyMetadata))
                throw new IllegalArgumentException("Privacy metadata should be String, but it's " + metadata.getClass().toString());
        } else if (data.getKind() == ContentsKind.Integer) {
            if (!(metadata instanceof IntColumnPrivacyMetadata))
                throw new IllegalArgumentException("Privacy metadata should be Int, but it's " + metadata.getClass().toString());
        } else {
            if (!(metadata instanceof DoubleColumnPrivacyMetadata)) {
                throw new IllegalArgumentException("Privacy metadata should be Double, but it's " + metadata.getClass().toString());
            }
        }
        this.data = data;
        this.metadata = metadata;
    }

    @Override
    public boolean isLoaded() {
        return this.data.isLoaded();
    }

    @Override
    public int sizeInRows() {
        // Note: this is not private!
        return this.data.sizeInRows();
    }

    @Override
    public double asDouble(int rowIndex) {
        switch (this.description.kind) {
            case Json:
            case String:
                throw new RuntimeException("Not supported for private string columns");
            case Integer:
                return this.metadata.roundDown(this.data.getInt(rowIndex));
            case Date:
            case Double:
            case Duration:
                return this.getDouble(rowIndex);
            default:
                throw new RuntimeException("Unexpected data type");
        }
    }

    @Override
    public double getDouble(final int rowIndex) {
        return this.metadata.roundDown(this.data.getDouble(rowIndex));
    }

    @Override
    public Instant getDate(final int rowIndex) {
        double d = this.getDouble(rowIndex);
        return Converters.toDate(d);
    }

    @Override
    public int getInt(final int rowIndex) {
        int v = this.data.getInt(rowIndex);
        return this.metadata.roundDown(v);
    }

    @Override
    public Duration getDuration(final int rowIndex) {
        double d = this.getDouble(rowIndex);
        return Converters.toDuration(d);
    }

    @Override
    public String getString(final int rowIndex) {
        return this.metadata.roundDown(this.data.getString(rowIndex));
    }

    @Override
    public boolean isMissing(final int rowIndex) { return this.data.isMissing(rowIndex); }

    @Nullable
    @Override
    public String asString(int rowIndex) {
        throw new RuntimeException("Operation not supported on private columns");
    }

    @Override
    public IndexComparator getComparator() {
        return new IndexComparator() {
            @Override
            public int compare(final int i, final int j) {
                final boolean iMissing = PrivateColumn.this.isMissing(i);
                final boolean jMissing = PrivateColumn.this.isMissing(j);
                if (iMissing && jMissing) {
                    return 0;
                } else if (iMissing) {
                    return 1;
                } else if (jMissing) {
                    return -1;
                } else {
                    switch (PrivateColumn.this.description.kind) {
                        case Json:
                        case String:
                            String si = PrivateColumn.this.getString(i);
                            String sj = PrivateColumn.this.getString(j);
                            assert si != null;
                            assert sj != null;
                            return si.compareTo(sj);
                        case Integer:
                            return Integer.compare(PrivateColumn.this.getInt(i),
                                    PrivateColumn.this.getInt(j));
                        case Duration:
                        case Double:
                        case Date:
                            return Double.compare(PrivateColumn.this.getDouble(i),
                                    PrivateColumn.this.getDouble(j));
                        default:
                            throw new RuntimeException("Unexpected data type");
                    }
                }
            }
        };
    }

    @Override
    public IColumn rename(String newName) {
        return new PrivateColumn(this.data.rename(newName), this.metadata);
    }

    @Override
    public IColumn convertKind(ContentsKind kind, String newColName, IMembershipSet set) {
        throw new RuntimeException("Cannot convert data in a private column");
    }

    @Override
    public long hashCode64(int rowIndex, LongHashFunction hash) {
        if (this.isMissing(rowIndex))
            return 0;
        switch (this.description.kind) {
            case Json:
            case String:
                String s = this.getString(rowIndex);
                return hash.hashChars(Converters.checkNull(s));
            case Integer:
                return hash.hashInt(this.getInt(rowIndex));
            case Date:
            case Double:
            case Duration:
                return hash.hashLong(Double.doubleToRawLongBits(this.getDouble(rowIndex)));
            default:
                throw new RuntimeException("Unexpected data type");
        }
    }
}
