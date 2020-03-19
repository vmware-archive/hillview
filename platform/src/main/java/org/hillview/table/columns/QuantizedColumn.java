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
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;

/**
 * A quantized column has an attached quantization policy and another (real) column.
 * The quantized column mediates access to the column data; when queried it
 * does not return the real values; each value is fit within a bucket of
 * the quantization policy and treated as if it is the lower bound of the bucket.
 */
public class QuantizedColumn extends BaseColumn {
    static final long serialVersionUID = 1;

    private final IColumn data;
    /**
     * Quantization policy.  If this is null the data is not really quantized.
     */
    @Nullable
    private final ColumnQuantization quantization;

    public QuantizedColumn(IColumn data, @Nullable ColumnQuantization quantization) {
        super(data.getDescription());
        if (quantization != null) {
            if (data.getKind().isString()) {
                if (!(quantization instanceof StringColumnQuantization))
                    throw new IllegalArgumentException(
                            "Quantization should be String, but it's " +
                                    quantization.getClass().toString());
            } else {
                if (!(quantization instanceof DoubleColumnQuantization)) {
                    throw new IllegalArgumentException(
                            "Quantization should be Double, but it's " +
                                    quantization.getClass().toString());
                }
            }
        }
        this.data = data;
        this.quantization = quantization;
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
        if (this.quantization == null)
            return this.data.asDouble(rowIndex);
        switch (this.description.kind) {
            case Json:
            case String:
                throw new RuntimeException("Not supported for private string columns");
            case Integer:
                return this.quantization.roundDown(this.data.getInt(rowIndex));
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
        if (this.quantization == null)
            return this.data.getDouble(rowIndex);
        return this.quantization.roundDown(this.data.getDouble(rowIndex));
    }

    @Override
    public Instant getDate(final int rowIndex) {
        double d = this.getDouble(rowIndex);
        return Converters.toDate(d);
    }

    @Override
    public int getInt(final int rowIndex) {
        int v = this.data.getInt(rowIndex);
        if (this.quantization == null)
            return v;
        return Utilities.toInt(this.quantization.roundDown(v));
    }

    @Override
    public Duration getDuration(final int rowIndex) {
        double d = this.getDouble(rowIndex);
        return Converters.toDuration(d);
    }

    @Override
    public String getString(final int rowIndex) {
        if (this.quantization == null)
            return this.data.getString(rowIndex);
        return this.quantization.roundDown(this.data.getString(rowIndex));
    }

    /**
     * Returns 'true' for missing value, but also
     * returns 'true' for values out of the min-max range.
     */
    @Override
    public boolean isMissing(final int rowIndex) {
        if (this.data.isMissing(rowIndex))
            return true;
        if (this.quantization == null)
            return false;
        switch (QuantizedColumn.this.description.kind) {
            case Json:
            case String:
                return this.quantization.outOfRange(this.data.getString(rowIndex));
            case Integer:
            case Duration:
            case Double:
            case Date:
                return this.quantization.outOfRange(this.data.asDouble(rowIndex));
            default:
                throw new RuntimeException("Unexpected data type");
        }
    }

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
                final boolean iMissing = QuantizedColumn.this.isMissing(i);
                final boolean jMissing = QuantizedColumn.this.isMissing(j);
                if (iMissing && jMissing) {
                    return 0;
                } else if (iMissing) {
                    return 1;
                } else if (jMissing) {
                    return -1;
                } else {
                    switch (QuantizedColumn.this.description.kind) {
                        case Json:
                        case String:
                            String si = QuantizedColumn.this.getString(i);
                            String sj = QuantizedColumn.this.getString(j);
                            assert si != null;
                            assert sj != null;
                            return si.compareTo(sj);
                        case Integer:
                            return Integer.compare(QuantizedColumn.this.getInt(i),
                                    QuantizedColumn.this.getInt(j));
                        case Duration:
                        case Double:
                        case Date:
                            return Double.compare(QuantizedColumn.this.getDouble(i),
                                    QuantizedColumn.this.getDouble(j));
                        default:
                            throw new RuntimeException("Unexpected data type");
                    }
                }
            }
        };
    }

    @Override
    public IColumn rename(String newName) {
        return new QuantizedColumn(this.data.rename(newName), this.quantization);
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

    @Override
    public String toString() {
        return "Quantized:" + this.data.toString();
    }
}
