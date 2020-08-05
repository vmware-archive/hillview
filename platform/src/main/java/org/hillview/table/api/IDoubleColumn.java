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
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.time.*;
import java.util.function.BiFunction;

public interface IDoubleColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex) {
        assert !this.isMissing(rowIndex);
        return this.getDouble(rowIndex);
    }

    @Nullable
    @Override
    default String asString(final int rowIndex) {
        if (this.isMissing(rowIndex))
            return null;
        double d = this.getDouble(rowIndex);
        switch (this.getKind()) {
            case Double:
                return Double.toString(d);
            case Date:
                Instant dt = Converters.toDate(d);
                return Converters.toString(dt);
            case Duration:
                Duration du = Converters.toDuration(d);
                return du.toString();
            case Time:
                LocalTime t = Converters.toTime(d);
                return t.toString();
            default:
                throw new RuntimeException("Unexpected column kind " + this.getKind());
        }
    }

    @Override
    default IndexComparator getComparator() {
        return new IndexComparator() {
            @Override
            public int compare(final int i, final int j) {
                final boolean iMissing = IDoubleColumn.this.isMissing(i);
                final boolean jMissing = IDoubleColumn.this.isMissing(j);
                if (iMissing && jMissing) {
                    return 0;
                } else if (iMissing) {
                    return 1;
                } else if (jMissing) {
                    return -1;
                } else {
                    return Double.compare(IDoubleColumn.this.getDouble(i), IDoubleColumn.this.getDouble(j));
                }
            }
        };
    }

    @Override
    default long hashCode64(int rowIndex, LongHashFunction hash) {
        assert !isMissing(rowIndex);
        return hash.hashLong(Double.doubleToRawLongBits(this.getDouble(rowIndex)));
    }

    @Override
    default IColumn convertKind(ContentsKind kind, String newColName, IMembershipSet set) {
        IMutableColumn newColumn = this.allocateConvertedColumn(
                kind, set, newColName);
        switch (this.getKind()) {
            case Double: {
                switch (kind) {
                    case Json:
                    case String:
                        this.convertToString(newColumn, set, this::asString);
                        break;
                    case Integer:
                        this.convertToInt(newColumn, set, row -> Converters.toInt(this.getDouble(row)));
                        break;
                    case Double:
                        this.convertToDouble(newColumn, set, this::getDouble);
                        break;
                    case Time:
                    case Interval:
                    case Date:
                    case Duration:
                        throw new UnsupportedOperationException("Conversion from " + this.getKind()
                                + " to " + kind + " is not supported.");
                    default:
                        throw new RuntimeException("Unexpected column kind " + this.getKind());
                }
                break;
            }
            case Date:
                switch (kind) {
                    case Json:
                    case String:
                        this.convertToString(newColumn, set, this::asString);
                        break;
                    case Date:
                        this.convertToDouble(newColumn, set, this::getDouble);
                        break;
                    case Time:
                        this.convertToDouble(newColumn, set, row -> {
                            Instant i = Converters.toDate(this.getDouble(row));
                            return Converters.toDouble(LocalDateTime.ofInstant(i, ZoneId.systemDefault()).toLocalTime());
                        });
                    case Integer:
                    case Double:
                    case Duration:
                    case Interval:
                        throw new UnsupportedOperationException("Conversion from " + this.getKind()
                                + " to " + kind + " is not supported.");
                    default:
                        throw new RuntimeException("Unexpected column kind " + this.getKind());
                }
                break;
            case Duration:
                switch (kind) {
                    case Json:
                    case String:
                        this.convertToString(newColumn, set, this::asString);
                        break;
                    case Duration:
                        this.convertToDouble(newColumn, set, this::getDouble);
                        break;
                    case Integer:
                    case Double:
                    case Date:
                    case Time:
                        throw new UnsupportedOperationException("Conversion from " + this.getKind()
                                + " to " + kind + " is not supported.");
                    default:
                        throw new RuntimeException("Unexpected column kind " + this.getKind());
                }
                break;
            case Time:
                switch(kind) {
                    case Json:
                    case String:
                        this.convertToString(newColumn, set, this::asString);
                        break;
                    case Time:
                        this.convertToDouble(newColumn, set, this::getDouble);
                        break;
                    case Integer:
                    case Double:
                    case Date:
                    case Duration:
                        throw new UnsupportedOperationException("Conversion from " + this.getKind()
                                + " to " + kind + " is not supported.");
                    default:
                        throw new RuntimeException("Unexpected column kind " + this.getKind());
                }
                break;
            default:
                throw new RuntimeException("Unexpected column kind " + this.getKind());
        }
        return newColumn;
    }

    default double reduceDouble(BiFunction<Double, Double, Double> reducer) {
        boolean first = true;
        double result = 0;
        for (int i = 0; i < this.sizeInRows(); i++) {
            if (this.isMissing(i))
                continue;
            double row = this.getDouble(i);
            if (first) {
                result = row;
                first = false;
            } else {
                result = reducer.apply(result, row);
            }
        }
        if (first)
            throw new RuntimeException("No data");
        return result;
    }

    default double minDouble() {
        return this.reduceDouble(Math::min);
    }

    default double maxDouble() {
        return this.reduceDouble(Math::max);
    }
}
