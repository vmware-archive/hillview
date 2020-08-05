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

import javax.annotation.Nullable;
import java.util.function.BiFunction;

public interface IIntColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex) {
        assert !isMissing(rowIndex);
        return this.getInt(rowIndex);
    }

    @Nullable
    @Override
    default String asString(final int rowIndex) {
        if (this.isMissing(rowIndex))
            return null;
        return Integer.toString(this.getInt(rowIndex));
    }

    /**
     * @return Let x and y be values indexed by i and j. Returns sign(x - y) in {-1, 0, 1} where
     * sign(0) = 0. Missing values are treated as +infinity. Two missing values are equal.
     */
    default IndexComparator getComparator() {
        return new IndexComparator() {
            @Override
            public int compare(final int i, final int j) {
                final boolean iMissing = IIntColumn.this.isMissing(i);
                final boolean jMissing = IIntColumn.this.isMissing(j);
                if (iMissing && jMissing) {
                    return 0;
                } else if (iMissing) {
                    return 1;
                } else if (jMissing) {
                    return -1;
                } else {
                    return Integer.compare(IIntColumn.this.getInt(i), IIntColumn.this.getInt(j));
                }
            }
        };
    }

    @Override
    default long hashCode64(int rowIndex, LongHashFunction hash) {
        assert !this.isMissing(rowIndex);
        return hash.hashInt(this.getInt(rowIndex));
    }

    @Override
    default IColumn convertKind(
            ContentsKind kind, String newColName, IMembershipSet set) {
        IMutableColumn newColumn = this.allocateConvertedColumn(
                kind, set, newColName);
        switch(kind) {
            case Json:
            case String:
                this.convertToString(newColumn, set, this::asString);
                break;
            case Integer: {
                this.convertToInt(newColumn, set, this::getInt);
                break;
            }
            case Double: {
                this.convertToDouble(newColumn, set, row -> (double)this.getInt(row));
                break;
            }
            case Date:
            case Duration:
                throw new UnsupportedOperationException("Conversion from " + this.getKind()
                        + " to " + kind + " is not supported.");
            default:
                throw new RuntimeException("Unexpected column kind " + kind);
        }
        return newColumn;
    }

    default int reduceInt(BiFunction<Integer, Integer, Integer> reducer) {
        boolean first = true;
        int result = 0;
        for (int i = 0; i < this.sizeInRows(); i++) {
            if (this.isMissing(i))
                continue;
            int row = this.getInt(i);
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

    default int minInt() {
        return this.reduceInt(Math::min);
    }

    default int maxInt() {
        return this.reduceInt(Math::max);
    }
}
