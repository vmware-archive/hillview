/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.hillview.table.api;

import net.openhft.hashing.LongHashFunction;

import javax.annotation.Nullable;

public interface IDoubleColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, @Nullable final IStringConverter unused) {
        if (isMissing(rowIndex))
            throw new MissingException(this, rowIndex);
        return this.getDouble(rowIndex);
    }

    @Nullable
    @Override
    default String asString(final int rowIndex) {
        if (this.isMissing(rowIndex))
            return null;
        return Double.toString(this.getDouble(rowIndex));
    }

    @Override
    default IndexComparator getComparator() {
        return new IndexComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
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
        if (isMissing(rowIndex)) return MISSING_HASH_VALUE;
        return hash.hashLong(Double.doubleToRawLongBits(this.getDouble(rowIndex)));
    }

    @Override
    default IColumn convertKind(ContentsKind kind, String newColName, IMembershipSet set) {
        IMutableColumn newColumn = this.allocateConvertedColumn(kind, set, newColName);
        switch(kind) {
            case Category:
            case Json:
            case String:
                this.convert(newColumn, set, row -> Double.toString(this.getDouble(row)));
                break;
            case Integer:
                this.convert(newColumn, set, row -> (int)this.getDouble(row));
                break;
            case Double:
                this.convert(newColumn, set, this::getDouble);
                break;
            case Date:
            case Duration:
                throw new UnsupportedOperationException("Conversion from " + this.getKind()
                        + " to " + kind + " is not supported.");
            default:
                throw new RuntimeException("Unexpected column kind " + this.getKind());
        }
        return newColumn;
    }
}
