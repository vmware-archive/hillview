/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

public interface IIntervalColumn extends IColumn {
    Double getValue(int rowIndex, boolean start);

    @Override
    default IColumn convertKind(
            ContentsKind kind, String newColName, IMembershipSet set) {
        IMutableColumn newColumn = this.allocateConvertedColumn(
                kind, set, newColName);
        switch(kind) {
            case Json:
                this.convert(newColumn, set, row -> "{ start: " + this.getValue(row, true) +
                        ", end: " + this.getValue(row, false));
                break;
            case String:
                this.convert(newColumn, set, this::asString);
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
        return newColumn;
    }

    @Override
    default IndexComparator getComparator() {
        // Lexicographic comparison start, end.
        return new IndexComparator() {
            @Override
            public int compare(final int i, final int j) {
                final boolean iMissing = IIntervalColumn.this.isMissing(i);
                final boolean jMissing = IIntervalColumn.this.isMissing(j);
                if (iMissing && jMissing) {
                    return 0;
                } else if (iMissing) {
                    return 1;
                } else if (jMissing) {
                    return -1;
                } else {
                    int first = Double.compare(IIntervalColumn.this.getValue(i, true),
                            IIntervalColumn.this.getValue(j, true));
                    if (first != 0)
                        return first;
                    return Double.compare(IIntervalColumn.this.getValue(i, false),
                            IIntervalColumn.this.getValue(j, false));
                }
            }
        };
    }

    @Override
    default long hashCode64(int rowIndex, LongHashFunction hash) {
        // Is this the right way to combine two hashes?
        return hash.hashLong(Double.doubleToRawLongBits(this.getValue(rowIndex, true)) ^
                hash.hashLong(Double.doubleToRawLongBits(this.getValue(rowIndex, false))));
    }
}
