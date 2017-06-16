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

package org.hillview.table.api;

import net.openhft.hashing.LongHashFunction;

import javax.annotation.Nullable;

public interface IIntColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, @Nullable final IStringConverter unused) {
        if (isMissing(rowIndex))
            throw new MissingException(this, rowIndex);
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
            public int compare(final Integer i, final Integer j) {
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
    default long hashCode64(int rowIndex, long seed) {
        if (isMissing(rowIndex))
            throw new MissingException(this, rowIndex);
        return LongHashFunction.xx(seed).hashInt(this.getInt(rowIndex));
    }
}
