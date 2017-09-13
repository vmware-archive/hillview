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
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.function.Function;

public interface IStringColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, @Nullable final IStringConverter converter) {
        if (isMissing(rowIndex))
            throw new MissingException(this, rowIndex);
        final String tmp = this.getString(rowIndex);
        return Converters.checkNull(converter).asDouble(Converters.checkNull(tmp));
    }

    @Nullable
    @Override
    default String asString(final int rowIndex) {
        return this.getString(rowIndex);
    }

    @Override
    default IndexComparator getComparator() {
        return new IndexComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
                final boolean iMissing = IStringColumn.this.isMissing(i);
                final boolean jMissing = IStringColumn.this.isMissing(j);
                if (iMissing && jMissing) {
                    return 0;
                } else if (iMissing) {
                    return 1;
                } else if (jMissing) {
                    return -1;
                } else {
                    return Converters.checkNull(IStringColumn.this.getString(i)).compareTo(
                            Converters.checkNull(IStringColumn.this.getString(j)));
                }
            }
        };
    }

    @Override
    default long hashCode64(int rowIndex, LongHashFunction hash) {
        if (isMissing(rowIndex))
            return MISSING_HASH_VALUE;
        //noinspection ConstantConditions
        return hash.hashChars(this.getString(rowIndex));
    }

    @Override
    default IColumn convertKind(ContentsKind kind, String newColName, IMembershipSet set) {
        IMutableColumn newColumn = this.allocateConvertedColumn(kind, set, newColName);
        switch(kind) {
            case Category:
            case Json:
            case String:
                this.convert(newColumn, set, this::getString);
                break;
            case Integer: {
                Function<Integer, Integer> f = rowIndex -> {
                    String s = this.getString(rowIndex);
                    //noinspection ConstantConditions
                    return Integer.parseInt(s);
                };
                this.convert(newColumn, set, f);
                break;
            }
            case Double: {
                Function<Integer, Double> f = rowIndex -> {
                    String s = this.getString(rowIndex);
                    //noinspection ConstantConditions
                    return Double.parseDouble(s);
                };
                this.convert(newColumn, set, f);
                break;
            }
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