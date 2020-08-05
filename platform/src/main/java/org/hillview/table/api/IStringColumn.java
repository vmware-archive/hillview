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
import org.hillview.table.RadixConverter;
import org.hillview.utils.Converters;
import org.hillview.utils.DateParsing;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.LocalTime;
import java.util.function.Function;

public interface IStringColumn extends IColumn {
    RadixConverter radixConverter = new RadixConverter();
    static double stringToDouble(@Nullable String s) {
        return radixConverter.asDouble(s);
    }

    @Override
    default double asDouble(final int rowIndex) {
        assert !isMissing(rowIndex);
        final String tmp = this.getString(rowIndex);
        return stringToDouble(tmp);
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
            public int compare(final int i, final int j) {
                String str1 = IStringColumn.this.getString(i);
                String str2 = IStringColumn.this.getString(j);
                return Converters.compareStrings(str1, str2);
            }
        };
    }

    @Override
    default long hashCode64(int rowIndex, LongHashFunction hash) {
        assert !isMissing(rowIndex);
        //noinspection ConstantConditions
        return hash.hashChars(this.getString(rowIndex));
    }

    class DateParserHelper implements Function<Integer, Double> {
        final IStringColumn column;
        @Nullable
        DateParsing parser = null;

        DateParserHelper(IStringColumn col) {
            this.column = col;
        }

        @Override
        @Nullable
        public Double apply(Integer index) {
            String s = this.column.getString(index);
            if (s == null)
                return null;
            if (this.parser == null)
                this.parser = new DateParsing(s);
            Instant i = this.parser.parse(s);
            return Converters.toDouble(i);
        }
    }

    @Override
    default IColumn convertKind(
            ContentsKind kind, String newColName, IMembershipSet set) {
        IMutableColumn newColumn = this.allocateConvertedColumn(
                kind, set, newColName);
        switch(kind) {
            case Json:
            case String:
                this.convertToString(newColumn, set, this::getString);
                break;
            case Integer: {
                Function<Integer, Integer> f = rowIndex -> {
                    String s = this.getString(rowIndex);
                    //noinspection ConstantConditions
                    if (s.trim().isEmpty())
                        return null;
                    return Integer.parseInt(s);
                };
                this.convertToInt(newColumn, set, f);
                break;
            }
            case Double: {
                Function<Integer, Double> f = rowIndex -> {
                    String s = this.getString(rowIndex);
                    //noinspection ConstantConditions
                    if (s.trim().isEmpty())
                        return null;
                    return Double.parseDouble(s);
                };
                this.convertToDouble(newColumn, set, f);
                break;
            }
            case Date: {
                Function<Integer, Double> p = new DateParserHelper(this);
                this.convertToDouble(newColumn, set, p);
                break;
            }
            case Time: {
                //noinspection ConstantConditions
                this.convertToDouble(newColumn, set, row -> Converters.toDouble(LocalTime.parse(this.getString(row))));
                break;
            }
            case Duration:
                throw new UnsupportedOperationException("Conversion from " + this.getKind()
                        + " to " + kind + " is not supported.");
            default:
                throw new RuntimeException("Unexpected column kind " + this.getKind());
        }
        return newColumn;
    }
}
