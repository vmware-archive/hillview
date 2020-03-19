/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

package org.hillview.table.filters;

import org.hillview.table.ColumnDescription;
import org.hillview.table.api.*;
import org.hillview.utils.Converters;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.util.function.Predicate;

/**
 * A filter that describes how values in a column should be compared with a constant.
 */
public class ComparisonFilterDescription implements ITableFilterDescription {
    static final long serialVersionUID = 1;
    
    private final ColumnDescription column;
    // Only one of the two below is set, depending on the column kind.
    @Nullable
    private final String stringValue;
    @Nullable
    private final Double doubleValue;
    private final String comparison;

    /**
     * Make a filter that accepts rows that have a specified value in the specified
     * column.
     * @param column Column that is compared.
     * @param stringValue String value that is compared.  May be null.
     * @param doubleValue Double value that is compared.  May be null.
     *                    The value will be to the left of the comparison.
     * @param comparison Operation for comparison: one of "==", "!=", "<", ">", "<=", ">="
     */
    public ComparisonFilterDescription(
            ColumnDescription column, @Nullable String stringValue,
            @Nullable Double doubleValue, String comparison) {
        this.column = column;
        this.doubleValue = doubleValue;
        this.stringValue = stringValue;
        this.comparison = comparison;
    }

    @Override
    public ITableFilter getFilter(ITable table) {
        return new ComparisonFilter(table);
    }

    /**
     * This filter maps a given Table to a Table that only contains the given value in the
     * specified column.
     */
    public class ComparisonFilter implements ITableFilter {
        private final IColumn column;
        private final Predicate<Integer> comparator;

        ComparisonFilter(ITable table) {
            boolean isNull;
            if (ComparisonFilterDescription.this.column.kind.isString())
                isNull = ComparisonFilterDescription.this.stringValue == null;
            else
                isNull = ComparisonFilterDescription.this.doubleValue == null;

            this.column = table.getLoadedColumn(ComparisonFilterDescription.this.column.name);
            if (isNull) {
                switch (ComparisonFilterDescription.this.comparison) {
                    case "<":
                    case "<=":
                        this.comparator = index -> true;
                        return;
                    case "==":
                        this.comparator = this.column::isMissing;
                        return;
                    case ">":
                        this.comparator = index -> false;
                        return;
                    case "!=":
                    case ">=":
                        this.comparator = index -> !this.column.isMissing(index);
                        return;
                    default:
                        throw new RuntimeException("Unexpected comparison operation " +
                                ComparisonFilterDescription.this.comparison);
                }
            }

            switch (this.column.getKind()) {
                case String:
                case Json:
                    String s = ComparisonFilterDescription.this.stringValue;
                    assert s != null;
                    switch (ComparisonFilterDescription.this.comparison) {
                        case "==":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return false;
                                String str = this.column.getString(index);
                                assert str != null;
                                return s.equals(str);
                            };
                            return;
                        case "!=":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return true;
                                String str = this.column.getString(index);
                                assert str != null;
                                return !s.equals(str);
                            };
                            return;
                        case ">":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return false;
                                String str = this.column.getString(index);
                                assert str != null;
                                return s.compareTo(str) > 0;
                            };
                            return;
                        case "<":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return true;
                                String str = this.column.getString(index);
                                assert str != null;
                                return s.compareTo(str) < 0;
                            };
                            return;
                        case "<=":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return true;
                                String str = this.column.getString(index);
                                assert str != null;
                                return s.compareTo(str) <= 0;
                            };
                            return;
                        case ">=":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return false;
                                String str = this.column.getString(index);
                                assert str != null;
                                return s.compareTo(str) >= 0;
                            };
                            return;
                        default:
                            throw new RuntimeException("Unexpected comparison operation " +
                                    ComparisonFilterDescription.this.comparison);
                    }
                case Integer:
                    Converters.checkNull(ComparisonFilterDescription.this.doubleValue);
                    int i = Utilities.toInt(ComparisonFilterDescription.this.doubleValue);
                    switch (ComparisonFilterDescription.this.comparison) {
                        case "==":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return false;
                                int x = this.column.getInt(index);
                                return i == x;
                            };
                            return;
                        case "!=":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return true;
                                int x = this.column.getInt(index);
                                return i != x;
                            };
                            return;
                        case ">":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return true;
                                int x = this.column.getInt(index);
                                return i > x;
                            };
                            return;
                        case "<":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return false;
                                int x = this.column.getInt(index);
                                return i < x;
                            };
                            return;
                        case "<=":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return true;
                                int x = this.column.getInt(index);
                                return i <= x;
                            };
                            return;
                        case ">=":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return false;
                                int x = this.column.getInt(index);
                                return i >= x;
                            };
                            return;
                        default:
                            throw new RuntimeException("Unexpected comparison operation " +
                                    ComparisonFilterDescription.this.comparison);
                    }
                case Double:
                case Duration:
                case Date:
                    assert ComparisonFilterDescription.this.doubleValue != null;
                    double d = ComparisonFilterDescription.this.doubleValue;
                    switch (ComparisonFilterDescription.this.comparison) {
                        case "==":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return false;
                                double x = this.column.getDouble(index);
                                return d == x;
                            };
                            return;
                        case "!=":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return true;
                                double x = this.column.getDouble(index);
                                return d != x;
                            };
                            return;
                        case ">":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return true;
                                double x = this.column.getDouble(index);
                                return d > x;
                            };
                            return;
                        case "<":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return false;
                                double x = this.column.getDouble(index);
                                return d < x;
                            };
                            return;
                        case "<=":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return true;
                                double x = this.column.getDouble(index);
                                return d <= x;
                            };
                            return;
                        case ">=":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return false;
                                double x = this.column.getDouble(index);
                                return d >= x;
                            };
                            return;
                        default:
                            throw new RuntimeException("Unexpected comparison operation " +
                                    ComparisonFilterDescription.this.comparison);
                    }
                default:
                    throw new RuntimeException("Unexpected kind " + this.column.getKind());
            }
        }

        /**
         * @return Whether the value at the specified row index matches to the compare value.
         */
        @Override
        public boolean test(int rowIndex) {
            return this.comparator.test(rowIndex);
        }
    }
}
