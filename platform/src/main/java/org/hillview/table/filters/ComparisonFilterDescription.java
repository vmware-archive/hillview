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

import org.hillview.table.api.*;

import javax.annotation.Nullable;
import java.util.function.Predicate;

/**
 * A filter that describes how values in a column should be compared with a constant.
 */
public class ComparisonFilterDescription implements ITableFilterDescription {
    public final String column;
    @Nullable
    public final String compareValue;
    public final String comparison;

    /**
     * Make a filter that accepts rows that (do not) have a specified value in the specified
     * column.
     * @param column Column that is compared.
     * @param compareValue Value that is compared.  The value will be to the left of the comparison.
     * @param comparison Operation for comparison: one of "==", "!=", "<", ">", "<=", ">="
     */
    public ComparisonFilterDescription(
            String column, @Nullable String compareValue, String comparison) {
        this.column = column;
        this.compareValue = compareValue;
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
        private final ColumnAndConverter column;
        private final Predicate<Integer> comparator;

        public ComparisonFilter(ITable table) {
            ColumnAndConverterDescription ccd = new ColumnAndConverterDescription
                    (ComparisonFilterDescription.this.column);
            this.column = table.getLoadedColumn(ccd);
            if (ComparisonFilterDescription.this.compareValue == null) {
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

            switch (this.column.column.getKind()) {
                case Category:
                case String:
                case Json:
                    String s = ComparisonFilterDescription.this.compareValue;
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
                                    return true;
                                String str = this.column.getString(index);
                                assert str != null;
                                return s.compareTo(str) > 0;
                            };
                            return;
                        case "<":
                            this.comparator = index -> {
                                if (this.column.isMissing(index))
                                    return false;
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
                    int i = Integer.parseInt(ComparisonFilterDescription.this.compareValue);
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
                    double d = Double.parseDouble(ComparisonFilterDescription.this.compareValue);
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
                    throw new RuntimeException("Unexpected kind " + this.column.column.getKind());
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
