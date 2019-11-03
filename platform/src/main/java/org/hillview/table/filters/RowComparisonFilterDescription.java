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

package org.hillview.table.filters;

import org.hillview.table.RecordOrder;
import org.hillview.table.api.*;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.Utilities;

import java.util.function.Function;

/**
 * A filter that describes how values in a whole row should be compared with a given row..
 */
public class RowComparisonFilterDescription implements ITableFilterDescription {
    private final RowSnapshot row;
    private final RecordOrder order;
    private final String comparison;

    /**
     * Make a filter that accepts rows that compares in the specific way with the
     * given row.
     * @param order Column that is compared.
     * @param row   Set of values to compare with.  Should be compatible with the order.
     *              The row will be to the left of the comparison.
     * @param comparison Operation for comparison: one of "==", "!=", "<", ">", "<=", ">="
     */
    public RowComparisonFilterDescription(
            RowSnapshot row, RecordOrder order, String comparison) {
        this.row = row;
        this.order = order;
        this.comparison = comparison;
    }

    class CompareFilter implements ITableFilter {
        private final VirtualRowSnapshot vrs;
        private final Function<Integer, Boolean> convertComparison;

        CompareFilter(ITable table) {
            this.vrs = new VirtualRowSnapshot(
                    table, RowComparisonFilterDescription.this.order.toSchema());
            this.convertComparison = Utilities.convertComparison(
                    RowComparisonFilterDescription.this.comparison);
        }

        public boolean test(int rowIndex) {
            vrs.setRow(rowIndex);
            int compare = RowComparisonFilterDescription.this.row.compareTo(
                    vrs, RowComparisonFilterDescription.this.order);
            return this.convertComparison.apply(compare);
        }
    }

    @Override
    public ITableFilter getFilter(ITable table) {
        VirtualRowSnapshot vrs = new VirtualRowSnapshot(table, this.order.toSchema());
        return new CompareFilter(table);
    }
}
