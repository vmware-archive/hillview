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

package org.hillview.table.filters;

import org.hillview.table.api.*;

@SuppressWarnings("CanBeFinal")
public class DoubleRangeFilterDescription implements ITableFilterDescription {
    private String columnName = "";
    private double min;
    private double max;
    private boolean complement;

    @Override
    public ITableFilter getFilter(ITable table) {
        return new RangeFilter(table.getLoadedColumn(this.columnName));
    }

    public class RangeFilter implements ITableFilter {
        final IColumn column;

        RangeFilter(IColumn column) {
            this.column = column;
        }

        public boolean test(int rowIndex) {
            DoubleRangeFilterDescription desc = DoubleRangeFilterDescription.this;
            boolean result;
            if (this.column.isMissing(rowIndex))
                result = false;
            else {
                double d = this.column.asDouble(rowIndex);
                result = (desc.min <= d) && (d <= desc.max);
            }
            if (desc.complement)
                result = !result;
            return result;
        }

        public String toString() {
            return "Rangefilter[" + DoubleRangeFilterDescription.this.min + "," +
                    DoubleRangeFilterDescription.this.max + "]";
        }
    }
}
