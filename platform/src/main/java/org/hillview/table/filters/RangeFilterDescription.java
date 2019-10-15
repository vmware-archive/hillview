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

import org.hillview.table.ColumnDescription;
import org.hillview.table.api.*;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.columns.DoubleColumnQuantization;
import org.hillview.table.columns.StringColumnQuantization;
import org.hillview.utils.Converters;

@SuppressWarnings("CanBeFinal")
public class RangeFilterDescription implements ITableFilterDescription {
    // Instances of this class are created by deserialization from JSON,
    // so these initializers are not useful.
    public ColumnDescription cd = new ColumnDescription();
    public double min = 0;
    public double max = 0;
    public String minString = "";
    public String maxString = "";
    public boolean complement = false;

    @Override
    public ITableFilter getFilter(ITable table) {
        IColumn col = table.getLoadedColumn(this.cd.name);
        if (this.cd.kind.isString())
            return new StringRangeFilter(col);
        else
            return new DoubleRangeFilter(col);
    }

    public class DoubleRangeFilter implements ITableFilter {
        final IColumn column;

        DoubleRangeFilter(IColumn column) {
            this.column = column;
        }

        public boolean test(int rowIndex) {
            RangeFilterDescription desc = RangeFilterDescription.this;
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
            return "Rangefilter[" + RangeFilterDescription.this.min + "," +
                    RangeFilterDescription.this.max + "]";
        }
    }

    public RangeFilterDescription intersect(RangeFilterDescription with) {
        RangeFilterDescription result = new RangeFilterDescription();
        result.cd = this.cd;
        if (this.cd.kind.isString()) {
            result.minString = Converters.checkNull(Converters.max(this.minString, with.minString));
            result.maxString = Converters.checkNull(Converters.min(this.maxString, with.maxString));
        } else {
            result.min = Math.max(this.min, with.min);
            result.max = Math.min(this.max, with.max);
        }
        result.complement = this.complement;
        return result;
    }

    public RangeFilterDescription intersect(ColumnQuantization with) {
        RangeFilterDescription result = new RangeFilterDescription();
        result.cd = this.cd;
        if (this.cd.kind.isString()) {
            StringColumnQuantization s = (StringColumnQuantization)with;
            result.minString = Converters.checkNull(Converters.max(this.minString, s.getMin()));
            result.maxString = Converters.checkNull(Converters.min(this.maxString, s.getMax()));
        } else {
            DoubleColumnQuantization q = (DoubleColumnQuantization)with;
            result.min = Math.max(this.min, q.globalMin);
            result.max = Math.min(this.max, q.globalMax);
        }
        result.complement = this.complement;
        return result;
    }

    public class StringRangeFilter implements ITableFilter {
        final IColumn column;

        StringRangeFilter(IColumn column) {
            this.column = column;
        }

        public boolean test(int rowIndex) {
            RangeFilterDescription desc = RangeFilterDescription.this;
            boolean result;
            if (this.column.isMissing(rowIndex))
                result = false;
            else {
                String s = this.column.getString(rowIndex);
                assert s != null;
                result = (s.compareTo(desc.minString) >= 0) && (s.compareTo(desc.maxString) <= 0);
            }
            if (desc.complement)
                result = !result;
            return result;
        }

        public String toString() {
            return "Rangefilter[" + RangeFilterDescription.this.minString + "," +
                    RangeFilterDescription.this.maxString + "]";
        }
    }
}
