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

import org.hillview.table.SortedStringsConverter;
import org.hillview.table.api.ITableFilter;
import org.hillview.table.api.ColumnAndConverter;
import org.hillview.table.api.IStringConverter;
import org.hillview.table.api.ITable;
import org.hillview.table.api.ITableFilterDescription;

import javax.annotation.Nullable;
import java.io.Serializable;

@SuppressWarnings("CanBeFinal")
public class RangeFilterDescription implements ITableFilterDescription {
    public String columnName = "";
    public double min;
    public double max;
    public boolean complement;
    @Nullable
    public String[] bucketBoundaries;  // only used for Categorical columns

    @Override
    public ITableFilter getFilter(ITable table) {
        IStringConverter converter = null;
        if (this.bucketBoundaries != null)
            converter = new SortedStringsConverter(
                    this.bucketBoundaries, (int)Math.ceil(this.min), (int)Math.floor(this.max));
        ColumnAndConverter column = new ColumnAndConverter(
                table.getColumn(this.columnName), converter);
        return new RangeFilter(column, this);
    }

    public static class RangeFilter implements ITableFilter, Serializable {
        final ColumnAndConverter column;
        final RangeFilterDescription description;

        public RangeFilter(ColumnAndConverter column, RangeFilterDescription desc) {
            this.column = column;
            this.description = desc;
        }

        public boolean test(int rowIndex) {
            boolean result;
            if (this.column.isMissing(rowIndex))
                result = false;
            else {
                double d = this.column.asDouble(rowIndex);
                result = this.description.min <= d && d <= this.description.max;
            }
            if (this.description.complement)
                result = !result;
            return result;
        }
    }
}
