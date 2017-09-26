/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.hillview.table.filters;

import org.hillview.table.api.ITable;
import org.hillview.table.api.ITableFilter;
import org.hillview.table.api.ITableFilterDescription;
import java.io.Serializable;

@SuppressWarnings("CanBeFinal")
public class RangeFilterPair implements ITableFilterDescription, Serializable {
    public final RangeFilterDescription first;
    public final RangeFilterDescription second;

    public RangeFilterPair(RangeFilterDescription first, RangeFilterDescription second) {
        this.first = first;
        this.second = second;
    }

    public ITableFilter getFilter(ITable table) {
        ITableFilter t1 = this.first.getFilter(table);
        ITableFilter t2 = this.second.getFilter(table);
        return new Range2DFilter(t2, t2);
    }

    public static class Range2DFilter implements ITableFilter {
        final ITableFilter first;
        final ITableFilter second;

        Range2DFilter(ITableFilter first, ITableFilter second) {
            this.first = first;
            this.second = second;
        }

        public boolean test(int rowIndex) {
            return this.first.test(rowIndex) && this.second.test(rowIndex);
        }
    }
}
