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

import org.hillview.table.api.ITable;
import org.hillview.table.api.ITableFilter;
import org.hillview.table.api.ITableFilterDescription;
import java.io.Serializable;

public final class RangeFilterPairDescription implements ITableFilterDescription, Serializable {
    public final RangeFilterDescription first;
    public final RangeFilterDescription second;

    public RangeFilterPairDescription(RangeFilterDescription first, RangeFilterDescription second) {
        this.first = first;
        this.second = second;
    }

    public ITableFilter getFilter(ITable table) {
        // The semantics of this class is a big convoluted: the two filters
        // are not applied independently.  Each of them indicates a filtering
        // operation on some axis.  The "complement" bit should be the same in both,
        // and it should apply to the And of the two filters.  That's why we use
        // an Or filter if the filter is complemented.
        ITableFilter t1 = this.first.getFilter(table);
        ITableFilter t2 = this.second.getFilter(table);
        if (this.first.complement) {
            assert this.second.complement;
            return new OrFilter(t1, t2);
        } else {
            assert !this.second.complement;
            return new AndFilter(t1, t2);
        }
    }
}
