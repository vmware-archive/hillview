/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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
import org.hillview.utils.Linq;

/**
 * Describes an array of RangeFilters and an optional complement.
 */
@SuppressWarnings("CanBeFinal")
public class RangeFilterArrayDescription implements ITableFilterDescription {
    public RangeFilterDescription[] filters = new RangeFilterDescription[0];
    public boolean complement;

    @Override
    public ITableFilter getFilter(ITable table) {
        String[] cols = Linq.map(this.filters, f -> f.cd.name, String.class);
        table.getLoadedColumns(cols);
        ITableFilter[] filters = Linq.map(this.filters, f -> f.getFilter(table), ITableFilter.class);
        ITableFilter result = new AndFilter(filters);
        if (this.complement)
            result = new NotFilter(result);
        return result;
    }
}

