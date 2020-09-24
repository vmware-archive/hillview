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

import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.api.ITableFilter;
import org.hillview.table.api.ITableFilterDescription;
import org.hillview.utils.Utilities;

import java.util.HashSet;

/**
 * A filter that uses JavaScript to filter data.
 */
public class FilterListDescription implements ITableFilterDescription {
    static final long serialVersionUID = 1;

    private final String[] keep;
    private final String   column;

    public FilterListDescription(String[] keep, String column) {
        this.keep = keep;
        this.column = column;
    }

    class ListFilter implements ITableFilter {
        IColumn column;
        HashSet<String> keep;

        ListFilter(ITable table) {
            this.column = table.getLoadedColumn(FilterListDescription.this.column);
            this.keep = new HashSet<String>(Utilities.list(FilterListDescription.this.keep));
        }

        public boolean test(int rowIndex) {
            String s = this.column.asString(rowIndex);
            return this.keep.contains(s);
        }
    }

    @Override
    public ITableFilter getFilter(ITable table) {
        return new ListFilter(table);
    }
}
