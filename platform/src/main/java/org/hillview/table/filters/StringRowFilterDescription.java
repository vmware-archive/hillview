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

public class StringRowFilterDescription implements ITableFilterDescription {
    /**
     * The name of the column on which the filtering operation is performed.
     */
    private final String colName;
    /**
     * The description of the filter that will be used.
     */
    private final StringFilterDescription stringFilterDescription;

    public StringRowFilterDescription(String colName, StringFilterDescription
            stringFilterDescription) {
        this.colName = colName;
        this.stringFilterDescription = stringFilterDescription;
    }

    @Override
    public ITableFilter getFilter(ITable table) {
        return new StringRowFilter(table);
    }

    /**
     * This filter maps a given Table to a Table that only contains the given value in the
     * specified column.
     */
    public class StringRowFilter implements ITableFilter {
        private final IColumn column;
        private final IStringFilter stringFilter;
        StringRowFilter(ITable table) {
            this.stringFilter = StringFilterFactory.getFilter(stringFilterDescription);
            this.column = table.getLoadedColumn(StringRowFilterDescription.this.colName);
        }

        /**
         * @return Whether the value at the specified row index matches to the compare value.
         */
        @Override
        public boolean test(int rowIndex) {
            return this.stringFilter.test(column.asString(rowIndex));
        }
    }
}
