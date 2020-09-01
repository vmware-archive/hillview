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

import org.hillview.table.Schema;
import org.hillview.table.api.*;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;

public class StringColumnsFilterDescription implements ITableFilterDescription {
    static final long serialVersionUID = 1;

    /**
     * The name of the columns on which the filtering operation is performed.
     */
    private final String[] colNames;
    /**
     * The description of the filter that will be used.
     */
    private final StringFilterDescription stringFilterDescription;
    /*
     * Map string->string described by a string array.
     */
    @Nullable
    private final String[] renameMap;

    public StringColumnsFilterDescription(String[] colNames, StringFilterDescription
            stringFilterDescription, @Nullable String[] renameMap) {
        this.colNames = colNames;
        this.stringFilterDescription = stringFilterDescription;
        this.renameMap = renameMap;
    }

    @Override
    public ITableFilter getFilter(ITable table) {
        return new StringColumnsFilter(table);
    }

    /**
     * Keep only the rows that match the filter in the specified columns.
     */
    public class StringColumnsFilter implements ITableFilter {
        private final IStringFilter stringFilter;
        private final VirtualRowSnapshot vrs;
        StringColumnsFilter(ITable table) {
            this.stringFilter = StringFilterFactory.getFilter(stringFilterDescription);
            Schema schema = table.getSchema().project(
                    c -> Utilities.indexOf(StringColumnsFilterDescription.this.colNames, c) >= 0);
            this.vrs = new VirtualRowSnapshot(
                    table, schema,
                    Utilities.arrayToMap(StringColumnsFilterDescription.this.renameMap));
        }

        /**
         * @return Whether the value at the specified row index matches to the compare value.
         */
        @Override
        public boolean test(int rowIndex) {
            this.vrs.setRow(rowIndex);
            for (String col: StringColumnsFilterDescription.this.colNames) {
                String value = vrs.asString(col);
                if (this.stringFilter.test(value))
                    return true;
            }
            return false;
        }
    }
}
