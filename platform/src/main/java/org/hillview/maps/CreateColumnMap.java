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

package org.hillview.maps;

import org.hillview.table.ColumnDescription;
import org.hillview.table.api.*;
import org.hillview.table.columns.BaseColumn;

import javax.annotation.Nullable;

/**
 * This map creates a new column by applying the function to each element in an input column.
 */
public abstract class CreateColumnMap extends AppendColumnMap {
    private final String inputColName;

    /**
     * @param inputColName The name of the column where data is extracted from.
     * @param newColName Name of the new column.
     * @param insertionIndex  Index where column will be inserted.
     */
    CreateColumnMap(String inputColName, String newColName,
                              int insertionIndex) {
        super(newColName, insertionIndex);
        this.inputColName = inputColName;
    }

    @Nullable
    public abstract String extract(@Nullable String s);

    @Override
    public IColumn createColumn(ITable table) {
        IColumn col = table.getLoadedColumn(this.inputColName);
        ColumnDescription outputColumn = new ColumnDescription(this.newColName, ContentsKind.String);
        IMutableColumn outCol = BaseColumn.create(outputColumn,
                table.getMembershipSet().getMax(),
                table.getMembershipSet().getSize());
        IRowIterator it = table.getMembershipSet().getIterator();
        int r = it.getNextRow();
        while (r >= 0) {
            String source = col.asString(r);
            String value = this.extract(source);
            outCol.set(r, value);
            r = it.getNextRow();
        }
        return outCol;
    }
}
