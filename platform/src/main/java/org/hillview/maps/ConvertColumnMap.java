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

package org.hillview.maps;

import org.hillview.dataset.api.IMap;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;

import java.util.ArrayList;
import java.util.List;

/**
 * This map receives a column name of the input table, and returns a table with the same column, with additionally
 * that specified column converted to different kind.
 */
public class ConvertColumnMap implements IMap<ITable, ITable> {
    private final String inputColName;
    private final String newColName;
    private final ContentsKind newKind;

    /**
     * @param inputColName The name of the column that has to be converted to a categorical column.
     * @param newColName Name of the new column. The table cannot have a column with this name already.
     * @param newKind Kind of the column.
     */
    public ConvertColumnMap(String inputColName, String newColName, ContentsKind newKind) {
        this.inputColName = inputColName;
        this.newColName = newColName;
        this.newKind = newKind;
    }

    @Override
    public ITable apply(ITable table) {
        if (table.getSchema().getColumnNames().contains(this.newColName))
            throw new IllegalArgumentException("Column " + this.newColName + " already exists in table.");
        // Make new list of columns.
        List<IColumn> columns = new ArrayList<IColumn>();
        table.getColumns().forEach(columns::add);

        IColumn newColumn =  table.getColumn(this.inputColName)
                .convertKind(this.newKind, this.newColName, table.getMembershipSet());

        // Insert the new column next to the input column.
        int inputColIndex = columns.indexOf(table.getColumn(this.inputColName));
        columns.add(inputColIndex + 1, newColumn);

        return new Table(columns, table.getMembershipSet());
    }
}
