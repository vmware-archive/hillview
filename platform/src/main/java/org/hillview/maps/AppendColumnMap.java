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
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;

/**
 * This is a base class for map computations that need to add
 * a column to a table.
 */
public abstract class AppendColumnMap implements IMap<ITable, ITable> {
    static final long serialVersionUID = 1;
    /**
     * Name for the new column.
     */
    final String newColName;
    /**
     * Where to insert the new column; if -1 the column is inserted at the end.
     */
    private final int    insertionIndex;

    AppendColumnMap(String newColName, int insertionIndex) {
        this.newColName = newColName;
        this.insertionIndex = insertionIndex;
    }

    /**
     * Method which creates the new column.
     * @param table  Table with data.
     * @return       The new column created.
     */
    abstract IColumn createColumn(ITable table);

    @Override
    public ITable apply(@Nullable ITable table) {
        assert table != null;
        if (table.getSchema().containsColumnName(this.newColName))
            throw new IllegalArgumentException("Column " + this.newColName + " already exists in table.");
        IColumn column = this.createColumn(table);
        return table.insertColumn(column, this.insertionIndex);
    }
}
