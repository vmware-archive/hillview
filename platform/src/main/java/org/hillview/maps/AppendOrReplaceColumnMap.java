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
import org.hillview.utils.Linq;

import javax.annotation.Nullable;
import java.util.List;

/**
 * This is a base class for map computations that need to add or replace
 * a column to a table.  If the newly created column has a name that already exists
 * then the old column is replaced and the index is ignored.
 */
public abstract class AppendOrReplaceColumnMap implements IMap<ITable, ITable> {
    static final long serialVersionUID = 1;
    /**
     * Where to insert the new column; if -1 the column is inserted at the end.
     */
    private final int    insertionIndex;

    AppendOrReplaceColumnMap(int insertionIndex) {
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
        IColumn column = this.createColumn(table);
        String name = column.getName();
        if (table.getSchema().containsColumnName(name)) {
            List<IColumn> cols = Linq.map(table.getColumns(table.getSchema()),
                    c -> c.getName().equals(name) ? column : c);
            return table.replace(cols);
        } else {
            return table.insertColumn(column, this.insertionIndex);
        }
    }
}
