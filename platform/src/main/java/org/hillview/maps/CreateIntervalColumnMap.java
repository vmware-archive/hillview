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

import org.hillview.table.ColumnDescription;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IDoubleColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.IntervalColumn;
import org.hillview.utils.Converters;

import java.io.Serializable;
import java.util.List;

/**
 * This map receives a column name of the input table, and returns a table with a new column,
 * containing the specified data converted to a new kind.
 */
public class CreateIntervalColumnMap extends AppendColumnMap {
    static final long serialVersionUID = 1;

    public static class Info implements Serializable {
        String startColName = "";
        String endColName = "";
        int columnIndex;
        String newColName = "";
    }

    private final Info info;

    public CreateIntervalColumnMap(Info info) {
        super(info.newColName, info.columnIndex);
        this.info = info;
    }

    @Override
    public IColumn createColumn(ITable table) {
        List<IColumn> cols = table.getLoadedColumns(info.startColName, info.endColName);
        ColumnDescription desc = new ColumnDescription(info.newColName, ContentsKind.Interval);
        IColumn col0 = cols.get(0);
        IColumn col1 = cols.get(1);
        return new IntervalColumn(desc, Converters.checkNull(col0), Converters.checkNull(col1));
    }
}
