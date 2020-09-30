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
import org.hillview.utils.Converters;
import org.hillview.utils.Linq;

import javax.annotation.Nullable;
import java.util.List;

/**
 * This map receives a column name of the input table, and returns a table with a new column,
 * containing the specified data converted to a new kind.
 */
public class RenameColumnMap implements IMap<ITable, ITable> {
    static final long serialVersionUID = 1;
    private final String from;
    private final String to;

    public RenameColumnMap(String from, String to) {
        this.from = from;
        this.to = to;
    }

    @Override
    public ITable apply(@Nullable ITable table) {
        List<IColumn> cols = Linq.map(Converters.checkNull(table).getColumns(table.getSchema()),
                c -> c.getName().equals(this.from) ? c.rename(this.to) : c);
        return table.replace(cols);
    }
}
