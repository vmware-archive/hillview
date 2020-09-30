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
import java.io.Serializable;

/**
 * This map creates a new column by applying the function to each element in an input column.
 */
public abstract class CreateColumnMap extends AppendOrReplaceColumnMap {
    static final long serialVersionUID = 1;

    static class Info implements Serializable {
        ColumnDescription inputColumn;
        String outputColumn;
        int    outputIndex;

        public Info(ColumnDescription desc, String outCol, int outputIndex) {
            this.inputColumn = desc;
            this.outputColumn = outCol;
            this.outputIndex = outputIndex;
        }
    }

    private final Info info;

    CreateColumnMap(Info info) {
        super(info.outputIndex);
        this.info = info;
    }

    @Nullable
    public abstract String extract(@Nullable String s);

    @Override
    public IColumn createColumn(ITable table) {
        IColumn col = table.getLoadedColumn(this.info.inputColumn.name);
        ColumnDescription outputColumn = new ColumnDescription(this.info.outputColumn, ContentsKind.String);
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
