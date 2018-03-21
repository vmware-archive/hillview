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

package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.*;
import org.hillview.utils.Converters;
import org.hillview.utils.JsonList;

import javax.annotation.Nullable;
import java.util.List;

/**
 * For each of the specified column names it computes the set of
 * all unique strings in the column.  It assumes that there are few
 * distinct strings in each such column; this makes sense when columns
 * are categorical.
 */
public class DistinctStringsSketch implements ISketch<ITable, JsonList<DistinctStrings>> {
    private final int maxSize;
    private final String[] colNames;

    public DistinctStringsSketch(int maxSize, String[] colNames) {
        this.maxSize = maxSize;
        this.colNames = colNames;
    }

    @Override
    public JsonList<DistinctStrings> zero() {
        JsonList<DistinctStrings> result = new JsonList<DistinctStrings>(this.colNames.length);
        for (String colName : this.colNames)
            result.add(new DistinctStrings(this.maxSize));
        return result;
    }

    @Override
    public JsonList<DistinctStrings> add(
            @Nullable JsonList<DistinctStrings> left,
            @Nullable JsonList<DistinctStrings> right) {
        JsonList<DistinctStrings> result = new JsonList<DistinctStrings>(this.colNames.length);
        for (int i = 0; i < this.colNames.length; i++)
            result.add(Converters.checkNull(left).get(i).union(Converters.checkNull(right).get(i)));
        return result;
    }

    @Override
    public JsonList<DistinctStrings> create(final ITable data) {
        List<ColumnAndConverterDescription> ccd =
                ColumnAndConverterDescription.create(this.colNames);
        List<ColumnAndConverter> cols = data.getLoadedColumns(ccd);
        JsonList<DistinctStrings> result = this.getZero();
        for (int i = 0; i < this.colNames.length; i++) {
            final DistinctStrings ri = result.get(i);
            IColumn col = cols.get(i).column;
            ri.setColumnSize(col.sizeInRows());
            IRowIterator it = data.getMembershipSet().getIterator();
            int row = it.getNextRow();
            while (row >= 0) {
                String s = col.getString(row);
                if (s != null)
                    ri.add(s);
                row = it.getNextRow();
            }
        }
        return result;
    }
}
