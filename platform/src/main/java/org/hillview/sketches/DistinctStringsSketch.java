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

import org.hillview.dataset.api.TableSketch;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

@Deprecated
// This class is used just for benckmarking
public class DistinctStringsSketch implements TableSketch<DistinctStrings> {
    static final long serialVersionUID = 1;
    private final String column;

    public DistinctStringsSketch(String column) {
        this.column = column;
    }

    @Override
    public DistinctStrings zero() {
        return new DistinctStrings();
    }

    @Override
    public DistinctStrings add(
            @Nullable DistinctStrings left,
            @Nullable DistinctStrings right) {
            return Converters.checkNull(left).union(Converters.checkNull(right));
    }

    @Override
    public DistinctStrings create(@Nullable final ITable data) {
        IColumn col = Converters.checkNull(data).getLoadedColumn(this.column);
        DistinctStrings result = this.getZero();
        Converters.checkNull(result);
        IRowIterator it = data.getMembershipSet().getIterator();
        int row = it.getNextRow();
        while (row >= 0) {
            String s = col.getString(row);
            if (s != null)
                result.add(s);
            row = it.getNextRow();
        }
        return result;
    }
}
