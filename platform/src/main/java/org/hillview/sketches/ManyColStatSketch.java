/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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
import org.hillview.table.api.ColumnAndConverter;
import org.hillview.table.api.ColumnAndConverterDescription;
import org.hillview.table.api.ITable;
import org.hillview.utils.JsonList;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Compute basic statistics for multiple columns at once.
 */
public class ManyColStatSketch implements ISketch<ITable, JsonList<BasicColStats>> {
    private final List<ColumnAndConverterDescription> columns;
    private final int momentCount;

    public ManyColStatSketch(List<ColumnAndConverterDescription> desc, int momentCount) {
        this.columns = desc;
        this.momentCount = momentCount;
    }

    @Override
    public JsonList<BasicColStats> create(ITable data) {
        JsonList<BasicColStats> result = new JsonList<BasicColStats>(this.columns.size());
        List<ColumnAndConverter> cols = data.getLoadedColumns(this.columns);
        for (ColumnAndConverter cc: cols) {
            BasicColStats bcs = new BasicColStats(this.momentCount, true);
            bcs.createStats(cc, data.getMembershipSet());
            result.add(bcs);
        }
        return result;
    }

    @Nullable
    @Override
    public JsonList<BasicColStats> zero() {
        JsonList<BasicColStats> result = new JsonList<BasicColStats>();
        for (ColumnAndConverterDescription ignored : this.columns)
            result.add(new BasicColStats(this.momentCount, true));
        return result;
    }

    @Nullable
    @Override
    public JsonList<BasicColStats> add(@Nullable JsonList<BasicColStats> left, @Nullable JsonList<BasicColStats> right) {
        assert left != null;
        assert right != null;
        return left.zip(right, BasicColStats::union);
    }
}
