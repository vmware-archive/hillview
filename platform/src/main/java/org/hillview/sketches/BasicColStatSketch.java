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
import org.hillview.sketches.results.BasicColStats;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.JsonList;

import javax.annotation.Nullable;

/**
 * A Sketch that computes basic column statistics for a set of columns.
 */
public class BasicColStatSketch implements ISketch<ITable, JsonList<BasicColStats>> {
    static final long serialVersionUID = 1;
    private final String[] cols;
    private final int momentNum;

    public BasicColStatSketch(String[] cols, int momentNum) {
        this.cols = cols;
        this.momentNum = momentNum;
    }

    public BasicColStatSketch(String col, int momentNum) {
        this.cols = new String[] { col };
        this.momentNum = momentNum;
    }

    @Override
    public JsonList<BasicColStats> create(@Nullable final ITable data) {
        Converters.checkNull(data);
        JsonList<BasicColStats> result = this.getZero();
        Converters.checkNull(result);
        for (int i = 0; i < this.cols.length; i++)
            result.get(i).scan(data.getLoadedColumn(this.cols[i]), data.getMembershipSet());
        return result;
    }

    @Override
    public JsonList<BasicColStats> zero() {
        JsonList<BasicColStats> result = new JsonList<BasicColStats>(this.cols.length);
        for (int i=0; i < this.cols.length; i++)
            result.add(new BasicColStats(this.momentNum, true));
        return result;
    }

    @Override
    public JsonList<BasicColStats> add(@Nullable final JsonList<BasicColStats> left,
                                       @Nullable final JsonList<BasicColStats> right) {
        assert left != null;
        assert right != null;
        return left.zip(right, BasicColStats::union);
    }
}
