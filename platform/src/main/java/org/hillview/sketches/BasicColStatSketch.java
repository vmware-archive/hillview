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
import org.hillview.sketches.results.BasicColStats;
import org.hillview.sketches.results.HLogLog;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.JsonList;
import org.hillview.utils.Pair;

import javax.annotation.Nullable;

/**
 * A Sketch that computes basic column statistics for a set of columns.
 */
public class BasicColStatSketch implements TableSketch<JsonList<Pair<BasicColStats, HLogLog>>> {
    static final long serialVersionUID = 1;
    private final String[] cols;
    private final int momentNum;
    private final long[] seeds;

    public BasicColStatSketch(String[] cols, int momentNum, long[] seeds) {
        this.cols = cols;
        this.momentNum = momentNum;
        this.seeds = seeds;
    }

    public BasicColStatSketch(String col, int momentNum, long seed) {
        this.cols = new String[] { col };
        this.momentNum = momentNum;
        this.seeds = new long[] { seed };
    }

    @Override
    public JsonList<Pair<BasicColStats, HLogLog>> create(@Nullable final ITable data) {
        Converters.checkNull(data);
        JsonList<Pair<BasicColStats, HLogLog>> result = this.getZero();
        Converters.checkNull(result);
        for (int i = 0; i < this.cols.length; i++) {
            IColumn col = data.getLoadedColumn(this.cols[i]);
            result.get(i).first.scan(col, data.getMembershipSet());
            result.get(i).second.createHLL(col, data.getMembershipSet());
        }
        return result;
    }

    @Override
    public JsonList<Pair<BasicColStats, HLogLog>> zero() {
        JsonList<Pair<BasicColStats, HLogLog>> result = new JsonList<>(this.cols.length);
        for (int i = 0; i < this.cols.length; i++)
            result.add(new Pair<>(
                    new BasicColStats(this.momentNum, true),
                    new HLogLog(HLogLogSketch.DEFAULT_LOG_SPACE_SIZE, seeds[i])
            ));

        return result;
    }

    @Override
    public JsonList<Pair<BasicColStats, HLogLog>> add(@Nullable final JsonList<Pair<BasicColStats, HLogLog>> left,
                                                      @Nullable final JsonList<Pair<BasicColStats, HLogLog>> right) {
        assert left != null;
        assert right != null;
        return left.zip(right, (p1, p2) -> new Pair<>(p1.first.union(p2.first), p1.second.union(p2.second)));
    }
}
