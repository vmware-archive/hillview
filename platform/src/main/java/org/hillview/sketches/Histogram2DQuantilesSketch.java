/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

import org.hillview.sketches.highorder.GroupBySketch;
import org.hillview.sketches.highorder.GroupByWorkspace;
import org.hillview.sketches.results.Groups;
import org.hillview.sketches.results.IHistogramBuckets;
import org.hillview.sketches.results.SampleSet;
import org.hillview.table.api.ITable;

/**
 * This computes a 2D array (defined by 2 histogram buckets) of SampleSets for a third column.
 */
public class Histogram2DQuantilesSketch
        extends GroupBySketch<Groups<SampleSet>,
        GroupByWorkspace<ColumnWorkspace<ReservoirSampleWorkspace>>,
                                      HistogramQuantilesSketch> {
    private final String qCol;

    public Histogram2DQuantilesSketch(
            String column,
            int quantileCount,
            long seed,
            IHistogramBuckets buckets0,
            IHistogramBuckets buckets1) {
        super(buckets1, new HistogramQuantilesSketch(column, quantileCount, seed, buckets0));
        this.qCol = column;
    }

    @Override
    public GroupByWorkspace<GroupByWorkspace<ColumnWorkspace<ReservoirSampleWorkspace>>>
    initialize(ITable data) {
        data.getLoadedColumns(this.buckets.getColumn(), super.buckets.getColumn(), this.qCol);
        return super.initialize(data);
    }
}
