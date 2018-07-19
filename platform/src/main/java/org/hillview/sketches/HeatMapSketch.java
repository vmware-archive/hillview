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
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;

public class HeatMapSketch implements ISketch<ITable, HeatMap> {
    private final IHistogramBuckets bucketDescD1;
    private final IHistogramBuckets bucketDescD2;
    private final String col0;
    private final String col1;
    private final double rate;
    private final long seed;

    public HeatMapSketch(IHistogramBuckets bucketDesc1, IHistogramBuckets bucketDesc2,
                         String col0, String col1,
                         double rate, long seed) {
        this.bucketDescD1 = bucketDesc1;
        this.bucketDescD2 = bucketDesc2;
        this.col0 = col0;
        this.col1 = col1;
        this.rate = rate;
        this.seed = seed;
    }

    @Override
    public HeatMap create(final ITable data) {
        HeatMap result = this.getZero();
        IColumn c0  = data.getLoadedColumn(this.col0);
        IColumn c1  = data.getLoadedColumn(this.col1);
        result.createHeatMap(c0, c1, data.getMembershipSet(), this.rate, this.seed, false);
        return result;
    }

    @Override
    public HeatMap zero() {
        return new HeatMap(this.bucketDescD1, this.bucketDescD2);
    }

    @Override
    public HeatMap add(@Nullable final HeatMap left, @Nullable final HeatMap right) {
        assert left != null;
        assert right != null;
        return left.union(right);
    }
}
