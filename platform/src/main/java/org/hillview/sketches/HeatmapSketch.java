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
import org.hillview.dataset.TableSketch;
import org.hillview.sketches.results.Heatmap;
import org.hillview.sketches.results.IHistogramBuckets;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.columns.QuantizedColumn;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.List;

public class HeatmapSketch implements TableSketch<Heatmap> {
    static final long serialVersionUID = 1;
    private final IHistogramBuckets bucketsD0;
    private final IHistogramBuckets bucketsD1;
    private final double samplingRate;
    private final long seed;
    @Nullable
    private final ColumnQuantization q0;
    @Nullable
    private final ColumnQuantization q1;

    public HeatmapSketch(IHistogramBuckets buckets0, IHistogramBuckets buckets1,
                         double samplingRate, long seed,
                         @Nullable ColumnQuantization q0, @Nullable ColumnQuantization q1) {
        this.bucketsD0 = buckets0;
        this.bucketsD1 = buckets1;
        this.samplingRate = samplingRate;
        this.seed = seed;
        this.q0 = q0;
        this.q1 = q1;
    }

    public HeatmapSketch(IHistogramBuckets bucketDesc1, IHistogramBuckets bucketDesc2,
                         double samplingRate, long seed) {
        this(bucketDesc1, bucketDesc2, samplingRate, seed, null, null);
    }

    @Override
    public Heatmap create(@Nullable final ITable data) {
        Heatmap result = this.getZero();
        Converters.checkNull(data);
        Converters.checkNull(result);
        List<IColumn> cols  = data.getLoadedColumns(this.bucketsD0.getColumn(), this.bucketsD1.getColumn());
        IColumn c0 = cols.get(0);
        IColumn c1  = cols.get(1);
        if (this.q0 != null)
            c0 = new QuantizedColumn(c0, this.q0);
        if (this.q1 != null)
            c1 = new QuantizedColumn(c1, this.q1);
        result.createHeatmap(c0, c1, this.bucketsD0, this.bucketsD1,
                data.getMembershipSet(), this.samplingRate, this.seed, false);
        return result;
    }

    @Override
    public Heatmap zero() {
        return new Heatmap(this.bucketsD0.getBucketCount(), this.bucketsD1.getBucketCount());
    }

    @Override
    public Heatmap add(@Nullable final Heatmap left, @Nullable final Heatmap right) {
        assert left != null;
        assert right != null;
        return left.union(right);
    }
}
