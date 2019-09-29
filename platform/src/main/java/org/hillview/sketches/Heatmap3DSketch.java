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
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class Heatmap3DSketch implements ISketch<ITable, Heatmap3D> {
    private final IHistogramBuckets bucketDescD1;
    private final IHistogramBuckets bucketDescD2;
    private final IHistogramBuckets bucketDescD3;
    private final String col1;
    private final String col2;
    private final String col3;
    private final double rate;
    private final long seed;

    public Heatmap3DSketch(IHistogramBuckets bucketDesc1,
                           IHistogramBuckets bucketDesc2,
                           IHistogramBuckets bucketDesc3,
                           String col1,
                           String col2,
                           String col3,
                           double rate, long seed) {
        this.bucketDescD1 = bucketDesc1;
        this.bucketDescD2 = bucketDesc2;
        this.bucketDescD3 = bucketDesc3;
        this.col1 = col1;
        this.col2 = col2;
        this.col3 = col3;
        this.rate = rate;
        this.seed = seed;
    }

    @Override
    public Heatmap3D create(@Nullable final ITable data) {
        List<String> colNames = new ArrayList<String>(3);
        colNames.add(this.col1);
        colNames.add(this.col2);
        colNames.add(this.col3);
        List<IColumn> cols = Converters.checkNull(data).getLoadedColumns(colNames);
        Heatmap3D result = this.getZero();
        Converters.checkNull(result).createHeatmap(cols.get(0), cols.get(1), cols.get(2),
                data.getMembershipSet(), this.rate, this.seed, true);
        return result;
    }

    @Override
    public Heatmap3D zero() {
        return new Heatmap3D(this.bucketDescD1, this.bucketDescD2, this.bucketDescD3);
    }

    @Override
    public Heatmap3D add(@Nullable final Heatmap3D left, @Nullable final Heatmap3D right) {
        assert left != null;
        assert right != null;
        return left.union(right);
    }
}
