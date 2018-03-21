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
import org.hillview.table.api.ColumnAndConverter;
import org.hillview.table.api.ColumnAndConverterDescription;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class HeatMap3DSketch implements ISketch<ITable, HeatMap3D> {
    private final IBucketsDescription bucketDescD1;
    private final IBucketsDescription bucketDescD2;
    private final IBucketsDescription bucketDescD3;
    private final ColumnAndConverterDescription col1;
    private final ColumnAndConverterDescription col2;
    private final ColumnAndConverterDescription col3;
    private final double rate;
    private final long seed;

    public HeatMap3DSketch(IBucketsDescription bucketDesc1,
                           IBucketsDescription bucketDesc2,
                           IBucketsDescription bucketDesc3,
                           ColumnAndConverterDescription col1,
                           ColumnAndConverterDescription col2,
                           ColumnAndConverterDescription col3,
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
    public HeatMap3D create(final ITable data) {
        List<ColumnAndConverterDescription> ccds = new ArrayList<ColumnAndConverterDescription>(3);
        ccds.add(this.col1);
        ccds.add(this.col2);
        ccds.add(this.col3);
        List<ColumnAndConverter> cols = data.getLoadedColumns(ccds);
        HeatMap3D result = this.getZero();
        result.createHeatMap(cols.get(0), cols.get(1), cols.get(2),
                data.getMembershipSet(), this.rate, this.seed, true);
        return result;
    }

    @Override
    public HeatMap3D zero() {
        return new HeatMap3D(this.bucketDescD1, this.bucketDescD2, this.bucketDescD3);
    }

    @Override
    public HeatMap3D add(@Nullable final HeatMap3D left, @Nullable final HeatMap3D right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }
}
