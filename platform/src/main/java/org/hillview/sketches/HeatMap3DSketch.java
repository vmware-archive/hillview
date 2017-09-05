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
 *
 */

package org.hillview.sketches;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.IStringConverter;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

public class HeatMap3DSketch implements ISketch<ITable, HeatMap3D> {
    private final IBucketsDescription bucketDescD1;
    private final IBucketsDescription bucketDescD2;
    private final IBucketsDescription bucketDescD3;
    private final String colNameD1;
    private final String colNameD2;
    private final String colNameD3;
    @Nullable
    private final IStringConverter converterD1;
    @Nullable
    private final IStringConverter converterD2;
    @Nullable
    private final IStringConverter converterD3;
    private final double rate;

    public HeatMap3DSketch(IBucketsDescription bucketDesc1, IBucketsDescription bucketDesc2, IBucketsDescription
            bucketDesc3, @Nullable IStringConverter converter1, @Nullable IStringConverter converter2, @Nullable
            IStringConverter converter3, String colName1, String colName2, String colName3) {
        this.bucketDescD1 = bucketDesc1;
        this.bucketDescD2 = bucketDesc2;
        this.bucketDescD3 = bucketDesc3;
        this.colNameD1 = colName1;
        this.colNameD2 = colName2;
        this.colNameD3 = colName3;
        this.converterD1 = converter1;
        this.converterD2 = converter2;
        this.converterD3 = converter3;
        this.rate = 1;
    }

    public HeatMap3DSketch(IBucketsDescription bucketDesc1, IBucketsDescription bucketDesc2, IBucketsDescription
            bucketDesc3, @Nullable IStringConverter converter1, @Nullable IStringConverter converter2, @Nullable
            IStringConverter converter3, String colName1, String colName2, String colName3, double rate) {
        this.bucketDescD1 = bucketDesc1;
        this.bucketDescD2 = bucketDesc2;
        this.bucketDescD3 = bucketDesc3;
        this.colNameD1 = colName1;
        this.colNameD2 = colName2;
        this.colNameD3 = colName3;
        this.converterD1 = converter1;
        this.converterD2 = converter2;
        this.converterD3 = converter3;
        this.rate = rate;
    }

    @Override
    public HeatMap3D create(final ITable data) {
        HeatMap3D result = this.getZero();
        result.createHeatMap(data.getColumn(this.colNameD1), data.getColumn(this.colNameD2), data.getColumn(this.colNameD3),
                this.converterD1, this.converterD2, this.converterD3, data.getMembershipSet().sample(this.rate));
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
