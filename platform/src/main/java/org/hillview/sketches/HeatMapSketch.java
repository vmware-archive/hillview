/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.hillview.table.api.ColumnNameAndConverter;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

public class HeatMapSketch implements ISketch<ITable, HeatMap> {
    private final IBucketsDescription bucketDescD1;
    private final IBucketsDescription bucketDescD2;
    private final ColumnNameAndConverter col1;
    private final ColumnNameAndConverter col2;
    private final double rate;

    public HeatMapSketch(IBucketsDescription bucketDesc1, IBucketsDescription bucketDesc2,
                         ColumnNameAndConverter col1, ColumnNameAndConverter col2) {
        this.bucketDescD1 = bucketDesc1;
        this.bucketDescD2 = bucketDesc2;
        this.col1 = col1;
        this.col2 = col2;
        this.rate = 1;
    }

    public HeatMapSketch(IBucketsDescription bucketDesc1, IBucketsDescription bucketDesc2,
                         ColumnNameAndConverter col1, ColumnNameAndConverter col2, double rate) {
        this.bucketDescD1 = bucketDesc1;
        this.bucketDescD2 = bucketDesc2;
        this.col1 = col1;
        this.col2 = col2;
        this.rate = rate;
    }

    @Override
    public HeatMap create(final ITable data) {
        HeatMap result = this.getZero();
        result.createHeatMap(data.getColumn(this.col1), data.getColumn(this.col2),
                             data.getMembershipSet().sample(this.rate));
        return result;
    }

    @Override
    public HeatMap zero() {
        return new HeatMap(this.bucketDescD1, this.bucketDescD2);
    }

    @Override
    public HeatMap add(@Nullable final HeatMap left, @Nullable final HeatMap right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }
}
