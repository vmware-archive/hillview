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
import org.hillview.sketches.results.Histogram;
import org.hillview.sketches.results.IHistogramBuckets;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.columns.QuantizedColumn;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * One-dimensional histogram
 */
public class HistogramSketch implements ISketch<ITable, Histogram> {
    private final IHistogramBuckets bucketDesc;
    protected final String columnName;
    protected final double rate;
    protected final long seed;
    @Nullable
    protected ColumnQuantization cpm;

    public HistogramSketch(IHistogramBuckets bucketDesc, String columnName,
                           double rate, long seed, @Nullable ColumnQuantization cpm) {
        this.bucketDesc = bucketDesc;
        this.columnName = columnName;
        this.rate = rate;
        this.seed = seed;
        this.cpm = cpm;
    }

    @Override
    public Histogram create(@Nullable final ITable data) {
        Converters.checkNull(data);
        Histogram result = this.getZero();
        Converters.checkNull(result);
        IColumn column = data.getLoadedColumn(this.columnName);
        if (this.cpm != null)
            column = new QuantizedColumn(column, this.cpm);
        result.create(column, data.getMembershipSet(), this.bucketDesc, this.rate, this.seed, false);
        return result;
    }

    @Override
    public Histogram add(@Nullable final Histogram left,
                         @Nullable final Histogram right) {
        assert left != null;
        assert right != null;
        return left.union(right);
    }

    @Override
    public Histogram zero() {
        return new Histogram(this.bucketDesc.getBucketCount());
    }
}
