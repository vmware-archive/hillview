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

public class HistogramSketch implements ISketch<ITable, Histogram> {
    private final IBucketsDescription bucketDesc;
    private final ColumnNameAndConverter col;
    private final double rate;

    public HistogramSketch(IBucketsDescription bucketDesc, ColumnNameAndConverter col) {
        this(bucketDesc, col, 1.0);
    }

    public HistogramSketch(IBucketsDescription bucketDesc, ColumnNameAndConverter col,
                           double rate) {
        this.bucketDesc = bucketDesc;
        this.col = col;
        this.rate = rate;
    }

    @Override
    public Histogram create(final ITable data) {
        Histogram result = this.getZero();
        result.create(data.getColumn(this.col), data.getMembershipSet(), this.rate);
        return result;
    }

    @Override
    public Histogram add(@Nullable final Histogram left,
                         @Nullable final Histogram right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }

    @Override
    public Histogram zero() {
        return new Histogram(this.bucketDesc);
    }
}
