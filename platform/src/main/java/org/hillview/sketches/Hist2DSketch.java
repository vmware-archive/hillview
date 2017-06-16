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

public class Hist2DSketch implements ISketch<ITable, Histogram2DHeavy> {
    private final IBucketsDescription1D bucketDescD1;
    private final IBucketsDescription1D bucketDescD2;
    private final String colNameD1;
    private final String colNameD2;
    @Nullable
    private final IStringConverter converterD1;
    @Nullable
    private final IStringConverter converterD2;
    private final double rate;

    public Hist2DSketch(IBucketsDescription1D bucketDesc1, IBucketsDescription1D bucketDesc2,
                         @Nullable IStringConverter converter1, @Nullable IStringConverter converter2,
                         String colName1, String colName2) {
        this.bucketDescD1 = bucketDesc1;
        this.bucketDescD2 = bucketDesc2;
        this.colNameD1 = colName1;
        this.colNameD2 = colName2;
        this.converterD1 = converter1;
        this.converterD2 = converter2;
        this.rate = 1;
    }

    public Hist2DSketch(IBucketsDescription1D bucketDesc1, IBucketsDescription1D bucketDesc2,
                         @Nullable IStringConverter converter1, @Nullable IStringConverter converter2,
                         String colName1, String colName2, double rate) {
        this.bucketDescD1 = bucketDesc1;
        this.bucketDescD2 = bucketDesc2;
        this.colNameD1 = colName1;
        this.colNameD2 = colName2;
        this.converterD1 = converter1;
        this.converterD2 = converter2;
        this.rate = rate;
    }

    @Override
    public Histogram2DHeavy create(final ITable data) {
        Histogram2DHeavy result = this.getZero();
        result.createHistogram(data.getColumn(this.colNameD1), data.getColumn(this.colNameD2),
                this.converterD1, this.converterD2, data.getMembershipSet().sample(this.rate));
        return result;
    }

    @Override
    public Histogram2DHeavy zero() {
        return new Histogram2DHeavy(this.bucketDescD1, this.bucketDescD2);
    }

    @Override
    public Histogram2DHeavy add(@Nullable final Histogram2DHeavy left,
                                @Nullable final Histogram2DHeavy right) {
        return Converters.checkNull(left).union(Converters.checkNull(right));
    }
}
