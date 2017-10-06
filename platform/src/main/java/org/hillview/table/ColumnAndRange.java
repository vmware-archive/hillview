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

package org.hillview.table;

import org.hillview.sketches.BucketsDescriptionEqSize;
import org.hillview.sketches.HistogramSketch;
import org.hillview.table.api.ColumnAndConverterDescription;
import org.hillview.table.api.IStringConverterDescription;

import javax.annotation.Nullable;
import java.io.Serializable;

@SuppressWarnings("CanBeFinal")
public class ColumnAndRange implements Serializable {
    public String columnName = "";
    public double min;
    public double max;
    public int cdfBucketCount;
    public int bucketCount;
    public double samplingRate;
    @Nullable
    public String[] bucketBoundaries;  // only used for Categorical columns

    public HistogramParts prepare() {
        IStringConverterDescription converter = null;
        if (this.bucketBoundaries != null) {
            converter = new SortedStringsConverterDescription(
                    this.bucketBoundaries, (int)Math.ceil(this.min), (int) Math.floor(this.max));
        }
        BucketsDescriptionEqSize buckets = new BucketsDescriptionEqSize(this.min, this.max, this.bucketCount);
        ColumnAndConverterDescription column = new ColumnAndConverterDescription(this.columnName, converter);
        HistogramSketch sketch = new HistogramSketch(buckets, column, this.samplingRate);
        return new HistogramParts(buckets, column, sketch);
    }

    public static class HistogramParts {
        public final BucketsDescriptionEqSize buckets;
        public final ColumnAndConverterDescription column;
        public final HistogramSketch sketch;

        public HistogramParts(BucketsDescriptionEqSize buckets,
                              ColumnAndConverterDescription column,
                              HistogramSketch sketch) {
            this.buckets = buckets;
            this.column = column;
            this.sketch = sketch;
        }
    }
}