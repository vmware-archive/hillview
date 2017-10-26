/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.jsonObjects;

import org.hillview.sketches.HistogramSketch;
import org.hillview.sketches.IBucketsDescription;
import org.hillview.table.api.ColumnAndConverterDescription;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

@SuppressWarnings("WeakerAccess")
public class HistogramArgs implements RpcArguments {
    @Nullable
    public ColumnAndRange column;
    public int cdfBucketCount;
    public int bucketCount;
    public double samplingRate;
    public double cdfSamplingRate;
    public long seed;

    public ColumnAndConverterDescription getDescription() {
        return Converters.checkNull(this.column).getDescription();
    }

    public HistogramSketch getSketch(boolean cdf) {
        IBucketsDescription buckets = this.getBuckets(cdf);
        double rate = cdf ? this.cdfSamplingRate : this.samplingRate;
        return new HistogramSketch(buckets, this.getDescription(), rate, this.seed);
    }

    public IBucketsDescription getBuckets(boolean cdf) {
        int count = cdf ? this.cdfBucketCount : this.bucketCount;
        return Converters.checkNull(this.column).getBuckets(count);
    }
}
