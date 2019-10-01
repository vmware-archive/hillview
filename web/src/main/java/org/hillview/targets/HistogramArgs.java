/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

package org.hillview.targets;

import org.hillview.sketches.DoubleHistogramBuckets;
import org.hillview.sketches.HistogramSketch;
import org.hillview.sketches.IHistogramBuckets;
import org.hillview.sketches.StringHistogramBuckets;
import org.hillview.table.ColumnDescription;

import javax.annotation.Nullable;

/**
 * Class used to deserialize JSON request from UI for a histogram.
 */
class HistogramArgs {
    ColumnDescription cd = new ColumnDescription();
    public double samplingRate;
    public long seed;
    // Only used when doing string histograms
    @Nullable
    private String[] leftBoundaries;
    // Only used when doing double histograms
    private double min;
    private double max;
    private int bucketCount;

    IHistogramBuckets getBuckets() {
        if (cd.kind.isString()) {
            assert this.leftBoundaries != null;
            return new StringHistogramBuckets(this.leftBoundaries);
        } else {
            return new DoubleHistogramBuckets(this.min, this.max, this.bucketCount);
        }
    }

    HistogramSketch getSketch() {
        IHistogramBuckets buckets = this.getBuckets();
        return new HistogramSketch(buckets, this.cd.name, this.samplingRate, this.seed);
    }
}
