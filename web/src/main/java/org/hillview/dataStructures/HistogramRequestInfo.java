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

package org.hillview.dataStructures;

import org.hillview.sketches.results.DoubleHistogramBuckets;
import org.hillview.sketches.HistogramSketch;
import org.hillview.sketches.results.IHistogramBuckets;
import org.hillview.sketches.results.StringHistogramBuckets;
import org.hillview.table.ColumnDescription;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.table.columns.DoubleColumnQuantization;
import org.hillview.table.columns.StringColumnQuantization;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;

/**
 * Class used to deserialize JSON request from UI for a histogram.
 */
public class HistogramRequestInfo {
    public ColumnDescription cd = new ColumnDescription();
    public double samplingRate;
    public long seed;
    // Only used when doing string histograms
    @Nullable
    private String[] leftBoundaries;
    // Only used when doing double histograms
    private double min;
    private double max;
    private int bucketCount;

    /**
     * Explicit constructors for headless testing.
     */
    public HistogramRequestInfo(ColumnDescription cd, long seed, double min, double max, int bucketCount) {
        this.cd = cd;
        this.seed = seed;
        this.min = min;
        this.max = max;
        this.bucketCount = bucketCount;
        this.samplingRate = 1.0;
    }

    public IHistogramBuckets getBuckets(@Nullable ColumnQuantization quantization) {
        if (cd.kind.isString()) {
            Converters.checkNull(this.leftBoundaries);
            if (quantization != null)
                return new StringHistogramBuckets(this.leftBoundaries,
                        ((StringColumnQuantization)quantization).globalMax);
            else
                return new StringHistogramBuckets(this.leftBoundaries);
        } else {
            return new DoubleHistogramBuckets(this.min, this.max, this.bucketCount);
        }
    }

    public IHistogramBuckets getBuckets() {
        return this.getBuckets(null);
    }

    public HistogramSketch getSketch(@Nullable ColumnQuantization quantization) {
        IHistogramBuckets buckets = this.getBuckets(quantization);
        return new HistogramSketch(buckets, this.cd.name, this.samplingRate, this.seed, quantization);
    }

    public IntervalDecomposition getDecomposition(ColumnQuantization quantization) {
        IHistogramBuckets buckets = this.getBuckets(quantization);
        if (cd.kind.isString()) {
            return new StringIntervalDecomposition(
                    (StringColumnQuantization)quantization,
                    (StringHistogramBuckets)buckets);
        } else {
            return new NumericIntervalDecomposition(
                    (DoubleColumnQuantization)quantization,
                    (DoubleHistogramBuckets)buckets);
        }
    }
}
