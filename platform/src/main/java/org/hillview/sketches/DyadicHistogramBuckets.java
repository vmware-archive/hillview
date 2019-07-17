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

import org.hillview.table.api.IColumn;

/**
 * DyadicHistogramBuckets ensures that every bucket is a multiple of the leaf granularity.
 * It is built on data that can be converted to doubles.
 * The last bucket is right-inclusive.
 */
public class DyadicHistogramBuckets implements IHistogramBuckets {
    private double minValue;
    private double maxValue;
    private int numOfBuckets;
    private final double bucketSize;
    private final double range;

    private final int granularity;
    private final int numLeaves;

    private void roundMinMax() {
        long intMin = (long)Math.floor(this.minValue);
        long intMax = (long)Math.ceil(this.maxValue);

        long minRem = intMin % this.granularity;
        long maxRem = intMax % this.granularity;

        intMin = intMin - minRem;
        if ( maxRem > 0 ) { // Don't accidentally add an extra leaf!
            intMax = intMax + (this.granularity - maxRem);
        }

        this.minValue = (double)intMin;
        this.maxValue = (double)intMax;
    }

    public DyadicHistogramBuckets(final double minValue, final double maxValue,
                                  final int numOfBuckets, final int granularity) {
        this.granularity = granularity;

        if (maxValue < minValue || numOfBuckets <= 0)
            throw new IllegalArgumentException("Negative range or number of buckets");
        this.minValue = minValue;
        this.maxValue = maxValue;

        this.range = this.maxValue - this.minValue;
        if (this.range <= 0)
            this.numOfBuckets = 1;  // ignore specified number of buckets
        else
            this.numOfBuckets = numOfBuckets;

        // Ensure that min and max are an integer multiple of granularity
        this.roundMinMax();
        this.bucketSize = (this.maxValue - this.minValue) / this.numOfBuckets;
        this.numLeaves = (int)((this.maxValue - this.minValue) / this.granularity);

        // Preserves semantics, will make noise computation easier
        if ( this.numLeaves < this.numOfBuckets ) {
            this.numOfBuckets = this.numLeaves;
        }
    }

    public int indexOf(double value) {
        if ((value < this.minValue) || (value > this.maxValue))
            return -1;

        // First compute which leaf interval this value participates in
        // and then which bucket the interval start falls in.
        int leafIdx = (int)(this.numLeaves * (value - this.minValue) / this.range);
        int intervalStart = leafIdx * this.granularity;

        // As overflow can occur when 'item' is very close to 'this.maxValue', clamp the resulting index.
        return Math.min((int) ((this.numOfBuckets * intervalStart) / this.range), this.numOfBuckets - 1);
    }

    // Get the number of leaves that would correspond to this bucket.
    // If bucket boundaries and leaf boundaries don't align,
    // we assign leaves to the bucket that their left boundary falls in.
    public int numLeavesInBucket(int bucketIdx) {
        double bucketLeft = this.bucketSize * bucketIdx;
        double bucketRight = bucketLeft + this.bucketSize;

        double prevRange = Math.max(0, bucketLeft - this.minValue);
        double prevLeaves = Math.ceil(prevRange / this.granularity); // Number of leaves before this bucket
        double bucketRangeStart = prevLeaves * this.granularity; // Effective start range for this bucket

        int numLeavesInBucket = (int)Math.ceil((bucketRight - bucketRangeStart) / granularity); // what to do about end?
        return numLeavesInBucket;
    }

    @Override
    public int indexOf(IColumn column, int rowIndex) {
        double item = column.asDouble(rowIndex);
        return this.indexOf(item);
    }

    @Override
    public int getNumOfBuckets() { return this.numOfBuckets; }

    public double getMin() { return this.minValue; }
    public double getMax() { return this.maxValue; }
}
