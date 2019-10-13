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

import org.hillview.utils.Converters;
import javax.annotation.Nullable;

/**
 * This class is intended for use with the binary mechanism for differential privacy
 * (Chan, Song, Shi TISSEC '11: https://eprint.iacr.org/2010/076.pdf) on numeric data.
 * The values are quantized to multiples of the granularity specified by the data curator (to create "leaves"
 * in the private range query tree). Since bucket boundaries may not fall on the quantized leaf boundaries,
 * leaves are assigned to buckets based on their left boundary value.
 */
public abstract class DyadicDecomposition<T extends Comparable<T>>
        implements IDyadicDecomposition {
    // Range for this (possibly filtered) histogram.
    protected T minValue;
    protected T maxValue;
    // Range for the unfiltered histogram. Used to compute appropriate amount of noise to add.
    public T globalMin;
    public T globalMax;

    protected int bucketCount;
    int numLeaves; // Total number of leaves.
    int globalNumLeaves; // Number of leaves in base histogram.
    /**
     * Covers leaves in [minLeafIdx, minLeafIdx + numLeaves). F
     * or infinite ranges, these values are absolute and centered
     * at zeroValue (i.e. minLeafIdx can be negative).
     */
    int minLeafIdx;
    int[] bucketLeftLeaves; // boundaries in terms of relative leaf indices
    /**
     * Boundaries in terms of leaf boundary values (for faster search).
     * Note that this is only used for computing bucket membership.
     * The UI will render buckets as if they were
     * evenly spaced within the specified interval, as usual.
     */
    @Nullable
    T[] bucketLeftBoundaries;

    // For testing
    public long bucketLeafIdx(final int bucketIdx) {
        if (bucketIdx < 0 || bucketIdx > this.bucketCount - 1) return -1;
        return this.bucketLeftLeaves[bucketIdx] + this.minLeafIdx;
    }

    protected abstract T leafLeftBoundary(final int leafIdx);

    /**
     * @param minValue the minimum value in the filtered histogram.
     * @param maxValue the maximum value in the filtered histogram.
     * @param globalMin the minimum value for the unfiltered histogram.
     * @param globalMax the maximum value for the unfiltered histogram.
     * @param bucketCount the number of histogram buckets.
     */
    DyadicDecomposition(final T minValue, final T maxValue,
                        final T globalMin, final T globalMax,
                        final int bucketCount) {
        if ((maxValue.compareTo(minValue) < 0) || bucketCount <= 0)
            throw new IllegalArgumentException("Negative range or number of buckets");

        if (maxValue.compareTo(minValue) <= 0)
            this.bucketCount = 1;  // ignore specified number of buckets
        else
            this.bucketCount = bucketCount;
        this.bucketLeftLeaves = new int[this.bucketCount];
        this.globalMax = globalMax;
        this.globalMin = globalMin;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    /**
     * Assign leaves appropriately to this.numOfBuckets buckets such that each bucket
     * contains approximately the same number of leaves.
     */
    private void populateBucketBoundaries() {
        double leavesPerBucket = (double)(this.numLeaves) / this.bucketCount; // approximate leaves per bucket
        int defaultLeaves = (int)Math.floor(leavesPerBucket);
        int overflow = (int)Math.ceil(1.0/(leavesPerBucket - defaultLeaves)); // interval at which to add extra leaf

        int prevLeafIdx = 0;
        this.bucketLeftLeaves[0] = prevLeafIdx;
        for (int i = 1; i < this.bucketCount; i++) {
            if (overflow > 0 && i % overflow == 0) {
                this.bucketLeftLeaves[i] = prevLeafIdx + defaultLeaves + 1;
            } else {
                this.bucketLeftLeaves[i] = prevLeafIdx + defaultLeaves;
            }

            prevLeafIdx = this.bucketLeftLeaves[i];
        }

        Converters.checkNull(this.bucketLeftBoundaries);
        for (int i = 0; i < this.bucketLeftBoundaries.length; i++) {
            this.bucketLeftBoundaries[i] = this.leafLeftBoundary(this.minLeafIdx + this.bucketLeftLeaves[i]);
        }
    }

    /**
     * Compute the largest leaf whose left boundary is <= value
     */
    int computeLeafIdx(T value) {
        int leafIdx = 0;
        while (this.leafLeftBoundary(leafIdx).compareTo(value) < 0) {
            leafIdx++;
        }
        if (this.leafLeftBoundary(leafIdx).compareTo(value) > 0) leafIdx--;
        return leafIdx;
    }

    /**
     * Additional initialization that calls abstract methods, so may depend on subclass variables.
     */
    protected void init(int numLeaves) {
        this.minLeafIdx = computeLeafIdx(this.minValue);
        this.numLeaves = numLeaves;
        this.populateBucketBoundaries();
    }

    /**
     * Get the number of leaves that would correspond to this bucket.
     * If bucket boundaries and leaf boundaries don't align,
     * we assign leaves to the bucket that their left boundary falls in.
     */
    public long numLeavesInBucket(int bucketIdx) {
        if (bucketIdx >= this.bucketCount || bucketIdx < 0) throw new IllegalArgumentException("Invalid bucket index");
        if (bucketIdx == this.bucketCount - 1) {
            return this.numLeaves - this.bucketLeftLeaves[bucketIdx];
        }

        return (this.bucketLeftLeaves[bucketIdx + 1] -
                this.bucketLeftLeaves[bucketIdx]);
    }

    public T getMin() { return this.minValue; }
    public T getMax() { return this.maxValue; }

    public int getGlobalNumLeaves() { return this.globalNumLeaves; }
}
