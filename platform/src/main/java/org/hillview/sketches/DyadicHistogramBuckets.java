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

package org.hillview.sketches;

import org.hillview.table.api.IColumn;

import java.util.Arrays;

/**
 * This bucket class is intended for use with the binary mechanism for differential privacy
 * (Chan, Song, Shi TISSEC '11: https://eprint.iacr.org/2010/076.pdf) on numeric data.
 * The values are quantized to multiples of the granularity specified by the data curator (to create "leaves"
 * in the private range query tree). Since bucket boundaries may not fall on the quantized leaf boundaries,
 * leaves are assigned to buckets based on their left boundary value.
 */
public class DyadicHistogramBuckets implements IHistogramBuckets {
    private double minValue;
    private double maxValue;
    private int numOfBuckets;

    private final double bucketSize; // The desired bucket size specified by the user or UI.

    private final double granularity; // The quantization interval (leaf size).
    private int numLeaves; // Total number of leaves.

    // Covers leaves in [minLeafIdx, maxLeafIdx)
    private int maxLeafIdx;
    private int minLeafIdx;

    private int[] bucketLeftLeaves; // boundaries in terms of relative leaf indices
    private double[] bucketLeftBoundaries; // boundaries in terms of leaf boundary values (for faster search)

    // compute leaves relative to zero to be globally consistent
    private double leafLeftBoundary(final int leafIdx) {
        return leafIdx * granularity;
    }

    // For testing
    public int bucketLeafIdx(final int bucketIdx) {
        if (bucketIdx < 0 || bucketIdx > this.numOfBuckets - 1) return -1;
        return this.bucketLeftLeaves[bucketIdx] + this.minLeafIdx;
    }

    // Assign leaves appropriately to this.numOfBuckets buckets
    // Could eventually replace with preprocessing similar to StringHistogram
    private void populateBucketBoundaries() {
        double leavesPerBucket = (double)(this.numLeaves) / this.numOfBuckets; // approximate leaves per bucket
        int defaultLeaves = (int)Math.floor(leavesPerBucket);
        int overflow = (int)Math.ceil(1.0/(leavesPerBucket - defaultLeaves)); // interval at which to add extra leaf

        int prevLeafIdx = 0;
        int total = 0;
        this.bucketLeftLeaves[0] = prevLeafIdx;
        for (int i = 1; i < this.numOfBuckets; i++) {
            if (overflow > 0 && i % overflow == 0) {
                this.bucketLeftLeaves[i] = prevLeafIdx + defaultLeaves + 1;
            } else {
                this.bucketLeftLeaves[i] = prevLeafIdx + defaultLeaves;
            }

            prevLeafIdx = this.bucketLeftLeaves[i];
            total += this.bucketLeftLeaves[i];
        }

        for (int i = 0; i < this.bucketLeftBoundaries.length; i++) {
            this.bucketLeftBoundaries[i] = leafLeftBoundary(this.minLeafIdx + this.bucketLeftLeaves[i]);
        }
    }

    public DyadicHistogramBuckets(final double minValue, final double maxValue,
                                  final int numOfBuckets, final double granularity) {
        if (maxValue < minValue || numOfBuckets <= 0)
            throw new IllegalArgumentException("Negative range or number of buckets");

        double range = maxValue - minValue;
        if (range <= 0)
            this.numOfBuckets = 1;  // ignore specified number of buckets
        else
            this.numOfBuckets = numOfBuckets;

        this.granularity = granularity;

        this.bucketSize = (maxValue - minValue) / this.numOfBuckets;
        this.numLeaves = (int)((maxValue - minValue) / this.granularity);

        // Preserves semantics, will make noise computation easier
        if (this.numLeaves < this.numOfBuckets) {
            this.numOfBuckets = this.numLeaves;
        }

        // User-specified range may not fall on leaf boundaries.
        // We choose max/min leaves such that the entire range is covered, i.e.
        // the left boundary of the min leaf <= the range min, and the
        // right boundary of the max leaf >= the range max.

        // Initialize max/min leaves with a linear search so we can do binary search afterwards.
        this.minLeafIdx = 0;
        if (minValue < 0) {
            while (this.leafLeftBoundary(this.minLeafIdx) > minValue) {
                this.minLeafIdx--;
            }
        } else {
            while (this.leafLeftBoundary(this.minLeafIdx) < minValue) {
                this.minLeafIdx++;
            }
            if (this.leafLeftBoundary(this.minLeafIdx) > minValue) this.minLeafIdx--;
        }

        this.maxLeafIdx = 0;
        if (maxValue < 0) {
            while (this.leafLeftBoundary(this.maxLeafIdx) > maxValue) {
                this.maxLeafIdx--;
            }
            if ((this.leafLeftBoundary(this.maxLeafIdx) + this.granularity) <= maxValue) this.maxLeafIdx++;
        } else {
            while ((this.leafLeftBoundary(this.maxLeafIdx) + granularity) <= maxValue) {
                this.maxLeafIdx++;
            }
        }

        this.numLeaves = this.maxLeafIdx - this.minLeafIdx;

        this.bucketLeftLeaves = new int[this.numOfBuckets];
        this.bucketLeftBoundaries = new double[this.numOfBuckets];
        this.populateBucketBoundaries();

        this.minValue = this.bucketLeftBoundaries[0];
        this.maxValue = this.minValue + (this.numLeaves * this.granularity);
    }

    public int indexOf(double value) {
        if ((value < this.minValue) || (value > this.maxValue))
            return -1;

        int index = Arrays.binarySearch(this.bucketLeftBoundaries, value);
        // This method returns index of the search key, if it is contained in the array,
        // else it returns (-(insertion point) - 1). The insertion point is the point
        // at which the key would be inserted into the array: the index of the first
        // element greater than the key, or a.length if all elements in the array
        // are less than the specified key.
        if (index < 0) {
            index = -index - 1;
            if (index == 0)
                // before first element
                return -1;
            return index - 1;
        }
        return index;
    }

    // Get the number of leaves that would correspond to this bucket.
    // If bucket boundaries and leaf boundaries don't align,
    // we assign leaves to the bucket that their left boundary falls in.
    public int numLeavesInBucket(int bucketIdx) {
        if (bucketIdx >= this.numOfBuckets || bucketIdx < 0) throw new IllegalArgumentException("Invalid bucket index");
        if (bucketIdx == this.numOfBuckets - 1) {
            return this.numLeaves - this.bucketLeftLeaves[bucketIdx];
        }

        return (this.bucketLeftLeaves[bucketIdx + 1] -
                this.bucketLeftLeaves[bucketIdx]);
    }

    public double getMin() { return this.minValue; }
    public double getMax() { return this.maxValue; }

    @Override
    public int indexOf(IColumn column, int rowIndex) {
        double item = column.asDouble(rowIndex);
        return this.indexOf(item);
    }

    @Override
    public int getNumOfBuckets() { return this.numOfBuckets; }
}
