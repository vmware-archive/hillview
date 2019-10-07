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

import org.hillview.dataset.api.Pair;
import org.hillview.table.api.IColumn;
import org.hillview.table.columns.DoubleColumnPrivacyMetadata;
import org.hillview.utils.Utilities;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * This bucket class is intended for use with the binary mechanism for differential privacy
 * (Chan, Song, Shi TISSEC '11: https://eprint.iacr.org/2010/076.pdf) on numeric data.
 * The values are quantized to multiples of the granularity specified by the data curator (to create "leaves"
 * in the private range query tree). Since bucket boundaries may not fall on the quantized leaf boundaries,
 * leaves are assigned to buckets based on their left boundary value.
 */
public class DyadicDoubleHistogramBuckets implements IHistogramBuckets {
    // Range for this (possibly filtered) histogram.
    private double minValue;
    private double maxValue;

    // Range for the unfiltered histogram. Used to compute appropriate amount of noise to add.
    public double globalMin;
    public double globalMax;

    private int numOfBuckets;

    public final double granularity; // The quantization interval (leaf size).
    private int numLeaves; // Total number of leaves.
    private int globalNumLeaves; // Number of leaves in base histogram.

    // Covers leaves in [minLeafIdx, maxLeafIdx).
    private long maxLeafIdx;
    private long minLeafIdx;

    private int[] bucketLeftLeaves; // boundaries in terms of relative leaf indices

    // Boundaries in terms of leaf boundary values (for faster search).
    // Note that this is only used for computing bucket membership. The UI will render buckets as if they were
    // evenly spaced within the specified interval, as usual.
    private double[] bucketLeftBoundaries;

    // compute leaves relative to zero to be globally consistent
    private double leafLeftBoundary(final long leafIdx) {
        return leafIdx * granularity;
    }

    // For testing
    public long bucketLeafIdx(final int bucketIdx) {
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
        this.bucketLeftLeaves[0] = prevLeafIdx;
        for (int i = 1; i < this.numOfBuckets; i++) {
            if (overflow > 0 && i % overflow == 0) {
                this.bucketLeftLeaves[i] = prevLeafIdx + defaultLeaves + 1;
            } else {
                this.bucketLeftLeaves[i] = prevLeafIdx + defaultLeaves;
            }

            prevLeafIdx = this.bucketLeftLeaves[i];
        }

        for (int i = 0; i < this.bucketLeftBoundaries.length; i++) {
            this.bucketLeftBoundaries[i] = leafLeftBoundary(this.minLeafIdx + this.bucketLeftLeaves[i]);
        }
    }

    /**
     * Compute the largest leaf whose left boundary is <= min
     */
     private long computeMinLeafIdx(double min) {
        long minIdx = 0;
        if (min < 0) {
            while (this.leafLeftBoundary(minIdx) > min) {
                minIdx--;
            }
        } else {
            while (this.leafLeftBoundary(minIdx) < min) {
                minIdx++;
            }
            if (this.leafLeftBoundary(minIdx) > min) minIdx--;
        }

        return minIdx;
    }

    /**
     * Compute the smallest leaf whose right boundary is >= max
     */
    private long computeMaxLeafIdx(double max) {
        long maxIdx = 0;
        if (max < 0) {
            while (this.leafLeftBoundary(maxIdx) > max) {
                maxIdx--;
            }
            if ((this.leafLeftBoundary(maxIdx) + this.granularity) <= max) this.maxLeafIdx++;
        } else {
            while ((this.leafLeftBoundary(maxIdx) + granularity) <= max) {
                maxIdx++;
            }
        }

        return maxIdx;
    }

    public DyadicDoubleHistogramBuckets(final double minValue, final double maxValue,
                                        final int numOfBuckets, DoubleColumnPrivacyMetadata metadata) {
        if (maxValue < minValue || numOfBuckets <= 0)
            throw new IllegalArgumentException("Negative range or number of buckets");

        double range = maxValue - minValue;
        if (range <= 0)
            this.numOfBuckets = 1;  // ignore specified number of buckets
        else
            this.numOfBuckets = numOfBuckets;

        this.granularity = metadata.granularity;
        this.globalMin = metadata.globalMin;
        this.globalMax = metadata.globalMax;

        this.numLeaves = (int)((maxValue - minValue) / this.granularity);
        this.globalNumLeaves = (int)((this.globalMax - this.globalMin) / this.granularity);

        // Preserves semantics, will make noise computation easier
        if (this.numLeaves < this.numOfBuckets) {
            this.numOfBuckets = this.numLeaves;
        }

        // User-specified range may not fall on leaf boundaries.
        // We choose max/min leaves such that the entire range is covered, i.e.
        // the left boundary of the min leaf <= the range min, and the
        // right boundary of the max leaf >= the range max.

        // Initialize max/min leaves with a linear search so we can do binary search afterwards.
        this.minLeafIdx = this.computeMinLeafIdx(minValue);
        this.maxLeafIdx = this.computeMaxLeafIdx(maxValue);

        this.numLeaves = (int)(this.maxLeafIdx - this.minLeafIdx);

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
    public long numLeavesInBucket(int bucketIdx) {
        if (bucketIdx >= this.numOfBuckets || bucketIdx < 0) throw new IllegalArgumentException("Invalid bucket index");
        if (bucketIdx == this.numOfBuckets - 1) {
            return this.numLeaves - this.bucketLeftLeaves[bucketIdx];
        }

        return (this.bucketLeftLeaves[bucketIdx + 1] -
                this.bucketLeftLeaves[bucketIdx]);
    }

    // Return the dyadic decomposition of this interval, as a list of <left boundary, right boundary> pairs
    // for each interval in the decomposition. The decomposition assumes that the first leaf of the dyadic tree
    // is at index 0.
    public static ArrayList<Pair<Integer, Integer>> dyadicDecomposition(int left, int right) {
        if (left < 0 || right < left) {
            throw new IllegalArgumentException("Invalid interval bounds");
        }

        ArrayList<Pair<Integer, Integer>> nodes = new ArrayList<>();
        while (left < right) {
            // get largest valid interval starting at left and not extending past right
            int lob = Integer.lowestOneBit(left);
            int lsb = lob > 0 ? Utilities.intLog2(lob) : -1; // smallest power of 2 that divides left

            int rem = Utilities.intLog2(right - left); // smallest power of 2 contained in remaining interval

            int pow = lsb < 0 ? rem : Math.min(lsb, rem); // largest valid covering interval
            int nodeEnd = (int)Math.pow(2, pow);

            nodes.add(new Pair<Integer, Integer>(left, nodeEnd));

            left += nodeEnd;
        }

        assert(right == left);

        return nodes;
    }

    // Compute the intervals in the dyadic tree that correspond to this bucket.
    public ArrayList<Pair<Integer, Integer>> bucketDecomposition(int bucketIdx, boolean cdf) {
        if (bucketIdx >= this.numOfBuckets || bucketIdx < 0) {
            throw new IllegalArgumentException("Invalid bucket index");
        }

        int leftLeaf;
        if (cdf) {
            leftLeaf = 0;
        } else {
            leftLeaf = this.bucketLeftLeaves[bucketIdx];
        }

        int rightLeaf;
        if (bucketIdx == this.numOfBuckets - 1) {
            rightLeaf = this.numLeaves;
        } else {
            rightLeaf = this.bucketLeftLeaves[bucketIdx + 1];
        }

        return dyadicDecomposition(leftLeaf, rightLeaf);
    }

    public double getMin() { return this.minValue; }
    public double getMax() { return this.maxValue; }

    @Override
    public int indexOf(IColumn column, int rowIndex) {
        double item = column.asDouble(rowIndex);
        return this.indexOf(item);
    }

    @Override
    public int getBucketCount() { return this.numOfBuckets; }

    public int getGlobalNumLeaves() { return this.globalNumLeaves; }

    public double getGranularity() { return this.granularity; }
}
