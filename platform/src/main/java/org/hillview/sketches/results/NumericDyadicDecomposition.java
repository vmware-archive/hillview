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

package org.hillview.sketches.results;

import org.hillview.dataset.api.Pair;
import org.hillview.table.api.IColumn;
import org.hillview.table.columns.DoubleColumnPrivacyMetadata;
import org.hillview.utils.Utilities;

import java.util.ArrayList;

public class NumericDyadicDecomposition extends DyadicDecomposition<Double> {
    public final double granularity; // The quantization interval (leaf size).

    /**
     * Returns the boundary relative to globalMin.
     */
    @Override
    protected Double leafLeftBoundary(final int leafIdx) {
        return leafIdx * granularity;
    }

    public NumericDyadicDecomposition(final double minValue, final double maxValue,
                                      final int bucketCount, DoubleColumnPrivacyMetadata metadata) {
        super(minValue, maxValue, metadata.globalMin, metadata.globalMax, 0.0, bucketCount);
        this.granularity = metadata.granularity;

        this.globalMin = metadata.globalMin;
        this.globalMax = metadata.globalMax;

        this.numLeaves = (int)((maxValue - minValue) / this.granularity);
        this.globalNumLeaves = (int)((this.globalMax - this.globalMin) / this.granularity);

        // Preserves semantics, will make noise computation easier
        if (this.numLeaves < this.bucketCount) {
            this.bucketCount = this.numLeaves;
        }

        this.bucketLeftBoundaries = new Double[this.bucketCount];
        this.init();

        // adjust min/max to reflect snapping to leaf boundaries
        this.minValue = this.bucketLeftBoundaries[0];
        this.maxValue = this.minValue + (this.numLeaves * this.granularity);
    }

    // For testing
    public long bucketLeafIdx(final int bucketIdx) {
        if (bucketIdx < 0 || bucketIdx > this.bucketCount - 1) return -1;
        return this.bucketLeftLeaves[bucketIdx] + this.minLeafIdx;
    }

    /**
     * Return the dyadic decomposition of this interval, as a list of <left boundary, right boundary> pairs
     * for each interval in the decomposition. The decomposition assumes that the first leaf of the dyadic tree
     * is at index 0.
     */
    public static ArrayList<Pair<Integer, Integer>> dyadicDecomposition(int left, int right) {
        if (left < 0 || right < left) {
            throw new IllegalArgumentException("Invalid interval bounds");
        }

        ArrayList<Pair<Integer, Integer>> nodes = new ArrayList<Pair<Integer, Integer>>();
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

    /**
     * Compute the intervals in the dyadic tree that correspond to this bucket.
     */
    @Override
    public ArrayList<Pair<Integer, Integer>> bucketDecomposition(int bucketIdx, boolean cdf) {
        if (bucketIdx >= this.bucketCount || bucketIdx < 0) {
            throw new IllegalArgumentException("Invalid bucket index");
        }

        int leftLeaf;
        if (cdf) {
            leftLeaf = 0;
        } else {
            leftLeaf = this.bucketLeftLeaves[bucketIdx];
        }

        int rightLeaf;
        if (bucketIdx == this.bucketCount - 1) {
            rightLeaf = this.numLeaves;
        } else {
            rightLeaf = this.bucketLeftLeaves[bucketIdx + 1];
        }

        return dyadicDecomposition(leftLeaf, rightLeaf);
    }

    public double getGranularity() { return this.granularity; }

    @Override
    public int indexOf(IColumn column, int rowIndex) {
        double item = column.asDouble(rowIndex);
        return this.indexOf(item);
    }
}
