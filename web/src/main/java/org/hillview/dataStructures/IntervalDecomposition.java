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

import org.hillview.dataset.api.Pair;
import org.hillview.sketches.results.IHistogramBuckets;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.utils.Converters;
import org.hillview.utils.Utilities;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is intended for use with the binary mechanism for differential privacy
 * (Chan, Song, Shi TISSEC '11: https://eprint.iacr.org/2010/076.pdf) on numeric data.
 * The values are quantized to multiples of the granularity specified by the data curator (to create "leaves"
 * in the private range query tree). Since bucket boundaries may not fall on the quantized leaf boundaries,
 * leaves are assigned to buckets based on their left boundary value.
 */
public abstract class IntervalDecomposition {
    /**
     * For each bucket boundary the quantization interval it belongs to.
     */
    final int[] bucketQuantizationIndexes;
    final ColumnQuantization quantization;

    public static final int BRANCHING_FACTOR = 20;

    protected IntervalDecomposition(ColumnQuantization quantization, int[] quantizationIndexes) {
        this.bucketQuantizationIndexes = quantizationIndexes;
        this.quantization = quantization;
    }

    protected IntervalDecomposition(ColumnQuantization quantization, int bucketCount) {
        this.bucketQuantizationIndexes = new int[bucketCount];
        this.quantization = quantization;
    }

    /**
     * Return the dyadic decomposition of this interval, as a list of <left boundary, size> pairs
     * for each interval in the decomposition. The decomposition assumes that the first leaf of the dyadic tree
     * is at index 0.
     */
    public static ArrayList<Pair<Integer, Integer>> dyadicDecomposition(int left, int right) {
        ArrayList<Pair<Integer, Integer>> nodes = new ArrayList<Pair<Integer, Integer>>();
        if (left == right)
            // handles the case -1,-1
            return nodes;

        if (left < 0 || right < left) {
            throw new IllegalArgumentException("Invalid interval bounds: " + left + ":" + right);
        }
        while (left < right) {
            // get largest valid interval starting at left and not extending past right
            int lob = Integer.lowestOneBit(left);
            int lsb = lob > 0 ? Utilities.intLog2(lob) : -1; // smallest power of 2 that divides left
            int rem = Utilities.intLog2(right - left); // largest power of 2 contained in remaining interval
            int pow = lsb < 0 ? rem : Math.min(lsb, rem); // largest valid covering interval
            int nodeEnd = (int)Math.pow(2, pow);
            nodes.add(new Pair<Integer, Integer>(left, nodeEnd));
            left += nodeEnd;
        }

        assert(right == left);
        return nodes;
    }

    /**
     * Return the k-adic decomposition of this interval, i.e. the decomposition corresponding
     * to a tree with degree k. The decomposition assumes that the first leaf of the k-adic tree
     * is at index 0.
     */
    public static ArrayList<Pair<Integer, Integer>> kadicDecomposition(int left, int right, int k) {
        ArrayList<Pair<Integer, Integer>> nodes = new ArrayList<Pair<Integer, Integer>>();
        if (left == right)
            // handles the case -1,-1
            return nodes;

        if (left < 0 || right < left) {
            throw new IllegalArgumentException("Invalid interval bounds: " + left + ":" + right);
        }

        if (right - left == k) {
            // no root node
            for (int i = left; i < right; i++) {
                nodes.add(new Pair<Integer, Integer>(i, 1));
            }
            return nodes;
        }

        while (left < right) {
            // get largest valid interval starting at left and not extending past right
            // smallest power of k that divides left
            int smallestPower = -1;
            if (left > 0) {
                smallestPower = (int) Math.floor(Math.log(left) / Math.log(k));
            }
            // largest power of k that actually fits in remaining interval
            int rem = (int)(Math.log(right - left) / Math.log(k));
            // largest valid covering interval
            int pow = smallestPower < 0 ? rem : Math.min(smallestPower, rem);
            int nodeEnd = (int)Math.pow(k, pow);
            nodes.add(new Pair<Integer, Integer>(left, nodeEnd));
            left += nodeEnd;
        }

        assert(right == left);
        return nodes;
    }

    public int getQuantizationIntervalCount() {
        return this.quantization.getIntervalCount();
    }

    /**
     * Return the start and end leaves for this bucket (right-exclusive).
     */
    public Pair<Integer, Integer> bucketRange(int bucketIdx, boolean cdf) {
        int left = 0;
        if (!cdf)
            left = this.bucketQuantizationIndexes[bucketIdx];
        int right = -1;
        if (bucketIdx < this.bucketQuantizationIndexes.length - 1)
            right = this.bucketQuantizationIndexes[bucketIdx + 1];
        if (left < 0 && right >= 0)
            // left endpoint out of bounds
            left = 0;
        if (left >= 0 && right < 0)
            // right endpoint out of bounds
            right = left + 1;
        return new Pair<Integer, Integer>(left, right);
    }

    /**
     * Return a list of leaf intervals that correspond to this bucket.
     * For numeric data, this could correspond to the dyadic decomposition of the bucket.
     * For categorical data (one-level tree), this can return a list of leaves that make
     * up the bucket.
     * Leaves are zero-indexed. The returned intervals are right-exclusive.
     */
    @SuppressWarnings("SameParameterValue")
    List<Pair<Integer, Integer>> bucketDecomposition(int bucketIdx, boolean cdf) {
        Pair<Integer, Integer> range = this.bucketRange(bucketIdx, cdf);
        return IntervalDecomposition.kadicDecomposition(
            Converters.checkNull(range.first), Converters.checkNull(range.second), BRANCHING_FACTOR);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < this.bucketQuantizationIndexes.length; i++) {
            if (i > 0)
                builder.append(",");
            builder.append(this.bucketQuantizationIndexes[i]);
        }
        return builder.toString();
    }

    /**
     * Combine neighboring buckets and return a new coarser-grain interval decomposition.
     */
    public abstract IntervalDecomposition mergeNeighbors();
}
