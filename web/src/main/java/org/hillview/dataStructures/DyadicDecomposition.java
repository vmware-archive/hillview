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

import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.hillview.dataset.api.Pair;
import org.hillview.sketches.results.IHistogramBuckets;
import org.hillview.table.columns.ColumnQuantization;
import org.hillview.utils.HashUtil;
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
public abstract class DyadicDecomposition {
    /**
     * For each bucket boundary the quantization interval it belongs to.
     */
    final int[] bucketQuantizationIndexes;
    final ColumnQuantization quantization;
    private final IHistogramBuckets buckets;

    DyadicDecomposition(ColumnQuantization quantization, IHistogramBuckets buckets) {
        this.bucketQuantizationIndexes = new int[buckets.getBucketCount()];
        this.buckets = buckets;
        this.quantization = quantization;
    }

    /**
     * Return the dyadic decomposition of this interval, as a list of <left boundary, right boundary> pairs
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
            int rem = Utilities.intLog2(right - left); // smallest power of 2 contained in remaining interval
            int pow = lsb < 0 ? rem : Math.min(lsb, rem); // largest valid covering interval
            int nodeEnd = (int)Math.pow(2, pow);
            nodes.add(new Pair<Integer, Integer>(left, nodeEnd));
            left += nodeEnd;
        }

        assert(right == left);
        return nodes;
    }

    public IHistogramBuckets getHistogramBuckets() {
        return this.buckets;
    }

    int getQuantizationIntervalCount() {
        return this.quantization.getIntervalCount();
    }

    /**
     * Return a list of leaf intervals that correspond to this bucket.
     * For numeric data, this could correspond to the dyadic decomposition of the bucket.
     * For categorical data (one-level tree), this can return a list of leaves that make
     * up the bucket.
     * Leaves are zero-indexed. The returned intervals are right-exclusive.
     */
    List<Pair<Integer, Integer>> bucketDecomposition(int bucketIdx, boolean cdf) {
        int left = 0;
        if (!cdf)
            left = this.bucketQuantizationIndexes[bucketIdx];
        int right = -1;
        if (bucketIdx < this.bucketQuantizationIndexes.length - 1)
            right = this.bucketQuantizationIndexes[bucketIdx + 1];
        if (left < 0 && right >= 0)
            // left endpoint ot of bounds
            left = 0;
        if (left >= 0 && right < 0)
            // right endpoint out of bounds
            right = left + 1;
        return DyadicDecomposition.dyadicDecomposition(left, right);
    }

    /**
     * Compute noise to add to this bucket using the dyadic decomposition as the PRG seed.
     * @param bucketIdx: index of the bucket to compute noise for.
     * @param dist:      laplace distribution used to sample data
     * @param baseVariance:  factor added to variance for each bucket
     * @param epsilon    Amount of privacy allocated for this computation.
     * @param isCdf: If true, computes the noise based on the dyadic decomposition of the interval [0, bucket right leaf]
     *             rather than [bucket left leaf, bucket right leaf].
     * Returns the noise and the total variance of the variables used to compute the noise.
     */
    @SuppressWarnings("ConstantConditions")
    Pair<Double, Double> noiseForBucket(int bucketIdx, double epsilon,
                                        LaplaceDistribution dist, double baseVariance, boolean isCdf) {
        List<Pair<Integer, Integer>> intervals = this.bucketDecomposition(bucketIdx, isCdf);
        double noise = 0;
        double variance = 0;

        int hashCode = 31;
        for (Pair<Integer, Integer> x : intervals) {
            hashCode = HashUtil.murmurHash3(hashCode, x.first);
            hashCode = HashUtil.murmurHash3(hashCode, x.second);
            dist.reseedRandomGenerator(hashCode);
            noise += dist.sample();
            variance += baseVariance;
        }
        return new Pair<Double, Double>(noise, variance);
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
}
