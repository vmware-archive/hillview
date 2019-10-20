package org.hillview.dataStructures;

import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.Pair;
import org.hillview.sketches.results.Heatmap;
import org.hillview.utils.HashUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PrivateHeatmap implements Serializable, IJson {
    public Heatmap heatmap;
    private double epsilon;

    public PrivateHeatmap(DyadicDecomposition d0, DyadicDecomposition d1,
                          Heatmap heatmap, double epsilon) {
        this.heatmap = heatmap;
        this.epsilon = epsilon;
        this.addDyadicLaplaceNoise(d0, d1);
    }

    static class Noise {
        double noise;
        double variance;
    }

    /**
     * Compute noise to add to this bucket using the dyadic decomposition as the PRG seed.
     * If cdfBuckets is true, computes the noise based on the dyadic decomposition of the interval [0, bucket right leaf]
     * rather than [bucket left leaf, bucket right leaf].
     * Returns the noise and the total variance of the variables used to compute the noise.
     */
    @SuppressWarnings("ConstantConditions")
    private void noiseForBucket(
            List<Pair<Integer, Integer>> xIntervals,
            List<Pair<Integer, Integer>> yIntervals,
            LaplaceDistribution dist,
            double baseVariance,
            /*out*/Noise result) {
        result.noise = 0.0;
        result.variance = 0.0;

        int hashCode = 31;
        for (Pair<Integer, Integer> x : xIntervals) {
            for (Pair<Integer, Integer> y : yIntervals) {
                hashCode = HashUtil.murmurHash3(hashCode, x.first);
                hashCode = HashUtil.murmurHash3(hashCode, x.second);
                hashCode = HashUtil.murmurHash3(hashCode, y.first);
                hashCode = HashUtil.murmurHash3(hashCode, y.second);
                dist.reseedRandomGenerator(hashCode);
                result.noise += dist.sample();
                result.variance += baseVariance;
            }
        }
    }

    /**
     * Add Laplace noise compatible with the binary mechanism to each bucket.
     * Noise is added as follows:
     * Let T := (globalMax - globalMin) / granularity, the total number of leaves in the data overall.
     * Each node in the dyadic interval tree is perturbed by an independent noise variable distributed as Laplace(log T / epsilon).
     * The total noise is the sum of the noise variables in the intervals composing the desired interval or bucket.
     */
    private void addDyadicLaplaceNoise(DyadicDecomposition dx, DyadicDecomposition dy) {
        List<List<Pair<Integer, Integer>>> xIntervals =
                new ArrayList<List<Pair<Integer, Integer>>>(this.heatmap.xBucketCount);
        List<List<Pair<Integer, Integer>>> yIntervals =
                new ArrayList<List<Pair<Integer, Integer>>>(this.heatmap.yBucketCount);
        for (int i = 0; i < this.heatmap.xBucketCount; i++)
            xIntervals.add(dx.bucketDecomposition(i, false));
        for (int i = 0; i < this.heatmap.yBucketCount; i++)
            yIntervals.add(dy.bucketDecomposition(i, false));

        Noise noise = new Noise();
        long totalLeaves = dx.getQuantizationIntervalCount() * dy.getQuantizationIntervalCount();
        double scale = Math.log(totalLeaves / this.epsilon) / Math.log(2);
        LaplaceDistribution dist = new LaplaceDistribution(0, scale);
        double baseVariance = 2 * (Math.pow(scale, 2));
        for (int i = 0; i < this.heatmap.buckets.length; i++) {
            for (int j = 0; j < this.heatmap.buckets[i].length; j++) {
                this.noiseForBucket(xIntervals.get(i), yIntervals.get(j), dist, baseVariance, noise);
                this.heatmap.buckets[i][j] += noise.noise;
                // TODO: use variance
                this.heatmap.buckets[i][j] = Math.max(0, this.heatmap.buckets[i][j]);
            }
        }
    }
}
