package org.hillview.dataStructures;

import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.Pair;
import org.hillview.sketches.DyadicHistogramBuckets;
import org.hillview.sketches.Heatmap;

import java.io.Serializable;
import java.util.ArrayList;

public class PrivateHeatmap implements Serializable, IJson {
    private DyadicHistogramBuckets bucketDescriptionX;
    private DyadicHistogramBuckets bucketDescriptionY;

    public Heatmap heatmap;

    private double epsilon;

    public PrivateHeatmap(Heatmap heatmap, double epsilon) {
        this.heatmap = heatmap;

        this.bucketDescriptionX = (DyadicHistogramBuckets)heatmap.getBucketDescX();
        this.bucketDescriptionY = (DyadicHistogramBuckets)heatmap.getBucketDescY();

        this.epsilon = epsilon;

        this.addDyadicLaplaceNoise();
    }

    // Compute noise to add to this bucket using the dyadic decomposition as the PRG seed.
    // If cdfBuckets is true, computes the noise based on the dyadic decomposition of the interval [0, bucket right leaf]
    // rather than [bucket left leaf, bucket right leaf].
    // Returns the noise and the total variance of the variables used to compute the noise.
    public Pair<Double, Double> noiseForBucket(int bucketXIdx, int bucketYIdx) {
        ArrayList<Pair<Integer, Integer>> xIntervals = bucketDescriptionX.bucketDecomposition(bucketXIdx, false);
        ArrayList<Pair<Integer, Integer>> yIntervals = bucketDescriptionY.bucketDecomposition(bucketYIdx, false);

        double noise = 0;
        double variance = 0;
        long totalLeaves = bucketDescriptionX.getGlobalNumLeaves() * bucketDescriptionY.getGlobalNumLeaves();
        double scale = Math.log(totalLeaves / this.epsilon) / Math.log(2);

        for (Pair<Integer, Integer> x : xIntervals) {
            for (Pair<Integer, Integer> y : yIntervals) {
                LaplaceDistribution dist = new LaplaceDistribution(0, scale); // TODO: (more) secure PRG
                Pair<Pair<Integer, Integer>, Pair<Integer, Integer>> xy = new Pair<>(x, y);
                dist.reseedRandomGenerator(xy.hashCode()); // Each node's noise should be deterministic, based on node's ID
                noise += dist.sample();
                variance += 2 * (Math.pow(scale, 2));
            }
        }

        return new Pair(noise, variance);
    }

    /**
     * Add Laplace noise compatible with the binary mechanism to each bucket.
     * Noise is added as follows:
     * Let T := (globalMax - globalMin) / granularity, the total number of leaves in the data overall.
     * Each node in the dyadic interval tree is perturbed by an independent noise variable distributed as Laplace(log T / epsilon).
     * The total noise is the sum of the noise variables in the intervals composing the desired interval or bucket.
     */
    private void addDyadicLaplaceNoise() {
        for (int i = 0; i < this.heatmap.buckets.length; i++) {
            for (int j = 0; j < this.heatmap.buckets[i].length; j++) {
                Pair<Double, Double> noise = this.noiseForBucket(i, j);
                this.heatmap.buckets[i][j] += noise.first;
                // Postprocess so that no buckets are negative
                this.heatmap.buckets[i][j] = Math.max(0, this.heatmap.buckets[i][j]);
            }
        }
    }
}
