package org.hillview.dataStructures;

import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.Pair;
import org.hillview.sketches.results.Heatmap;
import org.hillview.utils.Converters;
import java.io.Serializable;
import java.util.List;

public class PrivateHeatmap implements Serializable, IJson {
    public Heatmap heatmap;
    private double epsilon;

    public PrivateHeatmap(IDyadicDecomposition xb, IDyadicDecomposition yb, Heatmap heatmap, double epsilon) {
        this.heatmap = heatmap;
        this.epsilon = epsilon;
        this.addDyadicLaplaceNoise(xb, yb);
    }

    /**
     * Compute noise to add to this bucket using the dyadic decomposition as the PRG seed.
     * If cdfBuckets is true, computes the noise based on the dyadic decomposition of the interval [0, bucket right leaf]
     * rather than [bucket left leaf, bucket right leaf].
     * Returns the noise and the total variance of the variables used to compute the noise.
     */
    private Pair<Double, Double> noiseForBucket(IDyadicDecomposition xb,
                                                IDyadicDecomposition yb,
                                                int bucketXIdx, int bucketYIdx) {
        List<Pair<Integer, Integer>> xIntervals = xb.bucketDecomposition(bucketXIdx, false);
        List<Pair<Integer, Integer>> yIntervals = yb.bucketDecomposition(bucketYIdx, false);

        double noise = 0;
        double variance = 0;
        long totalLeaves = xb.getGlobalNumLeaves() * yb.getGlobalNumLeaves();
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

        return new Pair<Double, Double>(noise, variance);
    }

    /**
     * Add Laplace noise compatible with the binary mechanism to each bucket.
     * Noise is added as follows:
     * Let T := (globalMax - globalMin) / granularity, the total number of leaves in the data overall.
     * Each node in the dyadic interval tree is perturbed by an independent noise variable distributed as Laplace(log T / epsilon).
     * The total noise is the sum of the noise variables in the intervals composing the desired interval or bucket.
     */
    private void addDyadicLaplaceNoise(IDyadicDecomposition dx, IDyadicDecomposition dy) {
        for (int i = 0; i < this.heatmap.buckets.length; i++) {
            for (int j = 0; j < this.heatmap.buckets[i].length; j++) {
                Pair<Double, Double> noise = this.noiseForBucket(dx, dy, i, j);
                Converters.checkNull(noise.first);
                this.heatmap.buckets[i][j] += noise.first;
                // Postprocess so that no buckets are negative
                this.heatmap.buckets[i][j] = Math.max(0, this.heatmap.buckets[i][j]);
            }
        }
    }
}
