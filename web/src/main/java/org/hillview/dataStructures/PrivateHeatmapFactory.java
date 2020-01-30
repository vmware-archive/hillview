package org.hillview.dataStructures;

import org.hillview.dataset.api.Pair;
import org.hillview.security.SecureLaplace;
import org.hillview.sketches.results.Heatmap;
import org.hillview.utils.*;

import java.util.ArrayList;
import java.util.List;

import static org.hillview.dataStructures.IntervalDecomposition.kadicDecomposition;

/**
 * This class is used to add noise to a heatmap.
 */
public class PrivateHeatmapFactory {
    public Heatmap heatmap;
    private double epsilon;
    private SecureLaplace laplace;

    double scale;
    double baseVariance;

    IntervalDecomposition dx;
    IntervalDecomposition dy;

    private final int columnsIndex;

    public PrivateHeatmapFactory(int columnsIndex,
            IntervalDecomposition d0, IntervalDecomposition d1, Heatmap heatmap, double epsilon, SecureLaplace laplace) {
        this.columnsIndex = columnsIndex;

        this.heatmap = heatmap;
        this.epsilon = epsilon;
        this.laplace = laplace;

        this.dx = d0;
        this.dy = d1;

        this.scale = PrivacyUtils.computeNoiseScale(this.epsilon, d0, d1);
        this.baseVariance = PrivacyUtils.laplaceVariance(scale);

        this.addDyadicLaplaceNoise(d0, d1);
    }

    /**
     * Compute noise to add to this bucket using the dyadic decomposition as the PRG seed.
     * If cdfBuckets is true, computes the noise based on the dyadic decomposition of the interval [0, bucket right leaf]
     * rather than [bucket left leaf, bucket right leaf].
     * Stores the noise and the total variance of the variables used to compute the noise in the `noise` output.
     * Returns the total number of intervals used to compute the noise.
     */
    private int noiseForDecomposition(
            List<Pair<Integer, Integer>> xIntervals,
            List<Pair<Integer, Integer>> yIntervals,
            double scale,
            double baseVariance,
            /*out*/Noise result) {
        result.clear();
        for (Pair<Integer, Integer> x : xIntervals) {
            for (Pair<Integer, Integer> y : yIntervals) {
                result.add(this.laplace.sampleLaplace(this.columnsIndex, scale, x, y), baseVariance);
            }
        }

        return xIntervals.size() * yIntervals.size();
    }

    public int noiseForRange(int left, int right, int top, int bot,
                              double scale, double baseVariance, /*out*/Noise result) {
        List<Pair<Integer, Integer>> xIntervals = kadicDecomposition(left, right, IntervalDecomposition.BRANCHING_FACTOR);
        List<Pair<Integer, Integer>> yIntervals = kadicDecomposition(top, bot, IntervalDecomposition.BRANCHING_FACTOR);

        return noiseForDecomposition(xIntervals, yIntervals, scale, baseVariance, result);
    }

    /**
     * Add Laplace noise compatible with the binary mechanism to each bucket.
     * Noise is added as follows:
     * Let T := (globalMax - globalMin) / granularity, the total number of leaves in the data overall.
     * Each node in the dyadic interval tree is perturbed by an independent noise variable distributed as Laplace(log T / epsilon).
     * The total noise is the sum of the noise variables in the intervals composing the desired interval or bucket.
     */
    private void addDyadicLaplaceNoise(IntervalDecomposition dx, IntervalDecomposition dy) {
        int xSize = this.heatmap.xBucketCount;
        int ySize = this.heatmap.yBucketCount;

        HillviewLogger.instance.info("Adding heatmap noise with", "epsilon={0}", this.epsilon);
        List<List<Pair<Integer, Integer>>> xIntervals = new ArrayList<List<Pair<Integer, Integer>>>(xSize);
        List<List<Pair<Integer, Integer>>> yIntervals = new ArrayList<List<Pair<Integer, Integer>>>(ySize);
        for (int i = 0; i < xSize; i++)
            xIntervals.add(dx.bucketDecomposition(i, false));
        for (int i = 0; i < ySize; i++)
            yIntervals.add(dy.bucketDecomposition(i, false));

        // Compute the noise.
        Noise noise = new Noise();
        this.heatmap.allocateConfidence();
        Converters.checkNull(this.heatmap.confidence);

        for (int i = 0; i < this.heatmap.buckets.length; i++) {
            for (int j = 0; j < this.heatmap.buckets[i].length; j++) {
                int numIntervals = this.noiseForDecomposition(xIntervals.get(i), yIntervals.get(j), this.scale, this.baseVariance, noise);
                this.heatmap.buckets[i][j] += noise.getNoise();
                this.heatmap.confidence[i][j] = Utilities.toInt(PrivacyUtils.laplaceCI(numIntervals, this.scale,
                        PrivacyUtils.DEFAULT_ALPHA).second);
            }
        }
    }

    public double getEpsilon() {
        return this.epsilon;
    }
}
