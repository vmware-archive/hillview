package org.hillview.dataStructures;

import org.hillview.dataset.PostProcessedSketch;
import org.hillview.dataset.TableSketch;
import org.hillview.dataset.api.ISketch;
import org.hillview.dataset.api.Pair;
import org.hillview.security.SecureLaplace;
import org.hillview.sketches.results.Heatmap;
import org.hillview.table.api.ITable;
import org.hillview.utils.*;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static org.hillview.dataStructures.IntervalDecomposition.kadicDecomposition;

/**
 * Differentially-private heatmap.
 */
public class DPHeatmapSketch extends PostProcessedSketch<ITable, Heatmap, Heatmap> {
    private double epsilon;
    private SecureLaplace laplace;

    double scale;
    double baseVariance;

    IntervalDecomposition dx;
    IntervalDecomposition dy;

    private final int columnsIndex;

    public DPHeatmapSketch(
            ISketch<ITable, Heatmap> sketch,
            int columnsIndex,
            IntervalDecomposition d0, IntervalDecomposition d1,
            double epsilon, SecureLaplace laplace) {
        super(sketch);
        this.columnsIndex = columnsIndex;
        this.epsilon = epsilon;
        this.laplace = laplace;
        this.dx = d0;
        this.dy = d1;
        this.scale = PrivacyUtils.computeNoiseScale(this.epsilon, d0, d1);
        this.baseVariance = PrivacyUtils.laplaceVariance(scale);
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
        @SuppressWarnings("SuspiciousNameCombination")
        List<Pair<Integer, Integer>> yIntervals = kadicDecomposition(top, bot, IntervalDecomposition.BRANCHING_FACTOR);

        return noiseForDecomposition(xIntervals, yIntervals, scale, baseVariance, result);
    }

    public double getEpsilon() {
        return this.epsilon;
    }

    /**
     * Add Laplace noise compatible with the binary mechanism to each bucket.
     * Noise is added as follows:
     * Let T := (globalMax - globalMin) / granularity, the total number of leaves in the data overall.
     * Each node in the dyadic interval tree is perturbed by an independent noise variable distributed as Laplace(log T / epsilon).
     * The total noise is the sum of the noise variables in the intervals composing the desired interval or bucket.
     */
    @Nullable
    @Override
    public Heatmap postProcess(@Nullable Heatmap heatmap) {
        Converters.checkNull(heatmap);
        int xSize = heatmap.xBucketCount;
        int ySize = heatmap.yBucketCount;
        Heatmap result = new Heatmap(xSize, ySize, true);
        Converters.checkNull(result.confidence);

        HillviewLogger.instance.info("Adding heatmap noise with", "epsilon={0}", this.epsilon);
        List<List<Pair<Integer, Integer>>> xIntervals = new ArrayList<List<Pair<Integer, Integer>>>(xSize);
        List<List<Pair<Integer, Integer>>> yIntervals = new ArrayList<List<Pair<Integer, Integer>>>(ySize);
        for (int i = 0; i < xSize; i++)
            xIntervals.add(this.dx.bucketDecomposition(i, false));
        for (int i = 0; i < ySize; i++)
            yIntervals.add(this.dy.bucketDecomposition(i, false));

        // Compute the noise.
        Noise noise = new Noise();
        for (int i = 0; i < heatmap.buckets.length; i++) {
            for (int j = 0; j < heatmap.buckets[i].length; j++) {
                long nIntervals = this.noiseForDecomposition(xIntervals.get(i), yIntervals.get(j), this.scale, this.baseVariance, noise);
                result.buckets[i][j] = Utilities.toLong(heatmap.buckets[i][j] + noise.getNoise());
                result.confidence[i][j] = Utilities.toInt(
                        PrivacyUtils.laplaceCI(nIntervals, this.scale, PrivacyUtils.DEFAULT_ALPHA).second);
            }
        }
        return result;
    }
}
