package org.hillview.dataStructures;

import org.hillview.sketches.highorder.PostProcessedSketch;
import org.hillview.dataset.api.ISketch;
import org.hillview.security.SecureLaplace;
import org.hillview.sketches.results.Count;
import org.hillview.table.api.ITable;
import org.hillview.utils.*;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static org.hillview.dataStructures.IntervalDecomposition.kadicDecomposition;

/**
 * Differentially-private heatmap.
 */
public class DPHeatmapSketch<IG extends IGroup<Count>, OG extends IGroup<IG>>
        extends PostProcessedSketch<ITable, OG, Two<JsonGroups<JsonGroups<Count>>>> {
    private final double epsilon;
    private final SecureLaplace laplace;

    final double scale;
    final double baseVariance;

    final IntervalDecomposition dx;
    final IntervalDecomposition dy;

    private final int columnsIndex;

    public DPHeatmapSketch(
            ISketch<ITable, OG> sketch,
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
     * Each node in the dyadic interval tree is perturbed by an independent noise variable
     * distributed as Laplace(log T / epsilon).
     * The total noise is the sum of the noise variables in the intervals composing the desired interval or bucket.
     * @return Two heatmaps: one is the actual heatmap with the noise added,
     * and the second one is the confidence interval for each bucket.
     */
    @Nullable
    @Override
    public Two<JsonGroups<JsonGroups<Count>>> postProcess(@Nullable OG heatmap) {
        Converters.checkNull(heatmap);
        int xSize = heatmap.size();
        int ySize = heatmap.getBucket(0).size();
        long[][] counts = new long[xSize][ySize];
        int[][] confidences = new int[xSize][ySize];

        HillviewLogger.instance.info("Adding heatmap noise with", "epsilon={0}", this.epsilon);
        List<List<Pair<Integer, Integer>>> xIntervals = new ArrayList<List<Pair<Integer, Integer>>>(xSize);
        List<List<Pair<Integer, Integer>>> yIntervals = new ArrayList<List<Pair<Integer, Integer>>>(ySize);
        for (int i = 0; i < xSize; i++)
            xIntervals.add(this.dx.bucketDecomposition(i, false));
        for (int i = 0; i < ySize; i++)
            yIntervals.add(this.dy.bucketDecomposition(i, false));

        // Compute the noise.
        Noise noise = new Noise();
        for (int i = 0; i < xSize; i++) {
            for (int j = 0; j < ySize; j++) {
                long nIntervals = this.noiseForDecomposition(xIntervals.get(i), yIntervals.get(j), this.scale, this.baseVariance, noise);
                counts[i][j] = Converters.toLong(heatmap.getBucket(i).getBucket(j).count + noise.getNoise());
                confidences[i][j] = Converters.toInt(
                        PrivacyUtils.laplaceCI(nIntervals, this.scale, PrivacyUtils.DEFAULT_ALPHA).second);
            }
        }

        // TODO: this does not add noise for the "missing" counts
        JsonGroups<JsonGroups<Count>> cts = JsonGroups.fromArray(counts);
        JsonGroups<JsonGroups<Count>> conf = JsonGroups.fromArray(confidences);
        return new Two<>(cts, conf);
    }
}
