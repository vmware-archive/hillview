package org.hillview.dataStructures;

import org.hillview.sketches.highorder.PostProcessedSketch;
import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.results.Count;
import org.hillview.security.SecureLaplace;
import org.hillview.table.api.ITable;
import org.hillview.targets.DPWrapper;
import org.hillview.utils.*;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Postprocesses a histogram adding differentially-private noise.
 */
public class DPHistogram<G extends IGroup<Count>> extends
        PostProcessedSketch<ITable, G, Two<JsonGroups<Count>>> {
    private final IntervalDecomposition decomposition;
    private final double epsilon;
    private final boolean isCdf;
    private final SecureLaplace laplace;
    private final int columnIndex;

    public DPHistogram(ISketch<ITable, G> sketch,
                       int columnIndex,
                       IntervalDecomposition decomposition,
                       double epsilon, boolean isCdf, SecureLaplace laplace) {
        super(sketch);
        this.columnIndex = columnIndex;
        this.decomposition = decomposition;
        this.epsilon = epsilon;
        this.isCdf = isCdf;
        this.laplace = laplace;
    }

    /**
     * Compute noise for the given [left leaf, right leaf) range using the dyadic decomposition.
     * See also noiseForBucket.
     */
    public long noiseForRange(int left, int right,
                              double scale, double baseVariance,
                              SecureLaplace laplace,
                             /*out*/Noise noise) {
        List<Pair<Integer, Integer>> intervals =
                IntervalDecomposition.kadicDecomposition(left, right, IntervalDecomposition.BRANCHING_FACTOR);
        noise.clear();
        for (Pair<Integer, Integer> x : intervals) {
            noise.add(laplace.sampleLaplace(this.columnIndex, scale, x), baseVariance);
        }
        return intervals.size();
    }

    /**
     * Compute noise to add to this bucket using the dyadic decomposition as the PRG seed.
     * @param bucketIdx: index of the bucket to compute noise for.
     * @param scale:      scale of laplace distribution used to sample data
     * @param laplace:    laplace distribution to sample
     * @param baseVariance:  factor added to variance for each bucket
     * @param isCdf: If true, computes the noise based on the dyadic decomposition of the interval [0, bucket right leaf]
     *             rather than [bucket left leaf, bucket right leaf].
     * @param decomposition: Specifies the dyadic decomposition to use when computing the nodes for this bucket.
     * Returns the noise and the total variance of the variables used to compute the noise.
     */
    @SuppressWarnings("ConstantConditions")
    long noiseForBucket(int bucketIdx,
                        double scale, double baseVariance,
                        boolean isCdf, Noise noise,
                        SecureLaplace laplace,
                        IntervalDecomposition decomposition) {
        Pair<Integer, Integer> range = decomposition.bucketRange(bucketIdx, isCdf);
        return this.noiseForRange(range.first, range.second, scale, baseVariance, laplace, noise);
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
    public Two<JsonGroups<Count>> postProcess(@Nullable G histogram) {
        HillviewLogger.instance.info("Adding histogram noise with", "epsilon={0}", this.epsilon);
        double scale = PrivacyUtils.computeNoiseScale(this.epsilon, decomposition);
        double baseVariance = PrivacyUtils.laplaceVariance(scale);
        Converters.checkNull(histogram);
        long[] counts = new long[histogram.size()];
        int[]  conf = new int[histogram.size()];

        Noise noise = new Noise();
        long totalIntervals = 0;
        long previous = 0;
        for (int i = 0; i < histogram.size(); i++) {
            long nIntervals = this.noiseForBucket(i, scale, baseVariance, isCdf, noise, laplace, decomposition);
            long current;
            if (isCdf) {
                current = previous + histogram.getBucket(i).count;
                previous = current;
            } else {
                current = histogram.getBucket(i).count;
            }
            counts[i] = Converters.toLong(current + noise.getNoise());
            if (isCdf && i > 0) {
                // Ensure they are monotonically increasing
                counts[i] = Math.max(counts[i-1], counts[i]);
            }
            conf[i] = Converters.toInt(
                    PrivacyUtils.laplaceCI(nIntervals, scale, PrivacyUtils.DEFAULT_ALPHA).second);
            totalIntervals += nIntervals;
        }
        noise = DPWrapper.computeCountNoise(this.columnIndex, DPWrapper.SpecialBucket.NullCount, epsilon, laplace);
        int missingConfidence = Converters.toInt(
                PrivacyUtils.laplaceCI(1, 1.0/epsilon, PrivacyUtils.DEFAULT_ALPHA).second);
        HillviewLogger.instance.info("RNG calls", "{0}", totalIntervals);
        JsonGroups<Count> result = JsonGroups.fromArray(counts, histogram.getMissing().count);
        JsonGroups<Count> confidences = JsonGroups.fromArray(conf, missingConfidence);
        return new Two<>(result, confidences);
    }
}
