package org.hillview.dataStructures;

import org.hillview.dataset.PostProcessedSketch;
import org.hillview.dataset.api.ISketch;
import org.hillview.dataset.api.Pair;
import org.hillview.security.SecureLaplace;
import org.hillview.sketches.results.Histogram;
import org.hillview.table.api.ITable;
import org.hillview.targets.DPWrapper;
import org.hillview.utils.*;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Postprocesses a histogram adding differentially-private noise.
 */
public class DPHistogram extends PostProcessedSketch<ITable, Histogram, Histogram> {
    private final IntervalDecomposition decomposition;
    private final double epsilon;
    private final boolean isCdf;
    private final SecureLaplace laplace;
    private final int columnIndex;

    public DPHistogram(ISketch<ITable, Histogram> sketch,
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
    public Histogram postProcess(@Nullable Histogram histogram) {
        HillviewLogger.instance.info("Adding histogram noise with", "epsilon={0}", this.epsilon);
        double scale = PrivacyUtils.computeNoiseScale(this.epsilon, decomposition);
        double baseVariance = PrivacyUtils.laplaceVariance(scale);
        Converters.checkNull(histogram);
        Histogram result = new Histogram(histogram.buckets.length, true);
        Converters.checkNull(result.confidence);

        Noise noise = new Noise();
        long totalIntervals = 0;
        long previous = 0;
        for (int i = 0; i < histogram.buckets.length; i++) {
            long nIntervals = this.noiseForBucket(i, scale, baseVariance, isCdf, noise, laplace, decomposition);
            long current;
            if (isCdf) {
                current = previous + histogram.buckets[i];
                previous = current;
            } else {
                current = histogram.buckets[i];
            }
            result.buckets[i] = Converters.toLong(current + noise.getNoise());
            if (isCdf && i > 0) {
                // Ensure they are monotonically increasing
                result.buckets[i] = Math.max(result.buckets[i-1], result.buckets[i]);
            }
            result.confidence[i] = Converters.toInt(
                    PrivacyUtils.laplaceCI(nIntervals, scale, PrivacyUtils.DEFAULT_ALPHA).second);
            totalIntervals += nIntervals;
        }
        noise = DPWrapper.computeCountNoise(this.columnIndex, DPWrapper.SpecialBucket.NullCount, epsilon, laplace);
        result.missingConfidence = Converters.toInt(
                PrivacyUtils.laplaceCI(1, 1.0/epsilon, PrivacyUtils.DEFAULT_ALPHA).second);
        HillviewLogger.instance.info("RNG calls", "{0}", totalIntervals);
        return result;
    }
}
