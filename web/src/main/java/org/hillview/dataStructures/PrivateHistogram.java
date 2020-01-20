package org.hillview.dataStructures;

import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.Pair;
import org.hillview.security.SecureLaplace;
import org.hillview.sketches.results.Histogram;
import org.hillview.targets.DPWrapper;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Noise;
import org.hillview.utils.Utilities;
import sun.jvm.hotspot.utilities.Interval;

import java.util.List;

/**
 * Contains methods for adding privacy to a non-private histogram computed over dyadic buckets,
 * and for computing the CDF and confidence intervals on the buckets.
 */
@SuppressWarnings("MismatchedReadAndWriteOfArray")
public class PrivateHistogram extends HistogramPrefixSum implements IJson {
    private int[] confidence;
    // TODO(pratiksha): compute the missing value confidence
    private int missingConfidence;  // Confidence for the missing value
    private final double epsilon;

    public PrivateHistogram(IntervalDecomposition decomposition,
                            final Histogram histogram,
                            double epsilon, boolean isCdf, SecureLaplace laplace) {
        super(histogram);
        this.epsilon = epsilon;
        this.confidence  = new int[histogram.getBucketCount()];
        this.missingConfidence = 0;
        long numRngCalls = this.addDyadicLaplaceNoise(decomposition, laplace);
        HillviewLogger.instance.info("RNG calls", "{0}", numRngCalls);
        if (isCdf) {
            this.recomputeCDF(decomposition, laplace);
        }
    }

    /**
     * Compute noise for the given [left leaf, right leaf) range using the dyadic decomposition.
     * See also noiseForBucket.
     */
    public long noiseForRange(int left, int right,
                              double scale, double baseVariance,
                              SecureLaplace laplace,
                             /*out*/Noise noise) {
        List<Pair<Integer, Integer>> intervals = IntervalDecomposition.kadicDecomposition(left, right, IntervalDecomposition.BRANCHING_FACTOR);
        noise.clear();
        for (Pair<Integer, Integer> x : intervals) {
            noise.add(laplace.sampleLaplace(x, scale), baseVariance);
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

    /**
     * Adds noise to CDF and then recomputes.
     * Note that this is more complicated than simply integrating the noisy buckets,
     * since we would like to take advantage of the dyadic tree to add noise more efficiently.
     * This function uses the dyadic decomposition of each prefix to add the smallest amount of
     * noise for that prefix.
     */
    private void recomputeCDF(IntervalDecomposition decomposition, SecureLaplace laplace) {
        int totalLeaves = decomposition.getQuantizationIntervalCount();
        double scale = Utilities.logb(totalLeaves, IntervalDecomposition.BRANCHING_FACTOR);
        scale /= epsilon;
        double baseVariance = 2 * Math.pow(scale, 2);
        Noise noise = new Noise();
        for (int i = 0; i < this.cdfBuckets.length; i++) {
            noise.clear();
            this.noiseForBucket(i, scale, baseVariance, true, noise, laplace, decomposition);
            this.cdfBuckets[i] += noise.getNoise();
            if (i > 0) {
                // Postprocess CDF to be monotonically increasing
                this.cdfBuckets[i] = Math.max(this.cdfBuckets[i-1], this.cdfBuckets[i]);
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
    private long addDyadicLaplaceNoise(IntervalDecomposition decomposition, SecureLaplace laplace) {
        HillviewLogger.instance.info("Adding histogram noise with", "epsilon={0}", this.epsilon);
        int totalLeaves = decomposition.getQuantizationIntervalCount();
        double scale = Utilities.logb(totalLeaves, IntervalDecomposition.BRANCHING_FACTOR);
        scale /= epsilon;
        double baseVariance = 2 * Math.pow(scale, 2);

        Noise noise = new Noise();
        long totalIntervals = 0;
        for (int i = 0; i < this.histogram.buckets.length; i++) {
            long nIntervals = this.noiseForBucket(
                    i, scale, baseVariance, false, noise, laplace, decomposition);
            totalIntervals += nIntervals;
            this.histogram.buckets[i] += noise.getNoise();
            this.confidence[i] = Utilities.toInt(noise.getConfidence());
        }

        noise = DPWrapper.computeCountNoise(DPWrapper.SpecialBucket.NullCount, epsilon, laplace);
        missingConfidence = Utilities.toInt(noise.getConfidence());
        return totalIntervals;
    }

    public double getEpsilon() {
        return this.epsilon;
    }
}
