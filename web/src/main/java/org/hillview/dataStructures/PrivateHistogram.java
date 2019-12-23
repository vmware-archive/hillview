package org.hillview.dataStructures;

import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.Pair;
import org.hillview.security.SecureLaplace;
import org.hillview.sketches.results.Histogram;
import org.hillview.utils.HillviewLogger;

import java.util.List;

/**
 * Contains methods for adding privacy to a non-private histogram computed over dyadic buckets,
 * and for computing the CDF and confidence intervals on the buckets.
 */
@SuppressWarnings("MismatchedReadAndWriteOfArray")
public class PrivateHistogram extends HistogramPrefixSum implements IJson {
    private int[] confidence;
    private int missingConfidence;  // Confidence for the missing value
    // TODO(pratiksha): compute the missing value confidence
    private final double epsilon;
    private SecureLaplace laplace;

    public PrivateHistogram(IntervalDecomposition decomposition,
                            final Histogram histogram,
                            double epsilon, boolean isCdf,
                            SecureLaplace laplace) {
        super(histogram);
        this.laplace = laplace;
        this.epsilon = epsilon;
        this.confidence  = new int[histogram.getBucketCount()];
        long numRngCalls = this.addDyadicLaplaceNoise(decomposition);
        HillviewLogger.instance.info("RNG calls", "{0}", numRngCalls);
        if (isCdf) {
            this.recomputeCDF(decomposition);
        }
    }

    /**
     * Replace the Laplace distribution. For use in testing.
     *
     * @param laplace A new Laplace distribution to use in sampling noise.
     */
    public void setLaplace(SecureLaplace laplace) {
        this.laplace = laplace;
    }

    /**
     * Compute noise for the given [left leaf, right leaf) range using the dyadic decomposition.
     * See also noiseForBucket.
     */
    public long noiseForRange(int left, int right,
                              double scale, double baseVariance,
                             /*out*/Noise noise) {
        List<Pair<Integer, Integer>> intervals = IntervalDecomposition.kadicDecomposition(left, right, 2);
        noise.clear();
        for (Pair<Integer, Integer> x : intervals) {
            noise.noise += this.laplace.sampleLaplace(x, scale);
            noise.variance += baseVariance;
        }

        return intervals.size();
    }

    /**
     * Compute noise to add to this bucket using the dyadic decomposition as the PRG seed.
     * @param bucketIdx: index of the bucket to compute noise for.
     * @param scale:      scale of laplace distribution used to sample data
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
                        IntervalDecomposition decomposition) {
        Pair<Integer, Integer> range = decomposition.bucketRange(bucketIdx, isCdf);
        return this.noiseForRange(range.first, range.second, scale, baseVariance, noise);
    }

    /**
     * Adds noise to CDF and then recomputes.
     * Note that this is more complicated than simply integrating the noisy buckets,
     * since we would like to take advantage of the dyadic tree to add noise more efficiently.
     * This function uses the dyadic decomposition of each prefix to add the smallest amount of
     * noise for that prefix.
     */
    private void recomputeCDF(IntervalDecomposition decomposition) {
        int totalLeaves = decomposition.getQuantizationIntervalCount();
        double scale = Math.log(totalLeaves + 1) / Math.log(2);  // +1 leaf for NULL
        scale /= epsilon;
        double baseVariance = 2 * Math.pow(scale, 2);
        Noise noise = new Noise();
        for (int i = 0; i < this.cdfBuckets.length; i++) {
            noise.clear();
            this.noiseForBucket(
                    i, scale, baseVariance, true, noise, decomposition);
            this.cdfBuckets[i] += noise.noise;
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
    private long addDyadicLaplaceNoise(IntervalDecomposition decomposition) {
        HillviewLogger.instance.info("Adding histogram noise with", "epsilon={0}", this.epsilon);
        int totalLeaves = decomposition.getQuantizationIntervalCount();
        double scale = Math.log(totalLeaves + 1) / Math.log(2);  // +1 for NULL leaf
        scale /= epsilon;
        double baseVariance = 2 * Math.pow(scale, 2);

        Noise noise = new Noise();
        long totalIntervals = 0;
        for (int i = 0; i < this.histogram.buckets.length; i++) {
            long nIntervals = this.noiseForBucket(
                    i, scale, baseVariance, false, noise, decomposition);
            totalIntervals += nIntervals;
            this.histogram.buckets[i] += noise.noise;
            this.confidence[i] = (int)noise.getConfidence();
        }
        return totalIntervals;
    }

    public double getEpsilon() {
        return this.epsilon;
    }
}
