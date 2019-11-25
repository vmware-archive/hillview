package org.hillview.dataStructures;

import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.hillview.dataset.api.IJson;
import org.hillview.sketches.results.Histogram;
import org.hillview.utils.HillviewLogger;

/**
 * Contains methods for adding privacy to a non-private histogram computed over dyadic buckets,
 * and for computing the CDF and confidence intervals on the buckets.
 */
@SuppressWarnings("MismatchedReadAndWriteOfArray")
public class PrivateHistogram extends HistogramPrefixSum implements IJson {
    private int[] confidence;
    private final double epsilon;

    public PrivateHistogram(DyadicDecomposition decomposition,
                            final Histogram histogram,
                            double epsilon, boolean isCdf) {
        super(histogram);
        this.epsilon = epsilon;
        this.confidence  = new int[histogram.getBucketCount()];
        long numRngCalls = this.addDyadicLaplaceNoise(decomposition);
        System.out.println("Made " + numRngCalls + " RNG calls");
        if (isCdf) {
            this.recomputeCDF(decomposition);
        }
    }

    /**
     * Adds noise to CDF and then recomputes.
     * Note that this is more complicated than simply integrating the noisy buckets,
     * since we would like to take advantage of the dyadic tree to add noise more efficiently.
     * This function uses the dyadic decomposition of each prefix to add the smallest amount of
     * noise for that prefix.
     */
    private void recomputeCDF(DyadicDecomposition decomposition) {
        int totalLeaves = decomposition.getQuantizationIntervalCount();
        double scale = Math.log(totalLeaves + 1) / Math.log(2);  // +1 leaf for NULL
        scale /= epsilon;
        double baseVariance = 2 * Math.pow(scale, 2);
        LaplaceDistribution dist = new LaplaceDistribution(0, scale); // TODO: (more) secure PRG

        Noise noise = new Noise();
        for (int i = 0; i < this.cdfBuckets.length; i++) {
            noise.clear();
            decomposition.noiseForBucket(
                    i, this.epsilon, scale, baseVariance, true, noise);
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
    private long addDyadicLaplaceNoise(DyadicDecomposition decomposition) {
        HillviewLogger.instance.info("Adding histogram noise with", "epsilon={0}", this.epsilon);
        int totalLeaves = decomposition.getQuantizationIntervalCount();
        double scale = Math.log(totalLeaves + 1) / Math.log(2);  // +1 for NULL leaf
        scale /= epsilon;
        double baseVariance = 2 * Math.pow(scale, 2);

        Noise noise = new Noise();
        long totalIntervals = 0;
        for (int i = 0; i < this.histogram.buckets.length; i++) {
            long nIntervals = decomposition.noiseForBucket(
                    i, this.epsilon, scale, baseVariance, false, noise);
            totalIntervals += nIntervals;
            this.histogram.buckets[i] += noise.noise;
            this.confidence[i] = (int)noise.getConfidence();
        }
        return totalIntervals;
    }
}
