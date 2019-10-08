package org.hillview.dataStructures;

import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.Pair;
import org.hillview.sketches.results.DyadicDecomposition;
import org.hillview.sketches.results.Histogram;
import org.hillview.utils.Converters;

import java.util.ArrayList;

/**
 * Contains methods for adding privacy to a non-private histogram computed over dyadic buckets,
 * and for computing the CDF and confidence intervals on the buckets.
 */
@SuppressWarnings("MismatchedReadAndWriteOfArray")
public class PrivateHistogram extends HistogramPrefixSum implements IJson {
    private double[] confMins;
    private double[] confMaxes;
    private double epsilon;

    public PrivateHistogram(DyadicDecomposition bucketDescription,
                            final Histogram histogram, double epsilon, boolean cdf) {
        super(histogram);
        this.epsilon = epsilon;
        this.confMins  = new double[histogram.getBucketCount()];
        this.confMaxes = new double[histogram.getBucketCount()];
        this.addDyadicLaplaceNoise(bucketDescription);
        if (cdf) {
            this.recomputeCDF(bucketDescription);
        }
    }

    /**
     * Compute noise to add to this bucket using the dyadic decomposition as the PRG seed.
     * @param bucketIdx: index of the bucket to compute noise for.
     * @param cdf: If true, computes the noise based on the dyadic decomposition of the interval [0, bucket right leaf]
     *             rather than [bucket left leaf, bucket right leaf].
     * Returns the noise and the total variance of the variables used to compute the noise.
     */
    private Pair<Double, Double> noiseForBucket(DyadicDecomposition bucketDescription, int bucketIdx, boolean cdf) {
        ArrayList<Pair<Integer, Integer>> intervals = bucketDescription.bucketDecomposition(bucketIdx, cdf);

        double noise = 0;
        double variance = 0;
        int totalLeaves = bucketDescription.getGlobalNumLeaves();
        double scale = Math.log(totalLeaves / this.epsilon) / Math.log(2);
        for (Pair<Integer, Integer> x : intervals) {
            LaplaceDistribution dist = new LaplaceDistribution(0, scale); // TODO: (more) secure PRG
            dist.reseedRandomGenerator(x.hashCode()); // Each node's noise should be deterministic, based on node's ID
            noise += dist.sample();
            variance += 2*(Math.pow(scale, 2));
        }
        return new Pair<Double, Double>(noise, variance);
    }


    /**
     * Adds noise to CDF and then recomputes.
     * Note that this is more complicated than simply integrating the noisy buckets,
     * since we would like to take advantage of the dyadic tree to add noise more efficiently.
     * This function uses the dyadic decomposition of each prefix to add the smallest amount of
     * noise for that prefix.
     */
    private void recomputeCDF(DyadicDecomposition bucketDescription) {
        for (int i = 0; i < this.cdfBuckets.length; i++) {
            Pair<Double, Double> p = this.noiseForBucket(bucketDescription, i, true);
            Converters.checkNull(p.first);
            this.cdfBuckets[i] += p.first;
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
    private void addDyadicLaplaceNoise(DyadicDecomposition bucketDescription) {
        for (int i = 0; i < this.histogram.buckets.length; i++) {
            Pair<Double, Double> noise = this.noiseForBucket(bucketDescription, i, false);
            this.histogram.buckets[i] += Converters.checkNull(noise.first);

            // Postprocess so that no buckets are negative
            this.histogram.buckets[i] = Math.max(0, this.histogram.buckets[i]);

            // also set confidence intervals for this noise level
            Converters.checkNull(noise.second);
            this.confMins[i] = 2 * Math.sqrt(noise.second);
            this.confMaxes[i] = 2 * Math.sqrt(noise.second);
        }
    }
}
