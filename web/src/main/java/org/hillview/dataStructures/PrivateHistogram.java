package org.hillview.dataStructures;

import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.Pair;
import org.hillview.sketches.DyadicHistogramBuckets;
import org.hillview.sketches.Histogram;
import org.hillview.utils.Converters;

import java.io.Serializable;

/**
 * Contains methods for adding privacy to a non-private histogram computed over dyadic buckets,
 * and for computing the CDF and confidence intervals on the buckets.
 */
@SuppressWarnings("MismatchedReadAndWriteOfArray")
public class PrivateHistogram extends HistogramPrefixSum implements IJson {
    private DyadicHistogramBuckets bucketDescription; // Just an alias for the buckets in the histogram.

    private double[] confMins;
    private double[] confMaxes;

    public PrivateHistogram(final Histogram histogram, boolean cdf) {
        super(histogram);
        this.bucketDescription = (DyadicHistogramBuckets)histogram.getBucketDescription();
        this.confMins  = new double[this.bucketDescription.getNumOfBuckets()];
        this.confMaxes = new double[this.bucketDescription.getNumOfBuckets()];
        this.addDyadicLaplaceNoise();
        if (cdf) {
            this.recomputeCDF();
        }
    }

    /* Adds noise to CDF and then recomputes.
     * Note that this is more complicated than simply integrating the noisy buckets,
     * since we would like to take advantage of the dyadic tree to add noise more efficiently.
     * This function uses the dyadic decomposition of each prefix to add the smallest amount of
     * noise for that prefix. */
    private void recomputeCDF() {
        for (int i = 0; i < this.cdfBuckets.length; i++) {
            Pair<Double, Double> p = this.bucketDescription.noiseForBucket(i, true);
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
    private void addDyadicLaplaceNoise() {
        for (int i = 0; i < this.histogram.buckets.length; i++) {
            Pair<Double, Double> noise = this.bucketDescription.noiseForBucket(i, false);
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
