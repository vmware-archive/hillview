package org.hillview.sketches;

import org.hillview.dataset.api.IJson;
import org.hillview.dataset.api.Pair;

import java.io.Serializable;

public class PrivateHistogram implements Serializable, IJson {
    public Histogram histogram; // Note that this histogram should have DyadicHistogramBuckets as its bucket description.
    private DyadicHistogramBuckets bucketDescription; // Just an alias for the buckets in the histogram.

    public PrivateHistogram(final Histogram histogram) {
        this.histogram = histogram;
        this.bucketDescription = (DyadicHistogramBuckets)histogram.getBucketDescription();
    }

    /**
     * Add Laplace noise compatible with the binary mechanism to each bucket.
     * Noise is added as follows:
     * Let T := (globalMax - globalMin) / granularity, the total number of leaves in the data overall.
     * Each node in the dyadic interval tree is perturbed by an independent noise variable distributed as Laplace(log T / epsilon).
     * The total noise is the sum of the noise variables in the intervals composing the desired interval or bucket.
     */
    public void addDyadicLaplaceNoise() {
        System.out.println("Buckets: " + this.histogram.buckets.length);
        for (int i = 0; i < this.histogram.buckets.length; i++) {
            Pair<Double, Double> noise = this.bucketDescription.noiseForBucket(i, false);
            this.histogram.buckets[i] += noise.first;
            System.out.println("Bucket " + i + ": " + this.histogram.buckets[i]);
            // Postprocess so that no buckets are negative
            this.histogram.buckets[i] = Math.max(0, this.histogram.buckets[i]);

            // also set confidence intervals for this noise level
            this.histogram.confMins[i] = 2*Math.sqrt(noise.second);
            this.histogram.confMaxes[i] = 2*Math.sqrt(noise.second);
        }

        for (int i = 0; i < this.histogram.cdfBuckets.length; i++) {
            this.histogram.cdfBuckets[i] += this.bucketDescription.noiseForBucket(i, true).first;
            System.out.println("Bucket " + i + ": " + this.histogram.cdfBuckets[i]);
            if (i > 0) {
                // Postprocess CDF to be monotonically increasing
                this.histogram.cdfBuckets[i] = Math.max(this.histogram.cdfBuckets[i-1], this.histogram.cdfBuckets[i]);
            }
        }
    }
}
