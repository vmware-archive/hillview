package org.hillview.sketches;

import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.hillview.dataset.api.IJson;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ISampledRowIterator;
import org.hillview.table.rows.PrivacyMetadata;

import java.io.Serializable;

public class PrivateHistogram implements Serializable, IJson {
    public Histogram histogram; // Note that this histogram should have DyadicHistogramBuckets as its bucket description.
    private DyadicHistogramBuckets bucketDescription; // Just an alias for the buckets in the histogram.

    // Used for computing the total noise.
    private PrivacyMetadata metadata;

    public PrivateHistogram(final Histogram histogram, PrivacyMetadata metadata) {
        this.histogram = histogram;
        this.bucketDescription = (DyadicHistogramBuckets)histogram.getBucketDescription();

        this.metadata = metadata;
    }

    /**
     * Add Laplace noise compatible with the binary mechanism to each bucket.
     * Noise is added as follows:
     * Let T := (globalMax - globalMin) / granularity, the total number of leaves in the data overall.
     * Each node in the dyadic interval tree is perturbed by an independent noise variable distributed as Laplace(log T / epsilon).
     * The total noise is the sum of the noise variables in the intervals composing the desired interval or bucket.
     */
    public void addDyadicLaplaceNoise() {
        double leaves = (metadata.globalMax - metadata.globalMin) / metadata.granularity;
        double scale = (Math.log(leaves) / (metadata.epsilon * Math.log(2)));

        LaplaceDistribution dist = new LaplaceDistribution(0, scale); // TODO: (more) secure PRG
        System.out.println("Adding noise with scale: " + scale);
        System.out.println("Buckets: " + this.histogram.buckets.length);
        for (int i = 0; i < this.histogram.buckets.length; i++) {
            this.histogram.buckets[i] += this.bucketDescription.noiseForBucket(i);
            System.out.println("Bucket " + i + ": " + this.histogram.buckets[i]);
            this.histogram.buckets[i] = Math.max(0, this.histogram.buckets[i]);
        }
    }
}
