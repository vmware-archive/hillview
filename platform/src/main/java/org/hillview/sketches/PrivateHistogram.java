package org.hillview.sketches;

import org.apache.commons.math3.distribution.LaplaceDistribution;
import org.hillview.dataset.api.IJson;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ISampledRowIterator;

import java.io.Serializable;

public class PrivateHistogram implements Serializable, IJson {
    public Histogram histogram; // Note that this histogram should have DyadicHistogramBuckets as its bucket description.
    private DyadicHistogramBuckets bucketDescription; // Just an alias for the buckets in the histogram.

    public PrivateHistogram(final Histogram histogram) {
        this.histogram = histogram;
        this.bucketDescription = (DyadicHistogramBuckets)histogram.getBucketDescription();
    }

    public static int intLog2(int x) {
        assert (x > 0);
        return (int)(Math.ceil(Math.log(x) / Math.log(2))); // Note that we ceil in order to round up to the nearest leaf
    }

    public int noiseMultiplier(int bucketIdx) {
        // The amount of noise depends on the number of "leaves" that are required to compute the interval.
        int nLeaves = this.bucketDescription.numLeavesInBucket(bucketIdx);
        if (nLeaves == 1) return 1;

        return intLog2(nLeaves);
    }

    /* Add Laplace noise compatible with the binary mechanism to each bucket.
     * Note that the noise scale provided should be log T / epsilon where T is
     * the total number of leaves in the unfiltered histogram.
     * This is not computed in the function, because the range of the buckets might be
     * smaller than the full range T if the buckets are from a filtered histogram. */
    public void addDyadicLaplaceNoise(double scale) {
        LaplaceDistribution dist = new LaplaceDistribution(0, scale); // TODO: (more) secure PRG
        for (int i = 0; i < this.histogram.buckets.length; i++) {
            this.histogram.buckets[i] += dist.sample() * this.noiseMultiplier(i);
            this.histogram.buckets[i] = Math.max(0, this.histogram.buckets[i]);
        }
    }
}
