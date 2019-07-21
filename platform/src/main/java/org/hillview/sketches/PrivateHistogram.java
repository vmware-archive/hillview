package org.hillview.sketches;

import org.apache.commons.math3.distribution.LaplaceDistribution;

public class PrivateHistogram extends Histogram {
    private final DyadicHistogramBuckets dyadicBuckets;

    public PrivateHistogram(final IHistogramBuckets bucketDescription) {
        super(bucketDescription);
        dyadicBuckets = (DyadicHistogramBuckets)bucketDescription;
    }

    // Note that we ceil in order to round up to the nearest leaf
    public static int intLog2(int x) {
        assert (x > 0);
        return (int)(Math.ceil(Math.log(x) / Math.log(2)));
    }

    // The amount of noise depends on the number of "leaves" that are required to compute the interval.
    public int noiseMultiplier(int bucketIdx) {
        int nLeaves = dyadicBuckets.numLeavesInBucket(bucketIdx);
        if (nLeaves == 1) return 1;

        return intLog2(nLeaves);
    }

    /* Add Laplace noise compatible with the binary mechanism to each bucket.
     * This operation cannot be undone.
     * Note that the noise scale provided should be log T / epsilon where T is
     * the total number of leaves in the unfiltered histogram.
     * This is not computed in the function, because the range of the buckets might be
     * smaller than the full range T if the buckets are from a filtered histogram. */
    public void addDyadicLaplaceNoise(double scale) {
        LaplaceDistribution dist = new LaplaceDistribution(0, scale); // TODO: (more) secure PRG
        for (int i = 0; i < this.buckets.length; i++) {
            this.buckets[i] += dist.sample() * this.noiseMultiplier(i);
        }
    }

    public long getCount(final int index) { return this.buckets[index]; }
}
