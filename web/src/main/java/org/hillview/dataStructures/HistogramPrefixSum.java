package org.hillview.dataStructures;

import org.hillview.sketches.results.Histogram;

/**
 * Integrates histogram buckets to compute CDF.
 */
public class HistogramPrefixSum extends AugmentedHistogram {
    protected long[] cdfBuckets;

    public HistogramPrefixSum(Histogram histogram) {
        super(histogram);
        this.cdfBuckets = new long[histogram.getBucketCount()];
        this.cdfBuckets[0] = this.histogram.buckets[0];
        for (int i = 1; i < this.histogram.buckets.length; i++) {
            this.cdfBuckets[i] = this.cdfBuckets[i-1] + this.histogram.buckets[i];
        }
    }
}
