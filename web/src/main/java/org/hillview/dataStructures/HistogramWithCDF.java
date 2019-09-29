package org.hillview.dataStructures;

import org.hillview.sketches.Histogram;

/**
 * Integrates histogram buckets to compute CDF.
 */
public class HistogramWithCDF extends AugmentedHistogram {
    protected long[] cdfBuckets = new long[histogram.bucketDescription.getNumOfBuckets()];

    private void recomputeCDF() {
        this.cdfBuckets[0] = this.histogram.buckets[0];
        for (int i = 1; i < this.histogram.buckets.length; i++) {
            this.cdfBuckets[i] = this.cdfBuckets[i-1] + this.histogram.buckets[i];
        }
    }

    public HistogramWithCDF(Histogram histogram) {
        super(histogram);
        this.recomputeCDF();
    }
}
