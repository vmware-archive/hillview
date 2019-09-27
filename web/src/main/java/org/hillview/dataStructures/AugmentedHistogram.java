package org.hillview.dataStructures;

import org.hillview.sketches.Histogram;

/**
 * Base class for data structure for histogram with additional metadata, e.g. the CDF.
 */
public class AugmentedHistogram {
    protected Histogram histogram;

    public AugmentedHistogram(Histogram histogram) {
        this.histogram = histogram;
    }
}
