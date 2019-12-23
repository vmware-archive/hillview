package org.hillview.dataStructures;

import org.hillview.dataset.api.IJson;
import org.hillview.sketches.results.Histogram;

/**
 * Base class for data structure for histogram with additional metadata, e.g. the CDF.
 * The TypeScript interface AugmentedHistogram represents this class and its subclasses.
 */
public class AugmentedHistogram implements IJson {
    public Histogram histogram;
    public AugmentedHistogram(Histogram histogram) {
        this.histogram = histogram;
    }
}
