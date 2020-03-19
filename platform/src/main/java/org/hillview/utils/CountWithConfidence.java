package org.hillview.utils;

import org.hillview.dataset.api.IJson;

/**
 * A count and confidence around the counted value.
 */
public class CountWithConfidence implements IJson {
    static final long serialVersionUID = 1;

    public final long count;
    public final long confidence;

    public CountWithConfidence(long count, long confidence) {
        this.count = count;
        this.confidence = confidence;
    }

    public CountWithConfidence(long count) {
        this(count, 0);
    }

    public CountWithConfidence add(Noise noise) {
        return new CountWithConfidence(
                this.count + Utilities.toLong(noise.getNoise()),
                this.confidence + Utilities.toLong(noise.get2Stdev())); 
                /* TODO: this is not a real CI. Not sure how this is being used currently */
    }
}
