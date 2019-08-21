package org.hillview.table.rows;

import org.hillview.dataset.api.IJson;

/**
 * This class represents metadata used for computing differentially-private mechanisms.
 */
public class PrivacyMetadata implements IJson {
    /**
     * Total privacy budget allotted to this column.
     */
    public double epsilon;

    /**
     * Minimum quantization interval: users will only be able to
     * query ranges that are a multiple of this size.
     * This field is particularly useful for implementing the dyadic interval tree
     * in the binary mechanism of Chan, Song, Shi '11 (https://eprint.iacr.org/2010/076.pdf).
     */
    public double granularity;

    /**
     * Fixed global minimum value for the column. Should be computable from
     * public information or otherwise uncorrelated with the data.
     */
    public double globalMin;

    /**
     * Fixed global maximum value. Should be computable from
     * public information or otherwise uncorrelated with the data.
     */
    public double globalMax;

    public PrivacyMetadata(double epsilon, double granularity,
                           double globalMin, double globalMax) {
        this.epsilon = epsilon;
        this.granularity = granularity;
        this.globalMin = globalMin;
        this.globalMax = globalMax;
    }
}
