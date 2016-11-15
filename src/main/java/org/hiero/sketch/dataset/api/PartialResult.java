package org.hiero.sketch.dataset.api;

import java.io.Serializable;

/**
 * A partial result always describes a DELTA between the previous partial result
 * and the current one.
 * @param <T>  Type of data in partial result.
 */
public class PartialResult<T> implements Serializable {
    /**
     * How much more has been deltaDone from the computation has been deltaDone
     * since the previous partial result.  A number between 0 and 1.  Even if this
     * is 1, it does not necessarily mean that the work is finished.
     */
    public final double deltaDone;
    /**
     * Additional data computed since the previous partial result.
     */
    public final T deltaValue;

    /**
     * Creates a partial result.
     * @param deltaDone  How much more has been done.  A number between 0 and 1.
     * @param deltaValue Extra result produced.
     */
    public PartialResult(double deltaDone, final T deltaValue) {
        if (deltaDone < 0) {
            throw new RuntimeException("Illegal value for deltaDone");
        } else if (deltaDone > 1) {
            // This can happen due to double addition imprecision.
            deltaDone = 1.0;
        }
        this.deltaDone = deltaDone;
        this.deltaValue = deltaValue;
    }

    @Override
    public String toString() {
        return "PR[" + Double.toString(this.deltaDone) + "," + this.deltaValue + "]";
    }
}
