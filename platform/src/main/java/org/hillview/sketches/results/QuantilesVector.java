package org.hillview.sketches.results;

import org.hillview.dataset.api.IJson;

/**
 * A quantiles vector is a vector - with one element for each bucket of a
 * histogram - of quantiles.
 */
public class QuantilesVector implements IJson {
    static final long serialVersionUID = 1;

    public static class ColumnQuantiles implements IJson {
        static final long serialVersionUID = 1;
    }    
}