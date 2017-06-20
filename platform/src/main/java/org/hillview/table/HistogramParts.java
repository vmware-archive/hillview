package org.hillview.table;

import org.hillview.sketches.BucketsDescriptionEqSize;
import org.hillview.sketches.Hist1DLightSketch;
import org.hillview.table.api.IStringConverter;

import javax.annotation.Nullable;

/**
 * Created by lsuresh on 6/20/17.
 */
public class HistogramParts {
    public HistogramParts(BucketsDescriptionEqSize buckets, @Nullable IStringConverter converter,
                   Hist1DLightSketch sketch) {
        this.buckets = buckets;
        this.converter = converter;
        this.sketch = sketch;
    }

    public final BucketsDescriptionEqSize buckets;
    @Nullable
    public final IStringConverter converter;
    public final Hist1DLightSketch sketch;
}
