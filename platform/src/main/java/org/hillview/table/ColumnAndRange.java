package org.hillview.table;

import org.hillview.sketches.BucketsDescriptionEqSize;
import org.hillview.sketches.HistogramSketch;
import org.hillview.table.api.IStringConverter;

import javax.annotation.Nullable;
import java.io.Serializable;

@SuppressWarnings("CanBeFinal")
public class ColumnAndRange implements Serializable, IJsonRepr{
    public String columnName = "";
    public double min;
    public double max;
    public int cdfBucketCount;
    public int bucketCount;
    @Nullable
    public String[] bucketBoundaries;  // only used for Categorical columns

    public HistogramParts prepare() {
        IStringConverter converter = null;
        if (this.bucketBoundaries != null)
            converter = new SortedStringsConverter(
                    this.bucketBoundaries, (int)Math.ceil(this.min), (int)Math.floor(this.max));
        BucketsDescriptionEqSize buckets = new BucketsDescriptionEqSize(this.min, this.max, this.bucketCount);
        HistogramSketch sketch = new HistogramSketch(buckets, this.columnName, converter);
        return new HistogramParts(buckets, converter, sketch);
    }

    public static class HistogramParts {
        public final BucketsDescriptionEqSize buckets;
        @Nullable
        public final IStringConverter converter;
        public final HistogramSketch sketch;

        public HistogramParts(BucketsDescriptionEqSize buckets, @Nullable IStringConverter converter,
                              HistogramSketch sketch) {
            this.buckets = buckets;
            this.converter = converter;
            this.sketch = sketch;
        }
    }
}