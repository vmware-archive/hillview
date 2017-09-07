package org.hillview.table;

import org.hillview.sketches.BucketsDescriptionEqSize;
import org.hillview.sketches.HistogramSketch;
import org.hillview.table.api.ColumnNameAndConverter;
import org.hillview.table.api.IStringConverter;

import javax.annotation.Nullable;
import java.io.Serializable;

@SuppressWarnings("CanBeFinal")
public class ColumnAndRange implements Serializable, IJsonRepr {
    public String columnName = "";
    public double min;
    public double max;
    public int cdfBucketCount;
    public int bucketCount;
    public double samplingRate;
    @Nullable
    public String[] bucketBoundaries;  // only used for Categorical columns

    public HistogramParts prepare() {
        IStringConverter converter = null;
        if (this.bucketBoundaries != null)
            converter = new SortedStringsConverter(
                    this.bucketBoundaries, (int)Math.ceil(this.min), (int)Math.floor(this.max));
        BucketsDescriptionEqSize buckets = new BucketsDescriptionEqSize(this.min, this.max, this.bucketCount);
        ColumnNameAndConverter column = new ColumnNameAndConverter(this.columnName, converter);
        HistogramSketch sketch = new HistogramSketch(buckets, column, this.samplingRate);
        return new HistogramParts(buckets, column, sketch);
    }

    public static class HistogramParts {
        public final BucketsDescriptionEqSize buckets;
        public final ColumnNameAndConverter column;
        public final HistogramSketch sketch;

        public HistogramParts(BucketsDescriptionEqSize buckets,
                              ColumnNameAndConverter column,
                              HistogramSketch sketch) {
            this.buckets = buckets;
            this.column = column;
            this.sketch = sketch;
        }
    }
}