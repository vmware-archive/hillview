package org.hillview.table;

import org.hillview.sketches.BucketsDescriptionEqSize;
import org.hillview.sketches.Hist1DLightSketch;
import org.hillview.table.api.IStringConverter;

import javax.annotation.Nullable;
import java.io.Serializable;


public class ColumnAndRange implements Serializable {
    public String columnName = "";
    public double min;
    public double max;
    public int cdfBucketCount;
    public  int bucketCount;  // only used for histogram
    @Nullable
    public String[] bucketBoundaries;  // only used for Categorical columns histograms

    public HistogramParts prepare() {
        IStringConverter converter = null;
        if (this.bucketBoundaries != null)
            converter = new SortedStringsConverter(
                    this.bucketBoundaries, (int)Math.ceil(this.min), (int)Math.floor(this.max));
        BucketsDescriptionEqSize buckets = new BucketsDescriptionEqSize(this.min, this.max, this.bucketCount);
        Hist1DLightSketch sketch = new Hist1DLightSketch(buckets, this.columnName, converter);
        return new HistogramParts(buckets, converter, sketch);
    }
}