package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.results.IHistogramBuckets;
import org.hillview.sketches.results.NumericSamples;
import org.hillview.sketches.results.QuantilesVector;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.List;

/**
 * This sketch is given two columns: one that divides data into buckets, and one where
 * we compute a set of quantiles over each bucket.  For each bucket is computes a set of
 * approximate quantiles with the corresponding sampling rate.
 */
public class QuantilesVectorSketch implements ISketch<ITable, QuantilesVector> {
    private final IHistogramBuckets buckets;
    private final String bucketsColumn;
    private final String column;
    private final double[] samplingRates;
    private final long seed;

    public QuantilesVectorSketch(
            IHistogramBuckets buckets, String bucketsColumn, String column, double[] samplingRates, long seed) {
        this.buckets = buckets;
        this.bucketsColumn = bucketsColumn;
        this.column = column;
        this.samplingRates = samplingRates;
        this.seed = seed;
    }

    @Nullable
    @Override
    public QuantilesVector create(@Nullable ITable data) {
        if (buckets.getBucketCount() != this.samplingRates.length)
            throw new RuntimeException("Bucket count not the same as the sampling rates");
        List<IColumn> cols = Converters.checkNull(data).getLoadedColumns(this.bucketsColumn, this.column);
        IColumn bucketCol = cols.get(0);
        IColumn sampledCol = cols.get(1);
        IRowIterator it = data.getRowIterator();

        QuantilesVector result = Converters.checkNull(this.zero());
        int current = it.getNextRow();
        while (current >= 0) {
            int bucket = this.buckets.indexOf(bucketCol, current);
            if (bucket < 0)
                result.outOfBounds();
            else if (sampledCol.isMissing(current))
                result.addMissing(bucket);
            else
                result.add(bucket, sampledCol.asDouble(current));
            current = it.getNextRow();
        }
        result.seal();
        return result;
    }

    @Nullable
    @Override
    public QuantilesVector zero() {
        NumericSamples[] result = new NumericSamples[this.samplingRates.length];
        for (int i = 0; i < this.samplingRates.length; i++) {
            result[i] = new NumericSamples(this.samplingRates[i], this.seed + i);
        }
        return new QuantilesVector(result, 0);
    }

    @Nullable
    @Override
    public QuantilesVector add(@Nullable QuantilesVector left, @Nullable QuantilesVector right) {
        return Converters.checkNull(left).add(Converters.checkNull(right));
    }
}
