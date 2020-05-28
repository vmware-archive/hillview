package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.results.IHistogramBuckets;
import org.hillview.sketches.results.QuantilesMatrix;
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
public class QuantilesMatrixSketch implements ISketch<ITable, QuantilesMatrix> {
    private final IHistogramBuckets xBuckets;
    private final IHistogramBuckets gBuckets;

    private final String bucketsColumn;
    private final String groupByColumn;
    private final String column;
    private final double[][] samplingRates;
    private final long seed;

    public QuantilesMatrixSketch(
            IHistogramBuckets xBuckets, String bucketsColumn,
            IHistogramBuckets gBuckets, String groupByColumn,
            String column, double[][] samplingRates, long seed) {
        this.xBuckets = xBuckets;
        this.gBuckets = gBuckets;
        this.bucketsColumn = bucketsColumn;
        this.groupByColumn = groupByColumn;
        this.column = column;
        this.samplingRates = samplingRates;
        this.seed = seed;
    }

    @Nullable
    @Override
    public QuantilesMatrix create(@Nullable ITable data) {
        if (xBuckets.getBucketCount() != this.samplingRates.length ||
            gBuckets.getBucketCount() != this.samplingRates[0].length)
            throw new RuntimeException("Bucket count not the same as the sampling rates");
        List<IColumn> cols = Converters.checkNull(data).getLoadedColumns(
                this.bucketsColumn, this.groupByColumn, this.column);
        IColumn xBucketCol = cols.get(0);
        IColumn gBucketCol = cols.get(0);
        IColumn sampledCol = cols.get(2);
        IRowIterator it = data.getRowIterator();

        QuantilesMatrix result = Converters.checkNull(this.zero());
        int current = it.getNextRow();
        while (current >= 0) {
            int xBucket = this.xBuckets.indexOf(xBucketCol, current);
            int gBucket = this.gBuckets.indexOf(gBucketCol, current);
            if (xBucket < 0 || gBucket < 0)
                result.outOfBounds();
            else if (sampledCol.isMissing(current))
                result.addMissing(xBucket, gBucket);
            else
                result.add(xBucket, gBucket, sampledCol.asDouble(current));
            current = it.getNextRow();
        }
        result.seal();
        return result;
    }

    @Nullable
    @Override
    public QuantilesMatrix zero() {
        return QuantilesMatrix.zero(this.samplingRates, this.seed);
    }

    @Nullable
    @Override
    public QuantilesMatrix add(@Nullable QuantilesMatrix left, @Nullable QuantilesMatrix right) {
        return Converters.checkNull(left).add(Converters.checkNull(right));
    }
}
