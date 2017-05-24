package org.hiero.sketches;

import org.hiero.dataset.api.ISketch;
import org.hiero.table.ArrayRowOrder;
import org.hiero.table.RecordOrder;
import org.hiero.table.SmallTable;
import org.hiero.table.api.IColumn;
import org.hiero.table.api.IMembershipSet;
import org.hiero.table.api.IRowOrder;
import org.hiero.table.api.ITable;
import org.hiero.utils.Converters;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static org.hiero.table.ObjectArrayColumn.mergeColumns;

public class SampleQuantileSketch  implements ISketch<ITable, SampleList> {
    private final RecordOrder colSortOrder;
    /**
     * The rate at which we sample the data. It is set to be 5*(resolution)^2/dataSize,
     * so we expect a table of size 5*(resolution)^2. This is chosen so that the desired accuracy in
     * the quantiles is 1/resolution. It can be tuned by changing the size by a constant factor.
     * For a resolution of 100, the expected sample size is 50,000.
     */
    private final double samplingRate;
    /**
     * @param sortOrder The sorting order on the columns.
     * @param resolution Number of buckets: percentiles correspond to 100 buckets etc.
     * @param dataSize The size of the input table on which we want to run the quantile computation.
     */
    public SampleQuantileSketch(final RecordOrder sortOrder, final int resolution,
                                final long dataSize) {
        this.colSortOrder = sortOrder;
        int n = Math.max(resolution, 100);
        this.samplingRate = (5.0*resolution*resolution)/dataSize;
    }

    @Nullable
    @Override
    public SampleList zero() {
        return new SampleList(new SmallTable(this.colSortOrder.toSchema()));
    }

    /**
     * Creates a table by sampling with probability samplingRate and then sorting by sortOrder.
     * @param data  Data to sketch.
     * @return A table with samples sorted, and columns compressed to the relevant ones.
     */
    @Override
    public SampleList create(ITable data) {
        final IMembershipSet sampleSet = data.getMembershipSet().sample(this.samplingRate);
        final SmallTable sampleTable = data.compress(sampleSet);
        final IRowOrder rowOrder = new ArrayRowOrder(this.
                colSortOrder.getSortedRowOrder(sampleTable));
        return new SampleList(sampleTable.compress(this.colSortOrder.toSubSchema(), rowOrder));
    }

    /**
     * Merges two sample tables with the ordering specified by sortOrder.
     */
    @Override
    public SampleList add(@Nullable SampleList left, @Nullable SampleList right) {
        left = Converters.checkNull(left);
        right = Converters.checkNull(right);
        if (!left.table.getSchema().equals(right.table.getSchema()))
            throw new RuntimeException("The schemas do not match.");
        final List<IColumn> mergedCol = new ArrayList<IColumn>(left.table.getSchema().getColumnCount());
        final boolean[] mergeLeft = this.colSortOrder.getMergeOrder(left.table, right.table);
        for (String colName: left.table.getSchema().getColumnNames()) {
            IColumn newCol = mergeColumns(left.table.getColumn(colName),
                    right.table.getColumn(colName), mergeLeft);
            mergedCol.add(newCol);
        }
        return new SampleList(new SmallTable(mergedCol));
    }
}