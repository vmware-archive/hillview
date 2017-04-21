package org.hiero.sketches;

import org.hiero.dataset.api.ISketch;
import org.hiero.table.ArrayRowOrder;
import org.hiero.table.RecordOrder;
import org.hiero.table.SmallTable;
import org.hiero.table.api.IColumn;
import org.hiero.table.api.IMembershipSet;
import org.hiero.table.api.IRowOrder;
import org.hiero.table.api.ITable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static org.hiero.table.ObjectArrayColumn.mergeColumns;

public class SampleQuantileSketch  implements ISketch<ITable, SampleList>{

    private final RecordOrder colSortOrder;

    /**
     * The rate at which we sample the data. It is set to be (resolution)^2/dataSize,
     * so we expect a table of size (resolution)^2. This is chosen so that the desired accuracy in
     * the quantiles is 1/resolution. It can be tuned by changing the size by a constant factor.
     */
    private double samplingRate;

    /**
     * @param sortOrder The list of column orientations.
     * @param resolution Number of buckets: percentiles correspond to 100 buckets etc.
     */
    public SampleQuantileSketch(final RecordOrder sortOrder, final int resolution,
                                final long dataSize) {
        this.colSortOrder = sortOrder;
        int n = Math.max(resolution, 100);
        this.samplingRate = (1.0*resolution*resolution)/dataSize;
    }

    @Nullable
    @Override
    public SampleList zero() {
        return new SampleList(new SmallTable(this.colSortOrder.toSchema()));
    }

    @Override
    public SampleList create(ITable data) {
        final IMembershipSet sampleSet = data.getMembershipSet().sample(this.samplingRate);
        final SmallTable sampleTable = data.compress(sampleSet);
        final IRowOrder rowOrder = new ArrayRowOrder(this.
                colSortOrder.getSortedRowOrder(sampleTable));
        return new SampleList(sampleTable.compress(this.colSortOrder.toSubSchema(), rowOrder));
    }

    @Override
    public SampleList add(@Nullable SampleList left, @Nullable SampleList right) {
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