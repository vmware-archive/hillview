package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.dataset.api.PartialResult;
import org.hiero.sketch.table.ArrayMembership;
import org.hiero.sketch.table.HashSubSchema;
import org.hiero.sketch.table.ListComparator;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.RowComparator;
import rx.Observable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QuantileSketch implements ISketch<Table, Table> {

    private final List<OrderedColumn> sortOrder;
    private final Table data;

    public QuantileSketch(Table data, List<OrderedColumn> sortOrder) {
        this.data = data;
        this.sortOrder = sortOrder;
    }

    /** Returns a ListComparator for rows in a Table, based on the sort order.
     * @return A Comparator that compares two rows based on the Sort Order specified.
     */
    private ListComparator getComparator(final Table inpData) {
        List<RowComparator> comparatorList = new ArrayList<RowComparator>();
        for (final OrderedColumn ordCol: this.sortOrder) {
            final IColumn nextCol = inpData.getColumn(ordCol.colName);
            if (ordCol.isAscending) {
                comparatorList.add(nextCol.getComparator());
            } else {
                comparatorList.add(nextCol.getComparator().rev());
            }
        }
        return new ListComparator(comparatorList);
    }

    /**
     * Given a table and a desired resolution for percentiles, return the answer for a sample.
     * @param resolution Number of buckets: percentiles correspond to 100 buckets etc.
     * @return A table of size resolution, whose ith entry is ranked approximately i/resolution.
     */
    public Table getQuantile(final int resolution) {
        final int perBin = 100;
        final Table sampleTable = data.sampleTable(resolution * perBin);
        final int realSize = sampleTable.getNumOfRows();
        final Integer[] order = new Integer[realSize];
        for (int i = 0; i < realSize; i++)
            order[i] = i;
        Arrays.sort(order, this.getComparator(sampleTable));
        final int [] quantile = new int[resolution];
        for(int i =0; i < resolution; i++)
            quantile[i] = order[((realSize -1)*i)/(resolution - 1)];
        final IMembershipSet quantileMembers = new ArrayMembership(quantile);
        final HashSubSchema subSchema = new HashSubSchema();
        for (final OrderedColumn ordCol: this.sortOrder) {
            subSchema.add(ordCol.colName);
        }
        return sampleTable.compress(subSchema, quantileMembers);
    }

    @Override
    public Table zero() {
        return null;
    }

    @Override
    public Table add(final Table left, final Table right) {
        return null;
    }

    @Override
    public Observable<PartialResult<Table>> create(final Table data) {
        return null;
    }
}
