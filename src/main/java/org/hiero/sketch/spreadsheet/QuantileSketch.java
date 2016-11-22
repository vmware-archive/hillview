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

public class QuantileSketch implements ISketch<Table, QuantileList> {

    private final List<OrderedColumn> sortOrder;

    public QuantileSketch(final List<OrderedColumn> sortOrder) {
        this.sortOrder = sortOrder;
    }

    /** Returns a ListComparator for rows in a Table, based on the sort order.
     * @param data The table whose rows we wish to compare
     * @return A Comparator that compares two rows based on the Sort Order specified.
     */
    private ListComparator getComparator(final Table data) {
        List<RowComparator> comparatorList = new ArrayList<RowComparator>();
        for (final OrderedColumn ordCol: this.sortOrder) {
            final IColumn nextCol = data.getColumn(ordCol.colName);
            if (ordCol.isAscending) {
                comparatorList.add(nextCol.getComparator());
            } else {
                comparatorList.add(nextCol.getComparator().rev());
            }
        }
        return new ListComparator(comparatorList);
    }


    /**
     * Creates a sample Table for an input Table,
     * @param data The input table
     * @param numSamples The number of samples
     * @return A table of samples.
     */
    public Table sampleTable(final Table data, final int numSamples) {
        final int dataSize = data.getNumOfRows();
        final IMembershipSet sampleSet = data.members.sample(numSamples);
        final HashSubSchema subSchema = new HashSubSchema();
        for (final OrderedColumn ordCol: this.sortOrder) {
            subSchema.add(ordCol.colName);
        }
        return data.compress(subSchema, sampleSet);
    }

    /**
     * Given a table and a desired resolution for percentiles, return the answer for a sample.
     * @param data Input table
     * @param resolution Number of buckets: percentiles correspond to 100 bucket etc.
     * @return A table of size resolution, whose ith entry is ranked approximately i/resolution.
     */
    public Table getQuantile(final Table data, final int resolution){
        final int perBin =100;
        final int numSamples = resolution*perBin;
        final Table sampleTable = sampleTable(data, numSamples);
        final RowComparator rowComparator = this.getComparator(sampleTable);
        final Integer[] order = new Integer[numSamples];
        for (int i = 0; i < numSamples; i++)
            order[i] = i;
        Arrays.sort(order, rowComparator);
        final int [] quantile = new int[resolution];
        for(int i =0; i < resolution; i++)
            quantile[i] = order[perBin*i];
        final IMembershipSet quantileMembers = new ArrayMembership(quantile);
        return sampleTable.compress(quantileMembers);
    }

    @Override
    public QuantileList zero() {
        return null;
    }

    @Override
    public QuantileList add(final QuantileList left, final QuantileList right) {
        return null;
    }

    @Override
    public Observable<PartialResult<QuantileList>> create(final Table data) {
        return null;
    }
}
