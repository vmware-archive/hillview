package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.table.*;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowOrder;
import org.hiero.sketch.table.api.ITable;
import org.hiero.utils.Converters;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

/**
 * QuantileSketch is used to compute Quantiles over a distributed data set according to a prescribed
 * ordering of the elements. Quantiles are represented using the QuantileList class.
 * QuantileSketch provides two main methods:
 * - getQuantile: It creates a QuantileList from an input Table
 * - add: It combines two QuantileLists created from disjoint dataSets to create a single new
 *   QuantileList, that captures Quantile information for the union.
 */
public class QuantileSketch implements ISketch<ITable, QuantileList> {
    /**
     * the order and orientation of the columns to define the sorted order.
     */
    private final RecordOrder colSortOrder;
    /**
     * the desired number of quantiles.
     */
    private final int resolution;
    /**
     * a knob to control the size of the QuantileList that is shipped around
     * (the size is slack*resolution)
     */
    private final int slack = 10;
    /**
     * a knob to control the sample size taken fromm a table to create a QuantileList
     * (the size is perBin*resolution)
     */
    private static final int perBin = 100;

    /**
     * @param sortOrder The list of column orientations.
     * @param resolution Number of buckets: percentiles correspond to 100 buckets etc.
     */
    public QuantileSketch(final RecordOrder sortOrder, final int resolution) {
        this.colSortOrder = sortOrder;
        this.resolution = resolution;
    }

    /**
     * Given a table and a desired resolution for percentiles, return the answer for a sample.
     * The size of the sample is resolution*perBin, perBin is set to 100 by default.
     * The size of the Quantile table we return is slack*resolution. Since this table is shipped
     * around, we might want to take perBin >> slack to improve the quality of our sample.
     * @param data The input data on which we want to compute Quantiles.
     * @return A table of size resolution, whose ith entry is ranked approximately i/resolution.
     */
    public QuantileList getQuantile(final ITable data) {
        /* Sample a set of rows from the table, then sort the sampled rows. */
        final IMembershipSet sampleSet = data.getMembershipSet().sample(this.resolution * perBin);
        final SmallTable sampleTable = data.compress(sampleSet);
        final Integer[] order = this.colSortOrder.getSortedRowOrder(sampleTable);
        /* We will shrink the set of samples  down to slack*resolution. Number of samples might be
            less than resolution*perBin, because of repetitions. */
        final int newRes = Math.min(this.slack * this.resolution, sampleSet.getSize());
        final int[] quantile = new int[newRes];
        final QuantileList.WinsAndLosses[] winsAndLosses = new QuantileList.WinsAndLosses[newRes];
        final double sampleStep = ((double)sampleSet.getSize() + 1)/(newRes + 1);
        final double dataStep = ((double)data.getNumOfRows() + 1)/(newRes + 1);
        /* Pick equally spaced elements as the sample quantiles.
        *  Our estimate for the rank of element i is i*dataStep. */
        for (int i = 0; i < newRes; i++) {
            quantile[i] = order[(int)(Math.round((i + 1) * sampleStep) - 1)];
            winsAndLosses[i] = new QuantileList.WinsAndLosses((int)Math.round((i + 1) * dataStep),
                    (int)Math.round((newRes - i - 1) * dataStep));
        }
        final IRowOrder quantileMembers = new ArrayRowOrder(quantile);
        return new QuantileList(sampleTable.compress(this.colSortOrder.toSubSchema(),
                quantileMembers), winsAndLosses, data.getNumOfRows());
    }

    /**
     * Given two Columns left and right, merge them to a single Column, using the Boolean
     * array mergeLeft which represents the order in which elements merge.
     * mergeLeft[i] = true means the i^th element comes from the left column.
     * @param left The left column
     * @param right The right column
     * @param mergeLeft The order in which to merge the two columns.
     * @return The merged column.
     */
    private ObjectArrayColumn mergeColumns(final IColumn left,
                                           final IColumn right,
                                           final boolean[] mergeLeft) {
        if (mergeLeft.length != (left.sizeInRows() + right.sizeInRows())) {
            throw new InvalidParameterException("Length of mergeOrder must equal " +
                    "sum of lengths of the columns");
        }
        final ObjectArrayColumn merged = new
                ObjectArrayColumn(left.getDescription(), mergeLeft.length);
        int i = 0, j = 0, k = 0;
        while (k < mergeLeft.length) {
            if (mergeLeft[k]) {
                merged.set(k, left.getObject(i));
                i++;
            } else {
                merged.set(k, right.getObject(j));
                j++;
            }
            k++;
        }
        return merged;
    }
    /**
     * Given two QuantileLists left and right, compute the number of Wins and Losses for the
     * elements in the merged order, represented by Boolean array mergeLeft which represents the
     * order in which elements merge.
     * @param left The left column
     * @param right The right column
     * @param mergeLeft The order in which to merge the two columns.
     * @return The ApproxRanks (wins and losses) for elements in the merged QuantileList.
     */
    private QuantileList.WinsAndLosses[] mergeRanks(final QuantileList left,
                                                    final QuantileList right,
                                                    final boolean[] mergeLeft) {
        final int length = left.getQuantileSize() + right.getQuantileSize();
        final QuantileList.WinsAndLosses[] mergedRank = new QuantileList.WinsAndLosses[length];
        int i = 0, j = 0, lower, upper;
        for (int k = 0; k < length; k++) {
            if (mergeLeft[k]) { /* i lost to j, so we insert i next*/
                 /* Entry i gets its own Wins + the Wins for the biggest entry on
                 *  the right that lost to it. This is either the Wins of j-1, or 0 if i beat
                 *  nobody on the right (which means j = 0);*/
                lower = left.getWins(i) + ((j > 0) ? right.getWins(j - 1) : 0);
                /*  Similarly, its Losses are its own Losses + the Losses of the
                 *  smallest element on the right that beat it. This is the Losses of j if the
                 *  right hand side has not been exhausted, in which case it is 0. */
                upper = left.getLosses(i) +
                        ((j < right.getQuantileSize()) ? right.getLosses(j) : 0);
                mergedRank[k] = new QuantileList.WinsAndLosses(lower, upper);
                i++;
            } else {
                lower = right.getWins(j) + ((i > 0) ? left.getWins(i - 1) : 0);
                upper = right.getLosses(j) +
                        ((i < left.getQuantileSize()) ? left.getLosses(i) : 0);
                mergedRank[k] = new QuantileList.WinsAndLosses(lower, upper);
                j++;
            }
        }
        return mergedRank;
    }

    /**
     * Given two QuantileLists left and right, merge them to a single QuantileList.
     * @param left The left Quantile
     * @param right The right Quantile
     * @return The merged Quantile
     */
    @Override @Nullable
    public QuantileList add(@Nullable QuantileList left, @Nullable QuantileList right) {
        left = Converters.checkNull(left);
        right = Converters.checkNull(right);
        if (!left.getSchema().equals(right.getSchema()))
            throw new RuntimeException("The schemas do not match.");
        final int width = left.getSchema().getColumnCount();
        final List<IColumn> mergedCol = new ArrayList<IColumn>(width);
        final boolean[] mergeLeft = this.colSortOrder.getMergeOrder(left.quantile, right.quantile);
        for (String colName: left.getSchema().getColumnNames()) {
            IColumn newCol = mergeColumns(left.getColumn(colName),
                    right.getColumn(colName), mergeLeft);
            mergedCol.add(newCol);
        }
        final SmallTable mergedTable = new SmallTable(mergedCol);
        final QuantileList.WinsAndLosses[] mergedRank = mergeRanks(left, right, mergeLeft);
        final int mergedDataSize = left.getDataSize() + right.getDataSize();
        /* The returned quantileList can be of size up to slack* resolution*/
        return new QuantileList(mergedTable, mergedRank, mergedDataSize).
                compressExact(this.slack*this.resolution);
    }

    @Override @Nullable
    public QuantileList zero() {
        return new QuantileList(this.colSortOrder.toSchema());
    }

    @Override
    public QuantileList create(final ITable data) {
        return this.getQuantile(data);
    }
}