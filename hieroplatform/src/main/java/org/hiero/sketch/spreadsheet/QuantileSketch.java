package org.hiero.sketch.spreadsheet;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.dataset.api.PartialResult;
import org.hiero.sketch.table.*;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowOrder;
import rx.Observable;

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;

public class QuantileSketch implements ISketch<Table, QuantileList> {
    private final int resolution;
    private final ColumnSortOrder colSortOrder;

    /**
     * @param sortOrder The list of column orientations.
     * @param resolution Number of buckets: percentiles correspond to 100 buckets etc.
     */
    public QuantileSketch(final List<ColumnOrientation> sortOrder, final int resolution) {
        this.colSortOrder = new ColumnSortOrder(sortOrder);
        this.resolution = resolution;
    }

    /**
     * Given a table and a desired resolution for percentiles, return the answer for a sample.
     * The size of the sample is resolution*perBin, perBin is set to 100 by default.
     * @return A table of size resolution, whose ith entry is ranked approximately i/resolution.
     */
    public QuantileList getQuantile(final Table data) {
        /* Sample a set of rows from the table. */
        final int perBin = 100;
        final int numSamples = this.resolution * perBin;
        final int dataSize = data.getNumOfRows();
        final IMembershipSet sampleSet = data.members.sample(numSamples);
        final Table sampleTable = data.compress(sampleSet);
        /* Sort the sampled rows. */
        final int realSize = sampleTable.getNumOfRows();
        final Integer[] order = new Integer[realSize];
        for (int i = 0; i < realSize; i++) {
            order[i] = i;
        }
        Arrays.sort(order, this.colSortOrder.getComparator(sampleTable));
        /* Pick equally spaced elements as the sample quantiles.*/
        final int[] quantile = new int[this.resolution + 1];
        final ApproxRank[] approxRank = new ApproxRank[this.resolution + 1];
        for (int i = 0; i <= resolution; i++) {
            quantile[i] = order[((realSize - 1) * i) / resolution];
            final int step = dataSize / resolution;
            approxRank[i] = new ApproxRank(i * step, (resolution - i) * step);
        }
        final IRowOrder quantileMembers = new ArrayRowOrder(quantile);
        final HashSubSchema subSchema = new HashSubSchema();
        for (final ColumnOrientation ordCol : this.colSortOrder) {
            subSchema.add(ordCol.colName);
        }
        return new QuantileList(sampleTable.compress(subSchema, quantileMembers),
                approxRank, dataSize);
    }

    private ObjectArrayColumn mergeColumns(@NonNull final IColumn left,
                                           @NonNull final IColumn right,
                                           @NonNull final boolean[] mergeLeft) {
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

    private ApproxRank[] mergeRanks(@NonNull final QuantileList left,
                                    @NonNull final QuantileList right,
                                    @NonNull final boolean[] mergeLeft) {
        final int length = left.getQuantileSize() + right.getQuantileSize();
        final ApproxRank[] mergedRank = new ApproxRank[length];
        int i = 0, j = 0, lower, upper;
        for (int k = 0; k < length; k++) {
            if (mergeLeft[k]) { /* i lost to j, so we insert i next*/
                 /*
                 *  Entry i gets its own lowerRank + the lowerRank for the biggest entry on
                 *  the right that lost to it. This is either the lower Rank of j-1, or 0 if i beat
                 *  nobody on the right (which means j = 0);
                 */
                lower = left.getLowerRank(i) + ((j > 0) ? right.getLowerRank(j - 1) : 0);
                /*  Similarly, its upper bound is its own upper bound + the upper bound of the
                 *  smallest element on the right that beat it. This is the upper rank of j if the
                 *  right hand side has not been exhausted, in which case it is 0.
                 */
                upper = left.getUpperRank(i) +
                        ((j < right.getQuantileSize()) ? right.getUpperRank(j) : 0);
                mergedRank[k] = new ApproxRank(lower, upper);
                i++;
            } else {
                lower = right.getLowerRank(j) + ((i > 0) ? left.getLowerRank(i - 1) : 0);
                upper = right.getUpperRank(j) +
                        ((i < left.getQuantileSize()) ? left.getUpperRank(i) : 0);
                mergedRank[k] = new ApproxRank(lower, upper);
                j++;
            }
        }
        return mergedRank;
    }

    @Override
    public QuantileList add(@NonNull final QuantileList left, @NonNull final QuantileList right) {
        final int width = left.getSchema().getColumnCount();
        final int length = left.getQuantileSize() + right.getQuantileSize();
        final ObjectArrayColumn[] mergedCol = new ObjectArrayColumn[width];
        final boolean[] mergeLeft = this.colSortOrder.getMergeOrder(left.quantile, right.quantile);
        for (int i = 0; i < width; i++) {
            mergedCol[i] = mergeColumns(left.getColumn(i), right.getColumn(i), mergeLeft);
        }
        final IMembershipSet full = new FullMembership(length);
        final Table mergedTable = new Table(left.getSchema(), mergedCol, full);
        final ApproxRank[] mergedRank = mergeRanks(left, right, mergeLeft);
        final int mergedDataSize = left.getDataSize() + right.getDataSize();
        return new QuantileList(mergedTable, mergedRank, mergedDataSize);
    }

    @Override
    public QuantileList zero() {
        //TODO
        return null;
    }

    @Override
    public Observable<PartialResult<QuantileList>> create(final Table data) {
        QuantileList q = this.getQuantile(data);
        PartialResult<QuantileList> result = new PartialResult<>(1.0, q);
        return Observable.just(result);
    }
}