package org.hiero.sketch.spreadsheet;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.dataset.api.PartialResult;
import org.hiero.sketch.table.*;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowOrder;
import org.hiero.sketch.table.api.IndexComparator;
import rx.Observable;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QuantileSketch implements ISketch<Table, Table> {

    private final List<OrderedColumn> sortOrder;


    public QuantileSketch(final List<OrderedColumn> sortOrder) {
        this.sortOrder = sortOrder;
    }

    /**
     * Returns a ListComparator for rows in a Table, based on the sort order.
     * The table needs to share the schema of the Table data, but it could be a subset.
     *
     * @return A Comparator that compares two rows based on the Sort Order specified.
     */
    private ListComparator getComparator(final Table inpData) {
        final List<IndexComparator> comparatorList = new ArrayList<IndexComparator>();
        for (final OrderedColumn ordCol : this.sortOrder) {
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
     *
     * @param resolution Number of buckets: percentiles correspond to 100 buckets etc.
     * @return A table of size resolution, whose ith entry is ranked approximately i/resolution.
     */
    public QuantileList getQuantile(final Table data, final int resolution) {
        /* Sample a set of rows from the table. */
        final int perBin = 100;
        final int numSamples = resolution * perBin;
        final int dataSize = data.getNumOfRows();
        final IMembershipSet sampleSet = data.members.sample(numSamples);
        final Table sampleTable = data.compress(sampleSet);
        /* Sort the sampled rows. */
        final int realSize = sampleTable.getNumOfRows();
        final Integer[] order = new Integer[realSize];
        for (int i = 0; i < realSize; i++) {
            order[i] = i;
        }
        Arrays.sort(order, this.getComparator(sampleTable));
        /* Pick equally spaced elements as the sample quantiles.*/
        final int[] quantile = new int[resolution + 1];
        final ApproxRank[] approxRank = new ApproxRank[resolution + 1];
        for (int i = 0; i <= resolution; i++) {
            quantile[i] = order[((realSize - 1) * i) / resolution];
            final int step = dataSize / resolution;
            approxRank[i] = new ApproxRank(i * step, (resolution - i) * step);
        }
        final IRowOrder quantileMembers = new ArrayRowOrder(quantile);
        final HashSubSchema subSchema = new HashSubSchema();
        for (final OrderedColumn ordCol : this.sortOrder) {
            subSchema.add(ordCol.colName);
        }
        return new QuantileList(sampleTable.compress(subSchema, quantileMembers),
                approxRank, dataSize);
    }

    private boolean[] getMergeOrder(@NonNull final QuantileList left, @NonNull final QuantileList right) {
        if (!left.getSchema().equals(right.getSchema())) {
            throw new RuntimeException("Tables do not have matching schemas");
        }
        final int length = left.getQuantileSize() + right.getQuantileSize();
        final boolean[] mergeLeft = new boolean[length];
        int i = 0, j = 0, k = 0;
        int outcome;
        while ((i < left.getQuantileSize()) && (j < right.getQuantileSize())) {
            outcome = 0;
            for (final OrderedColumn ordCol : this.sortOrder) {
                final IColumn leftCol = left.getColumn(ordCol.colName);
                final IColumn rightCol = right.getColumn(ordCol.colName);
                final boolean leftMissing = leftCol.isMissing(i);
                final boolean rightMissing = rightCol.isMissing(j);
                if (leftMissing && rightMissing) {
                    outcome = 0;
                } else if (leftMissing) {
                    outcome = 1;
                } else if (rightMissing) {
                    outcome = -1;
                } else {
                    switch (left.getSchema().getKind(ordCol.colName)) {
                        case String:
                            outcome = leftCol.getString(i).compareTo(rightCol.getString(j));
                            break;
                        case Date:
                            outcome = leftCol.getDate(i).compareTo(rightCol.getDate(j));
                            break;
                        case Int:
                            outcome = Integer.compare(leftCol.getInt(i), rightCol.getInt(j));
                            /*if(outcome == 1)
                                System.out.printf("Left is larger%n");
                            else if(outcome == -1)
                                System.out.printf("Right is larger%n");
                            else
                                System.out.printf("Its a tie!%n");*/
                            break;
                        case Json:
                            outcome = leftCol.getString(i).compareTo(rightCol.getString(j));
                            break;
                        case Double:
                            outcome = Double.compare(leftCol.getDouble(i), rightCol.getDouble(j));
                            break;
                        case Duration:
                            outcome = leftCol.getDuration(i).compareTo(rightCol.getDuration(j));
                            break;
                    }
                }
                if (!ordCol.isAscending) {
                    outcome *= -1;
                }
                if (outcome == -1) {
                    mergeLeft[k] = true;
                    i++;
                    k++;
                    break;
                } else if (outcome == 1) {
                    mergeLeft[k] = false;
                    j++;
                    k++;
                    break;
                }
            }
            if (outcome == 0) {
                /*System.out.printf("Strange: Its a tie!%n");*/
                mergeLeft[k] = true;
                mergeLeft[k + 1] = false;
                i++;
                j++;
                k += 2;
            }
        }
        if (i < left.getQuantileSize()) {
            while (k < length) {
                mergeLeft[k] = true;
                k++;
            }
        } else if (j < right.getQuantileSize()) {
            while (k < length) {
                mergeLeft[k] = false;
                k++;
            }
        }
        return mergeLeft;
    }

    private ObjectArrayColumn mergeColumns(@NonNull final IColumn left, @NonNull final IColumn right,
                                           @NonNull final boolean[] mergeLeft) {
        if (mergeLeft.length != (left.sizeInRows() + right.sizeInRows())) {
            throw new InvalidParameterException("Length of mergeOrder must equal " +
                    "sum of lengths of the columns");
        }
        final ObjectArrayColumn merged = new ObjectArrayColumn(left.getDescription(), mergeLeft.length);
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

    private ApproxRank[] mergeRanks(@NonNull final QuantileList left, @NonNull final QuantileList right,
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

    public QuantileList mergeQuantiles(@NonNull final QuantileList left, @NonNull final QuantileList right) {
        final int width = left.getSchema().getColumnCount();
        final int length = left.getQuantileSize() + right.getQuantileSize();
        final ObjectArrayColumn[] mergedCol = new ObjectArrayColumn[width];
        final boolean[] mergeLeft = getMergeOrder(left, right);
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