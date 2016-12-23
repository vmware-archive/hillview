package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.spreadsheet.ColumnSortOrientation;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.ISchema;
import org.hiero.sketch.table.api.ISubSchema;
import org.hiero.sketch.table.api.IndexComparator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * This class specifies an ordering over all records that share a particular schema. These could be
 * records in a single table, or across distinct tables (possibly with different schemas).
 * A RecordOrder is specified by an ordered list of Column names, along with orientations for each.
 * As long as a record contains all these columns, we can project onto just these columns and order
 * the record.
 */
public class RecordOrder implements Iterable<ColumnSortOrientation> {
    private final List<ColumnSortOrientation> sortOrientationList;

    public RecordOrder() {
        this.sortOrientationList = new ArrayList<ColumnSortOrientation>();
    }

    public void append(ColumnSortOrientation columnSortOrientation) {
        this.sortOrientationList.add(columnSortOrientation);
    }

    /**
     * Return a schema describing the columns in this sort order.
     */
    public ISchema toSchema() {
        Schema newSchema = new Schema();
        for (ColumnSortOrientation o: sortOrientationList) {
            newSchema.append(o.columnDescription);
        }
        return newSchema;
    }

    /**
     * Return a subSchema describing the columns in this sort order.
     */
    public ISubSchema toSubSchema() {
        final HashSubSchema subSchema = new HashSubSchema();
        for (final ColumnSortOrientation ordCol : this) {
            subSchema.add(ordCol.columnDescription.name);
        }
        return subSchema;
    }

    @Override
    public Iterator<ColumnSortOrientation> iterator() {
        return sortOrientationList.iterator();
    }

    /**
     * Returns an IndexComparator for rows in a Table, based on the sort order.
     * The table and the RecordOrder need to be compatible.
     * @param table The Table we wish to sort.
     * @return A Comparator that compares two records based on the Sort Order specified.
     */
    public IndexComparator getComparator(@NonNull final Table table) {
        final List<IndexComparator> comparatorList = new ArrayList<IndexComparator>();
        for (final ColumnSortOrientation ordCol : this.sortOrientationList) {
            final IColumn nextCol = table.getColumn(ordCol.columnDescription.name);
            if (ordCol.isAscending) {
                comparatorList.add(nextCol.getComparator());
            } else {
                comparatorList.add(nextCol.getComparator().rev());
            }
        }
        return new ListComparator(comparatorList);
    }
    /**
     * Returns a ordering of rows in a Table, based on the sort order.
     * The table and the RecordOrder need to be compatible.
     * @param table The Table we wish to sort.
     * @return A Comparator that compares two rows based on the Sort Order specified.
     */
    public Integer[] getSortedRowOrder(@NonNull final Table table) {
        final int size = table.getNumOfRows();
        final Integer[] order = new Integer[size];
        for (int i = 0; i < size; i++) {
            order[i] = i;
        }
        Arrays.sort(order, this.getComparator(table));
        return order;
    }

    /**
     * Given two Tables in sorted order, decide the order in which to merge them.
     * @param left The left side Table
     * @param right the right side Table
     * @return A Boolean array where the i^th element is True if the i^th element in merged table
     * comes form the Left.
     */
    public boolean[] getMergeOrder(@NonNull final Table left, @NonNull final Table right) {
        if (!left.schema.equals(right.schema)) {
            throw new RuntimeException("Tables do not have matching schemas");
        }
        final int  leftLength = left.getNumOfRows();
        final int  rightLength = right.getNumOfRows();
        final int length = leftLength + rightLength;
        final boolean[] mergeLeft = new boolean[length];
        int i = 0, j = 0, k = 0;
        int outcome;
        while ((i < leftLength) && (j < rightLength)) {
            outcome = 0;
            for (final ColumnSortOrientation ordCol : this.sortOrientationList) {
                final IColumn leftCol = left.getColumn(ordCol.columnDescription.name);
                final IColumn rightCol = right.getColumn(ordCol.columnDescription.name);
                final boolean leftMissing = leftCol.isMissing(i);
                final boolean rightMissing = rightCol.isMissing(j);
                if (leftMissing && rightMissing) {
                    outcome = 0;
                } else if (leftMissing) {
                    outcome = 1;
                } else if (rightMissing) {
                    outcome = -1;
                } else {
                    switch (left.schema.getKind(ordCol.columnDescription.name)) {
                        case String:
                            outcome = leftCol.getString(i).compareTo(rightCol.getString(j));
                            break;
                        case Date:
                            outcome = leftCol.getDate(i).compareTo(rightCol.getDate(j));
                            break;
                        case Int:
                            outcome = Integer.compare(leftCol.getInt(i), rightCol.getInt(j));
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
                mergeLeft[k] = true;
                mergeLeft[k + 1] = false;
                i++;
                j++;
                k += 2;
            }
        }
        if (i < leftLength) {
            while (k < length) {
                mergeLeft[k] = true;
                k++;
            }
        } else if (j < rightLength) {
            while (k < length) {
                mergeLeft[k] = false;
                k++;
            }
        }
        return mergeLeft;
    }
}
