package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.spreadsheet.ColumnOrientation;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.ISchema;
import org.hiero.sketch.table.api.IndexComparator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ColumnSortOrder implements Iterable<ColumnOrientation> {
    private final List<ColumnOrientation> columnOrientationList;

    public ColumnSortOrder() {
        this.columnOrientationList = new ArrayList<ColumnOrientation>();
    }

    public void append(ColumnOrientation columnOrientation) {
        this.columnOrientationList.add(columnOrientation);
    }

    /**
     * Return a schema describing the columns in this sort order.
     */
    public ISchema toSchema() {
        Schema newSchema = new Schema();
        for (ColumnOrientation o: columnOrientationList) {
            newSchema.append(o.columnDescription);
        }
        return newSchema;
    }

    @Override
    public Iterator<ColumnOrientation> iterator() {
        return columnOrientationList.iterator();
    }

    /**
     * Returns a ListComparator for rows in a Table, based on the sort order.
     * The table and the list of Column Orientations need to be compatible.
     * @param inpData The Table we wish to sort.
     * @return A Comparator that compares two rows based on the Sort Order specified.
     */
    public ListComparator getComparator(@NonNull final Table inpData) {
        final List<IndexComparator> comparatorList = new ArrayList<IndexComparator>();
        for (final ColumnOrientation ordCol : this.columnOrientationList) {
            final IColumn nextCol = inpData.getColumn(ordCol.columnDescription.name);
            if (ordCol.isAscending) {
                comparatorList.add(nextCol.getComparator());
            } else {
                comparatorList.add(nextCol.getComparator().rev());
            }
        }
        return new ListComparator(comparatorList);
    }

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
     * @return A boolean array which
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
            for (final ColumnOrientation ordCol : this.columnOrientationList) {
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
