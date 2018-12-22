/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.table;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntList;
import org.hillview.sketches.ColumnSortOrientation;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.ITable;
import org.hillview.table.api.IndexComparator;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.Linq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * This class specifies an ordering over all records that share a particular schema. These could be
 * records in a single table, or across distinct tables (possibly with different schemas).
 * A RecordOrder is specified by an ordered list of Column names, along with orientations for each.
 * As long as a record contains all these columns, we can project onto just these columns and order
 * the record.
 */
public class RecordOrder implements Serializable {
    private final List<ColumnSortOrientation> sortOrientationList;

    public RecordOrder() {
        this.sortOrientationList = new ArrayList<ColumnSortOrientation>();
    }

    public void append(ColumnSortOrientation columnSortOrientation) {
        this.sortOrientationList.add(columnSortOrientation);
    }

    /**
     * Reverse the given RecordOrder.
     */
    public RecordOrder reverse() {
        RecordOrder rev = new RecordOrder();
        for (int i=0; i < this.getSize(); i++) {
            ColumnSortOrientation ordCol = this.getOrientation(i);
            rev.append(new ColumnSortOrientation(ordCol.columnDescription, !ordCol.isAscending));
        }
        return rev;
    }

    /**
     * Return a schema describing the columns in this sort order.
     */
    public Schema toSchema() {
        Schema newSchema = new Schema();
        for (int i=0; i < this.getSize(); i++) {
            ColumnSortOrientation ordCol = this.getOrientation(i);
            newSchema.append(ordCol.columnDescription);
        }
        return newSchema;
    }

    public int getSize() {
        return this.sortOrientationList.size();
    }

    public ColumnSortOrientation getOrientation(int index) {
        return this.sortOrientationList.get(index);
    }

    /**
     * Returns an IndexComparator for rows in a Table, based on the sort order.
     * The table and the RecordOrder need to be compatible.
     * @param table The Table we wish to sort.
     * @return A Comparator that compares two records based on the RecordOrder specified.
     */
    public IndexComparator getIndexComparator(final ITable table) {
        final ArrayList<IndexComparator> comparatorList = new ArrayList<IndexComparator>();
        List<IColumn> cols = table.getLoadedColumns(Linq.map(this.sortOrientationList,
                ordCol -> ordCol.columnDescription.name));

        for (int i=0; i < this.sortOrientationList.size(); i++) {
            ColumnSortOrientation ordCol = this.sortOrientationList.get(i);
            IColumn col = cols.get(i);
            if (ordCol.isAscending) {
                comparatorList.add(col.getComparator());
            } else {
                comparatorList.add(col.getComparator().rev());
            }
        }
        return new ListComparator(comparatorList);
    }

    public Comparator<RowSnapshot> getRowComparator() {
        return (o1, o2) -> o1.compareTo(o2, RecordOrder.this);
    }

    /**
     * Considers only the rows in the table given by the membership set.
     * Returns an array containing rows indices of a Table in sorted order,
     * using the getIndexComparator method above. The table and the RecordOrder need to be compatible.
     * Should only be applied to very small membership sets.
     * @param table The Table we wish to sort.
     * @param set   Membership set of the table containing the rows to consider.
     */
    public int[] getSortedRowOrder(final ITable table, IMembershipSet set) {
        int[] order = set.getRows();
        IntArrays.quickSort(order, this.getIndexComparator(table));
        return order;
    }

    /**
     * Given two Tables in sorted order, decide the order in which to merge them. We do not treat
     * equality specially: any order is ok. This is used for instance in computing Quantiles.
     * @param left The left side Table
     * @param right the right side Table
     * @return A Boolean array where the i^th element is True if the i^th element in merged table
     * comes form the Left.
     */
    public boolean[] getMergeOrder(final SmallTable left, final SmallTable right) {
        left.check();
        right.check();
        if (!left.schema.equals(right.schema)) {
            throw new RuntimeException("Tables do not have matching schemas");
        }
        final int leftLength = left.getNumOfRows();
        final int rightLength = right.getNumOfRows();
        final int length = leftLength + rightLength;
        final boolean[] mergeLeft = new boolean[length];
        int i = 0, j = 0, k = 0;

        VirtualRowSnapshot vrsLeft = new VirtualRowSnapshot(left, left.schema);
        VirtualRowSnapshot vrsRight = new VirtualRowSnapshot(right, right.schema);
        while ((i < leftLength) && (j < rightLength)) {
            vrsLeft.setRow(i);
            vrsRight.setRow(j);
            int outcome = vrsLeft.compareTo(vrsRight, this);
            if (outcome < 0) {
                mergeLeft[k] = true;
                i++;
                k++;
            } else if (outcome > 0) {
                mergeLeft[k] = false;
                j++;
                k++;
            } else {
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

    /**
     * Given two Tables in sorted order, decide the order in which to merge them. If two rows
     * are equal, they will be combined to a single row (perhaps with a larger count). This is used
     * for instance in TopK computations.
     * @param left The left side Table
     * @param right the right side Table
     * @return A Integer array whose entries encode where the next element in the sorted order comes
     * from. Its i^th element is -1 if the i^th element in merged table
     * comes form the Left, 1 if it comes from the Right, 0 if the two are equal.
     */
    public IntList getIntMergeOrder(SmallTable left, SmallTable right) {
        if (!left.schema.equals(right.schema))
            throw new RuntimeException("Tables do not have matching schemas");
        final int leftLength = left.getNumOfRows();
        final int rightLength = right.getNumOfRows();
        final IntList merge = new IntArrayList();
        int i = 0, j = 0;
        VirtualRowSnapshot vrsLeft = new VirtualRowSnapshot(left, left.schema);
        VirtualRowSnapshot vrsRight = new VirtualRowSnapshot(right, right.schema);
        while ((i < leftLength) && (j < rightLength)) {
            vrsLeft.setRow(i);
            vrsRight.setRow(j);
            int outcome = vrsLeft.compareTo(vrsRight, this);
            if (outcome < 0) {
                merge.add(outcome);
                i++;
            } else if (outcome > 0) {
                merge.add(outcome);
                j++;
            } else {
                merge.add(outcome);
                i++;
                j++;
            }
        }
        while (i < leftLength) {
            merge.add(-1);
            i++;
        }
        while (j < rightLength) {
            merge.add(1);
            j++;
        }
        return merge;
    }
}
