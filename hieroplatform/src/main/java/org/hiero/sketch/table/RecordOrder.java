/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0 2
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
 *
 */

package org.hiero.sketch.table;

import org.hiero.sketch.spreadsheet.ColumnSortOrientation;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.ISubSchema;
import org.hiero.sketch.table.api.ITable;
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
    public Schema toSchema() {
        Schema newSchema = new Schema();
        for (ColumnSortOrientation o: this.sortOrientationList) {
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
        return this.sortOrientationList.iterator();
    }

    /**
     * Returns an IndexComparator for rows in a Table, based on the sort order.
     * The table and the RecordOrder need to be compatible.
     * @param table The Table we wish to sort.
     * @return A Comparator that compares two records based on the RecordOrder specified.
     */
    public IndexComparator getComparator(final ITable table) {
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
     * Returns an array containing rows indices of a Table in sorted order, using the getComparator
     * method above. The table and the RecordOrder need to be compatible.
     * Should only be applied to very small tables.
     * @param table The Table we wish to sort.
     * @return A Comparator that compares two rows based on the Sort Order specified.
     */
    public Integer[] getSortedRowOrder(final SmallTable table) {
        final int size = table.getNumOfRows();
        final Integer[] order = new Integer[size];
        for (int i = 0; i < size; i++) {
            order[i] = i;
        }
        Arrays.sort(order, this.getComparator(table));
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
    public List<Integer> getIntMergeOrder(SmallTable left, SmallTable right) {
        if (!left.schema.equals(right.schema)) {
            throw new RuntimeException("Tables do not have matching schemas");
        }
        final int  leftLength = left.getNumOfRows();
        final int  rightLength = right.getNumOfRows();
        final List<Integer> merge = new ArrayList<>();
        int i = 0, j = 0;
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
                        case Json:
                            outcome = leftCol.getString(i).compareTo(rightCol.getString(j));
                            break;
                        case Date:
                            outcome = leftCol.getDate(i).compareTo(rightCol.getDate(j));
                            break;
                        case Int:
                            outcome = Integer.compare(leftCol.getInt(i), rightCol.getInt(j));
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
                    merge.add(outcome);
                    i++;
                    break;
                } else if (outcome == 1) {
                    merge.add(outcome);
                    j++;
                    break;
                }
            }
            if (outcome == 0) {
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
