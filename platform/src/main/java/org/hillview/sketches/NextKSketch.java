/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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

package org.hillview.sketches;

import org.hillview.dataset.api.ISketch;
import org.hillview.table.*;
import org.hillview.table.api.*;
import org.hillview.table.columns.ObjectArrayColumn;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

/**
 * Given a data set, the NextKSketch generates the Next K items in Sorted Order (specified by a
 * RecordOrder) starting from a specified rowSnapShot (topRow). It also computes counts for how many
 * rows in the data project onto each entry, and how many rows come before topRow.
 */
public class NextKSketch implements ISketch<ITable, NextKList> {
    private final RecordOrder recordOrder;
    @Nullable
    private final RowSnapshot topRow;
    private final int maxSize;

    /**
     * @param recordOrder The ordering on rows of the table
     * @param topRow The row to start from, set to null if we want to start from the top row.
     * @param maxSize The parameter K in NextK.
     */
    public NextKSketch(RecordOrder recordOrder, @Nullable RowSnapshot topRow, int maxSize) {
        this.recordOrder = recordOrder;
        this.topRow = topRow;
        this.maxSize = maxSize;
    }

    /**
     * Given a table, generate the Next K items in Sorted Order starting from a specified
     * rowSnapShot (topRow), together with counts.
     * @param data The input table on which we want to compute the NextK list.
     * @return A NextKList.
     */
    @Override
    public NextKList create(ITable data) {
        IndexComparator comp = this.recordOrder.getComparator(data);
        TreeTopK<Integer> topK = new TreeTopK<Integer>(this.maxSize, comp);
        IRowIterator rowIt = data.getRowIterator();
        int i = rowIt.getNextRow();
        int position = 0;
        VirtualRowSnapshot vw = new VirtualRowSnapshot(data);
        while (i >= 0) {
            vw.setRow(i);
            if ((this.topRow == null) ||
                    (this.topRow.compareTo(vw, this.recordOrder) <= 0))
                topK.push(i);
            else
                position++;
            i = rowIt.getNextRow();
        }
        SortedMap<Integer, Integer> topKList = topK.getTopK();
        IRowOrder rowOrder = new ArrayRowOrder(topKList.keySet());
        SmallTable topKRows = data.compress(this.recordOrder.toSubSchema(), rowOrder);
        List<Integer> count = new ArrayList<Integer>();
        count.addAll(topKList.values());
        return new NextKList(topKRows, count, position, data.getNumOfRows());
    }

    /**
     * Given two Columns left and right, merge them to a single Column, using an Integer
     * array mergeOrder which represents the order in which elements merge as follows:
     * -1: left; +1: right; 0: both are equal, so add either but advance in both lists.
     * @param left       The left column
     * @param right      The right column
     * @param mergeOrder The order in which to merge the two columns.
     * @return The merged column.
     */
    private ObjectArrayColumn mergeColumns(final IColumn left,
                                           final IColumn right,
                                           final List<Integer> mergeOrder) {
        final int size = Math.min(this.maxSize, mergeOrder.size());
        final ObjectArrayColumn merged = new ObjectArrayColumn(left.getDescription(), size);
        int i = 0, j = 0, k = 0;
        while (k < size) {
            if (mergeOrder.get(k) < 0) {
                merged.set(k, left.getObject(i));
                i++;
            } else if (mergeOrder.get(k) > 0) {
                merged.set(k, right.getObject(j));
                j++;
            } else {
                merged.set(k, right.getObject(j));
                i++;
                j++;
            }
            k++;
        }
        return merged;
    }

    /**
     * Given two Columns containing counts left and right, merge them to a single Column, using an
     * Integer array mergeLeft which represents the order in which elements merge.
     * @param left       The left counts.
     * @param right      The right counts.
     * @param mergeOrder The order in which to merge the two columns.
     * @return The merged counts.
     */
    private List<Integer> mergeCounts(final List<Integer> left,
                                      final List<Integer> right,
                                      final List<Integer> mergeOrder) {
        final int size = Math.min(this.maxSize, mergeOrder.size());
        final List<Integer> mergedCounts = new ArrayList<Integer>(mergeOrder.size());
        int i = 0, j = 0, k = 0;
        while (k < size) {
            if (mergeOrder.get(k) < 0) {
                mergedCounts.add(left.get(i));
                i++;
            } else if (mergeOrder.get(k) > 0) {
                mergedCounts.add(right.get(j));
                j++;
            } else {
                mergedCounts.add(left.get(i) + right.get(j));
                i++;
                j++;
            }
            k++;
        }
        return mergedCounts;
    }

    /**
     * Add two NextK Lists, merging counts of identical rows.
     * @param left The left TopK list
     * @param right The right TopK list
     * @return The merged list.
     */
    @Override @Nullable
    public NextKList add(@Nullable NextKList left, @Nullable NextKList right) {
        left = Converters.checkNull(left);
        right = Converters.checkNull(right);
        if (!left.table.getSchema().equals(right.table.getSchema()))
            throw new RuntimeException("The schemas do not match.");
        int width = left.table.getSchema().getColumnCount();
        List<IColumn> mergedCol = new ArrayList<IColumn>(width);
        List<Integer> mergeOrder = this.recordOrder.getIntMergeOrder(left.table, right.table);
        for (String colName : left.table.getSchema().getColumnNames()) {
            IColumn newCol = this.mergeColumns(left.table.getColumn(colName),
                    right.table.getColumn(colName), mergeOrder);
            mergedCol.add(newCol);
        }
        List<Integer> mergedCounts = this.mergeCounts(left.count, right.count, mergeOrder);
        final SmallTable mergedTable = new SmallTable(mergedCol);
        return new NextKList(mergedTable, mergedCounts,
                left.startPosition + right.startPosition,
                left.totalRows + right.totalRows);
    }

    @Override
    public NextKList zero() {
        return new NextKList(this.recordOrder.toSchema());
    }

    @Override
    public String toString() {
        return "NextKSketch(" + this.maxSize + ")";
    }
}
