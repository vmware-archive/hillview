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

package org.hillview.sketches;

import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.results.IntTopK;
import org.hillview.sketches.results.IntTreeTopK;
import org.hillview.sketches.results.NextKList;
import org.hillview.table.ArrayRowOrder;
import org.hillview.table.RecordOrder;
import org.hillview.table.Schema;
import org.hillview.table.SmallTable;
import org.hillview.table.api.*;
import org.hillview.table.columns.ObjectArrayColumn;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

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
    public NextKSketch(RecordOrder recordOrder, @Nullable RowSnapshot topRow, int maxSize,
                       boolean toHash) {
        this.recordOrder = recordOrder;
        this.topRow = topRow;
        this.maxSize = maxSize;
    }

    public NextKSketch(RecordOrder recordOrder, @Nullable RowSnapshot topRow, int maxSize) {
        this(recordOrder, topRow, maxSize, true);
    }

    /**
     * Given a table, generate the Next K items in Sorted Order starting from a specified
     * rowSnapShot (topRow), together with counts.
     * @param data The input table on which we want to compute the NextK list.
     * @return A NextKList.
     */
    public NextKList create(@Nullable ITable data) {
        IndexComparator comp = this.recordOrder.getIndexComparator(Converters.checkNull(data));
        IntTopK topK = new IntTreeTopK(this.maxSize, comp);
        IRowIterator rowIt = data.getRowIterator();
        int position = 0;
        Schema toBring = this.recordOrder.toSchema();
        VirtualRowSnapshot vw = new VirtualRowSnapshot(data, toBring);
        for (int i = rowIt.getNextRow(); i >= 0; i = rowIt.getNextRow()) {
            vw.setRow(i);
            if ((this.topRow == null) ||
                    (this.topRow.compareTo(vw, this.recordOrder) <= 0))
                topK.push(i);
            else
                position++;
        }
        Int2IntSortedMap topKList = topK.getTopK();
        IRowOrder rowOrder = new ArrayRowOrder(topKList.keySet().toIntArray());
        SmallTable topKRows = data.compress(this.recordOrder.toSchema(), rowOrder);
        IntList count = new IntArrayList(topKList.size());
        count.addAll(topKList.values());
        return new NextKList(topKRows, count, position, data.getNumOfRows());
    }

    /**
     * Given two Columns containing counts left and right, merge them to a single Column, using an
     * Integer array mergeLeft which represents the order in which elements merge.
     * @param left       The left counts.
     * @param right      The right counts.
     * @param mergeOrder The order in which to merge the two columns.
     * @return The merged counts.
     */
    private IntList mergeCounts(final IntList left,
                                final IntList right,
                                final IntList mergeOrder) {
        final int size = Math.min(this.maxSize, mergeOrder.size());
        final IntList mergedCounts = new IntArrayList(mergeOrder.size());
        int i = 0, j = 0, k = 0;
        while (k < size) {
            if (mergeOrder.getInt(k) < 0) {
                mergedCounts.add(left.getInt(i));
                i++;
            } else if (mergeOrder.getInt(k) > 0) {
                mergedCounts.add(right.getInt(j));
                j++;
            } else {
                mergedCounts.add(left.getInt(i) + right.getInt(j));
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
        Converters.checkNull(left);
        Converters.checkNull(right);
        if (!left.table.getSchema().equals(right.table.getSchema()))
            throw new RuntimeException("The schemas do not match.");
        int width = left.table.getSchema().getColumnCount();
        List<IColumn> mergedCol = new ArrayList<IColumn>(width);
        IntList mergeOrder = this.recordOrder.getIntMergeOrder(left.table, right.table);
        for (String colName : left.table.getSchema().getColumnNames()) {
            IColumn newCol = ObjectArrayColumn.mergeColumns(left.table.getColumn(colName),
                    right.table.getColumn(colName), mergeOrder, this.maxSize);
            mergedCol.add(newCol);
        }
        IntList mergedCounts = this.mergeCounts(left.count, right.count, mergeOrder);
        final SmallTable mergedTable = new SmallTable(mergedCol);
        return new NextKList(mergedTable, mergedCounts,
                left.startPosition + right.startPosition,
                left.rowsScanned + right.rowsScanned);
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
