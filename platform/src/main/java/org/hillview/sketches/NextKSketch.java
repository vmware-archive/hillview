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
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.results.IntTopK;
import org.hillview.sketches.results.IntTreeTopK;
import org.hillview.sketches.results.NextKList;
import org.hillview.table.*;
import org.hillview.table.api.*;
import org.hillview.table.columns.DoubleListColumn;
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
    private final AggregateDescription[] aggregates;
    @Nullable
    private final RowSnapshot topRow;
    @Nullable
    private final QuantizationSchema quantizationSchema;
    private final int maxSize;

    /**
     * @param recordOrder The ordering on rows of the table
     * @param topRow The row to start from, set to null if we want to start from the top row.
     * @param maxSize The parameter K in NextK.
     * @param aggregates The columns to compute aggregates on.
     * @param quantizationSchema If not null used to access the data in a differentially-private manner.
     */
    public NextKSketch(RecordOrder recordOrder,
                       @Nullable AggregateDescription[] aggregates,
                       @Nullable RowSnapshot topRow, int maxSize,
                       @Nullable QuantizationSchema quantizationSchema) {
        this.recordOrder = recordOrder;
        this.aggregates = aggregates;
        this.topRow = topRow;
        this.maxSize = maxSize;
        this.quantizationSchema = quantizationSchema;
    }

    public NextKSketch(RecordOrder recordOrder,
                       @Nullable AggregateDescription[] aggregates,
                       @Nullable RowSnapshot topRow, int maxSize) {
        this(recordOrder, aggregates, topRow, maxSize, null);
    }

    /**
     * Given a table, generate the Next K items in Sorted Order starting from a specified
     * rowSnapShot (topRow), together with counts.
     * @param data The input table on which we want to compute the NextK list.
     * @return A NextKList.
     */
    public NextKList create(@Nullable ITable data) {
        Converters.checkNull(data);
        if (this.quantizationSchema != null)
            data = new QuantizedTable(data, this.quantizationSchema);
        IndexComparator comp = this.recordOrder.getIndexComparator(data);
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

        SmallTable aggTable = null;
        if (this.aggregates != null) {
            // Create a map with the indexes of the rows that need to be aggregated
            Int2ObjectRBTreeMap<Double[]> aggregates =
                    new Int2ObjectRBTreeMap<Double[]>(comp);
            IRowIterator it = rowOrder.getIterator();
            for (int row = it.getNextRow(); row >= 0; row = it.getNextRow())
                aggregates.put(row, new Double[this.aggregates.length]);

            // Do a second pass over the data to compute the aggregates
            Schema aggSchema = new Schema();
            for (AggregateDescription ad: this.aggregates)
                // The same column can be aggregated multiple times
                if (!aggSchema.containsColumnName(ad.cd.name))
                    aggSchema.append(ad.cd);
            vw = new VirtualRowSnapshot(data, aggSchema);
            rowIt = data.getRowIterator();
            for (int i = rowIt.getNextRow(); i >= 0; i = rowIt.getNextRow()) {
                vw.setRow(i);
                Double[] agg = aggregates.get(i);
                if (agg == null)
                    continue;
                for (int a = 0; a < this.aggregates.length; a++) {
                    AggregateDescription cad = this.aggregates[a];
                    if (vw.isMissing(cad.cd.name))
                        continue;
                    double d = vw.asDouble(cad.cd.name);
                    switch (cad.agkind) {
                        case Sum:
                            if (agg[a] == null)
                                agg[a] = d;
                            else
                                agg[a] += d;
                            break;
                        case Count:
                            if (agg[a] == null)
                                agg[a] = 1.0;
                            else
                                agg[a]++;
                            break;
                        case Min:
                            if (agg[a] == null)
                                agg[a] = d;
                            else
                                agg[a] = Math.min(agg[a], d);
                            break;
                        case Max:
                            if (agg[a] == null)
                                agg[a] = d;
                            else
                                agg[a] = Math.max(agg[a], d);
                            break;
                    }
                }
            }

            // Create columns for the aggregate table
            List<DoubleListColumn> aggCols = new ArrayList<DoubleListColumn>(this.aggregates.length);
            Schema aggTableSchema = NextKList.getSchema(this.aggregates);
            List<ColumnDescription> cds = aggTableSchema.getColumnDescriptions();
            for (int i = 0; i < this.aggregates.length; i++) {
                AggregateDescription cad = this.aggregates[i];
                ColumnDescription cd = cds.get(i);
                DoubleListColumn col = new DoubleListColumn(cd);
                aggCols.add(col);
            }

            ObjectCollection<Double[]> values = aggregates.values();
            for (Double[] agg: values) {
                for (int a = 0; a < this.aggregates.length; a++) {
                    AggregateDescription cad = this.aggregates[a];
                    DoubleListColumn col = aggCols.get(a);
                    col.append(agg[a]);
                }
            }
            aggTable = new SmallTable(aggCols);
        }

        return new NextKList(topKRows, aggTable, count, position, data.getNumOfRows());
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

    private static ObjectArrayColumn mergeAggregates(
            IColumn left, IColumn right, final List<Integer> mergeOrder, int maxSize,
            AggregateDescription.AggregateKind agkind) {
        final int size = Math.min(maxSize, mergeOrder.size());
        final ObjectArrayColumn merged = new ObjectArrayColumn(left.getDescription(), size);
        int i = 0, j = 0, k = 0;
        while (k < size) {
            if (mergeOrder.get(k) < 0) {
                merged.set(k, left.getDouble(i));
                i++;
            } else if (mergeOrder.get(k) > 0) {
                merged.set(k, right.getDouble(j));
                j++;
            } else {
                switch (agkind) {
                    case Sum:
                    case Count:
                        merged.set(k, right.getDouble(j) + left.getDouble(i));
                        break;
                    case Min:
                        merged.set(k, Math.min(right.getDouble(j), left.getDouble(i)));
                        break;
                    case Max:
                        merged.set(k, Math.max(right.getDouble(j), left.getDouble(i)));
                        break;
                }
                i++;
                j++;
            }
            k++;
        }
        return merged;
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
        if (!left.rows.getSchema().equals(right.rows.getSchema()))
            throw new RuntimeException("The schemas do not match.");
        int width = left.rows.getSchema().getColumnCount();
        List<IColumn> mergedCol = new ArrayList<IColumn>(width);
        IntList mergeOrder = this.recordOrder.getIntMergeOrder(left.rows, right.rows);
        for (String colName : left.rows.getSchema().getColumnNames()) {
            IColumn newCol = ObjectArrayColumn.mergeColumns(left.rows.getColumn(colName),
                    right.rows.getColumn(colName), mergeOrder, this.maxSize);
            mergedCol.add(newCol);
        }
        IntList mergedCounts = this.mergeCounts(left.count, right.count, mergeOrder);
        SmallTable mergedTable = new SmallTable(mergedCol);

        SmallTable aggTable = null;
        if (left.aggregates != null) {
            Converters.checkNull(right.aggregates);
            Converters.checkNull(this.aggregates);
            List<IColumn> aggCols = new ArrayList<IColumn>(width);
            int index = 0;
            for (String colName : left.aggregates.getSchema().getColumnNames()) {
                IColumn newCol = mergeAggregates(left.aggregates.getColumn(colName),
                        right.aggregates.getColumn(colName), mergeOrder, this.maxSize,
                        this.aggregates[index].agkind);
                aggCols.add(newCol);
                index++;
            }
            aggTable = new SmallTable(aggCols);
        }
        return new NextKList(mergedTable, aggTable, mergedCounts,
                left.startPosition + right.startPosition,
                left.rowsScanned + right.rowsScanned);
    }

    @Override
    public NextKList zero() {
        return new NextKList(this.recordOrder.toSchema(), this.aggregates);
    }

    @Override
    public String toString() {
        return "NextKSketch(" + this.maxSize + ")";
    }
}
