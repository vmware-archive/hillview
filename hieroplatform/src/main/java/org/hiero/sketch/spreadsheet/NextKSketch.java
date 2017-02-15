package org.hiero.sketch.spreadsheet;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.api.ISketch;
import org.hiero.sketch.dataset.api.PartialResult;
import org.hiero.sketch.table.*;
import org.hiero.sketch.table.api.*;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

public class NextKSketch implements ISketch<ITable, NextKList> {
    @NonNull
    private final RecordOrder recordOrder;
    private final RowSnapshot topRow;
    private final int maxSize;

    public NextKSketch(@NonNull RecordOrder recordOrder, @NonNull RowSnapshot topRow, int maxSize) {
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
    public NextKList getNextKList(ITable data) {
        IndexComparator comp = this.recordOrder.getComparator(data);
        TreeTopK<Integer> topK = new TreeTopK<Integer>(this.maxSize, comp);
        IRowIterator rowIt = data.getRowIterator();
        RowToTable rowToTable = new RowToTable(this.topRow, data, this.recordOrder);
        int i, position = 0;
        do {
            i = rowIt.getNextRow();
            if (i != -1) {
                if (rowToTable.compareToRow(i) >= 0) {
                    topK.push(i);
                } else {
                    position++;
                }
            }
        } while (i != -1);
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
    private ObjectArrayColumn mergeColumns(@NonNull final IColumn left,
                                           @NonNull final IColumn right,
                                           @NonNull final List<Integer> mergeOrder) {
        final int size = Math.min(this.maxSize, mergeOrder.size());
        final ObjectArrayColumn merged = new ObjectArrayColumn(left.getDescription(), size);
        int i = 0, j = 0, k = 0;
        while (k < size) {
            if (mergeOrder.get(k) == -1) {
                merged.set(k, left.getObject(i));
                i++;
            } else if (mergeOrder.get(k) == 1) {
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
    private List<Integer> mergeCounts(@NonNull final List<Integer> left,
                                      @NonNull final List<Integer> right,
                                      @NonNull final List<Integer> mergeOrder) {
        final int size = Math.min(this.maxSize, mergeOrder.size());
        final List<Integer> mergedCounts = new ArrayList<Integer>(mergeOrder.size());
        int i = 0, j = 0, k = 0;
        while (k < size) {
            if (mergeOrder.get(k) == -1) {
                mergedCounts.add(left.get(i));
                i++;
            } else if (mergeOrder.get(k) == 1) {
                mergedCounts.add(right.get(i));
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
    @Override
    public NextKList add(NextKList left, NextKList right) {
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
    public Observable<PartialResult<NextKList>> create(ITable data) {
        return this.pack(this.getNextKList(data));
    }
}
