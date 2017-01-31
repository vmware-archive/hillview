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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

public class TopKSketch implements ISketch<Table, NextKList> {
    @NonNull
    private final RecordOrder colSortOrder;
    private final int maxSize;


    public TopKSketch(RecordOrder colSortOrder, int maxSize) {
        this.colSortOrder = colSortOrder;
        this.maxSize = maxSize;
    }

    @Override
    public NextKList zero() {
        return new NextKList(this.colSortOrder.toSchema());
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
     * Add two NextK Lists using the preceding helper functions.
     * @param left The left TopK list
     * @param right The right TopK list
     * @return The merged list.
     */
    @Override
    public NextKList add(NextKList left, NextKList right) {
        if (!left.table.schema.equals(right.table.schema))
            throw new RuntimeException("The schemas do not match.");
        final int width = left.table.schema.getColumnCount();
        final ObjectArrayColumn[] mergedCol = new ObjectArrayColumn[width];
        List<Integer> mergeOrder = this.colSortOrder.getIntMergeOrder(left.table, right.table);
        int i = 0;
        for (String colName : left.table.schema.getColumnNames()) {
            mergedCol[i] = this.mergeColumns(left.table.getColumn(colName),
                    right.table.getColumn(colName), mergeOrder);
            i++;
        }
        List<Integer> mergedCounts = this.mergeCounts(left.count,
                right.count, mergeOrder);
        final IMembershipSet full = new FullMembership(mergedCounts.size());
        final Table mergedTable = new Table(left.table.schema, mergedCol, full);
        /* The returned quantileList can be of size up to slack* resolution*/
        return new NextKList(mergedTable, mergedCounts, 0, null);
    }

    /**
     * Given a table, generate a TopK List according to the Sorted Order.
     * @param data The input table on which we want to compute the TopK list.
     * @return A Table containing the top K rows (projected onto the relevant columns) and a list
     * containing counts of how often each rows appeared.
     */
    public NextKList getKList(Table data) {
        IndexComparator comp = this.colSortOrder.getComparator(data);
        TreeTopK<Integer> topK = new TreeTopK<>(this.maxSize, comp);
        for(int i = 0; i < data.getNumOfRows(); i++)
            topK.push(i);
        SortedMap<Integer, Integer> topKList = topK.getTopK();
        IRowOrder rowOrder = new ArrayRowOrder(topKList.keySet());
        Table topKRows = data.compress(this.colSortOrder.toSubSchema(), rowOrder);
        List<Integer> count = new ArrayList<Integer>();
        Iterator<Integer> it = topKList.values().iterator();
        while (it.hasNext()) {
            count.add(it.next());
        }
        return new NextKList(topKRows, count, 0, null);
    }

    @Override
    public Observable<PartialResult<NextKList>> create(final Table data) {
        NextKList q = this.getKList(data);
        PartialResult<NextKList> result = new PartialResult<>(1.0, q);
        return Observable.just(result);
    }
}
