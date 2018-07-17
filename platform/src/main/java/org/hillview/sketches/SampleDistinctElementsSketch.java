package org.hillview.sketches;

import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import net.openhft.hashing.LongHashFunction;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.*;

import javax.annotation.Nullable;
import java.util.Comparator;

/**
 * This sketch produces a uniformly random sample of maxSize distinct values from a column (assuming
 * truly random hashing). The values are sampled from the set of distinct values in that column.
 * Missing values are ignored. IF there are fewer than msxSize distinct values, then all of them are
 * returned.
 */
public class SampleDistinctElementsSketch implements ISketch<ITable, MinKSet<String>> {
    private final String colName;
    private final long seed;
    final int maxSize;

    public SampleDistinctElementsSketch(String colName, long seed, int maxSize) {
        this.colName = colName;
        this.seed = seed;
        this.maxSize = maxSize;
    }

    /**
     * The create function takes a table and computes the k distinct values in a specified column
     * that have the minimum hash values.
     */
    @Override
    public MinKSet create(ITable data) {
        IColumn col = data.getLoadedColumn(new ColumnAndConverterDescription(this.colName)).column;
        if (col.getDescription().kind != ContentsKind.String)
            throw new IllegalArgumentException(
                    "SampleDistinctElementsSketch only supports String columns");
        LongHashFunction hash = LongHashFunction.xx(this.seed);
        @Nullable String minString = null;
        @Nullable String maxString = null;
        final IRowIterator myIter = data.getMembershipSet().getIterator();
        MinKRows mkRows = new MinKRows(this.maxSize);
        int currRow = myIter.getNextRow();
        while (currRow >= 0) {
            if (!col.isMissing(currRow)) {
                mkRows.push(col.hashCode64(currRow, hash), currRow);
                String thisString = col.getString(currRow);
                if (minString == null) {
                    minString = thisString;
                    maxString = thisString;
                } else {
                    if (minString.compareTo(thisString) > 0)
                        minString = thisString;
                    if (maxString.compareTo(thisString) < 0)
                        maxString = thisString;
                }
            }
            currRow = myIter.getNextRow();
        }
        return getMinStrings(col, mkRows, minString, maxString);
    }

    private MinKSet<String> getMinStrings(IColumn col, MinKRows mkRows, @Nullable  String minString,
                                          @Nullable String maxString) {
        Long2ObjectRBTreeMap<String> data = new Long2ObjectRBTreeMap<String>();
        for (long hashKey: mkRows.treeMap.keySet())
            data.put(hashKey, col.getString(mkRows.treeMap.get(hashKey)));
        return new MinKSet<String>(this.maxSize, data, Comparator.<String>naturalOrder(),
                minString, maxString);
    }

    @Nullable
    @Override
    public MinKSet<String> zero() {
        return new MinKSet(this.maxSize, Comparator.<String>naturalOrder());
    }

    /**
     * The merge function takes two sets of up to k hash values and merges them to give the set of
     * k smallest hash values overall.
     */
    @Nullable
    public MinKSet<String> add(@Nullable MinKSet<String> left, @Nullable MinKSet<String> right) {
        Long2ObjectRBTreeMap<String> data = new Long2ObjectRBTreeMap<>();
        String minString, maxString;
        if (left == null)
            return right;
        if (right == null)
            return left;
        if (left.min == null) {
            minString = right.min;
            maxString = right.max;
        } else if (right.min == null) {
            minString = left.min;
            maxString = left.max;
        } else {
            minString = (left.min.compareTo(right.min) < 0) ? left.min : right.min;
            maxString = (left.max.compareTo(right.max) > 0) ? left.max : right.max;
        }
        data.putAll(left.data);
        data.putAll(right.data);
        while (data.size() > this.maxSize) {
            long maxKey = data.lastLongKey();
            data.remove(maxKey);
        }
        return new MinKSet(this.maxSize, data, Comparator.<String>naturalOrder(), minString, maxString);
    }
}
