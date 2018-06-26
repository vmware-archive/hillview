package org.hillview.sketches;

import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import net.openhft.hashing.LongHashFunction;
import org.hillview.dataset.api.ISketch;
import org.hillview.table.api.*;

import javax.annotation.Nullable;
import java.util.Comparator;

/**
 * This sketch produces a uniformly random sample of k distinct values from a column (assuming truly
 * random hashing). The values are sampled from the *set* of distinct values taken by that column.
 */
public class BottomKSketch implements ISketch<ITable, MinKSet<String>> {
    private final String colName;
    LongHashFunction hash;
    final int maxSize;

    public BottomKSketch(String colName, long seed, int maxSize) {
        this.colName= colName;
        this.hash = LongHashFunction.xx(seed);
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
            throw new IllegalArgumentException("BottomKSketch only supports String columns");
        final IRowIterator myIter = data.getMembershipSet().getIterator();
        int currRow = myIter.getNextRow();
        MinKRows mkRows = new MinKRows(this.maxSize);
        while (currRow >= 0) {
            if (!col.isMissing(currRow)) {
                mkRows.push(col.hashCode64(currRow, hash), currRow);
            }
            currRow = myIter.getNextRow();
        }
        return getMinStrings(col, mkRows);
    }

    private MinKSet<String> getMinStrings(IColumn col, MinKRows mkRows) {
        Long2ObjectRBTreeMap<String> data = new Long2ObjectRBTreeMap<String>();
        for (long hashKey: mkRows.treeMap.keySet())
            data.put(hashKey, col.getString(mkRows.treeMap.get(hashKey)));
        return new MinKSet<String>(this.maxSize, data, Comparator.comparing(String::toString));
    }

    @Nullable
    @Override
    public MinKSet<String> zero() {
        return new MinKSet(maxSize, Comparator.<String>naturalOrder());
    }

    /**
     * The merge function takes two sets of up to k hash values and merges them to give the set of
     * k smallest hash values overall.
     */
    @Nullable
    public MinKSet<String> add(@Nullable MinKSet<String> left, @Nullable MinKSet<String> right) {
        Long2ObjectRBTreeMap<String> data = new Long2ObjectRBTreeMap<>();
        data.putAll(left.data);
        data.putAll(right.data);
        while (data.size() > this.maxSize) {
            long maxKey = data.lastLongKey();
            data.remove(maxKey);
        }
        return new MinKSet(this.maxSize, data, Comparator.<String>naturalOrder());
    }
}
