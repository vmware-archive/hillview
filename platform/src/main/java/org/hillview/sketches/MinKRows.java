package org.hillview.sketches;

import it.unimi.dsi.fastutil.longs.Long2IntRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import org.hillview.table.api.IColumn;

/**
 * A data structure that computes the minHash of table. It stores the row indices of the k rows that
 * hash to the minimum value (for some column to which the hash is applied). We assume no collisions
 * in the hash function.Finally, we will replace the row indices by the values of the column at
 * those indices. This structure is used to iterate over the table (and avoid boxing/unboxing).
 */
public class MinKRows {
    /**
     * The parameter k in min k hash values.
     */
    public final int maxSize;
    /**
     * The actual number, which can be smaller if there are fewer distinct keys.
     */
    public int curSize;
    /**
     * The largest hash value currently in the min k.
     */
    public long cutoff;
    /**
     * A hashmap consisting of <hashValue, rowIndex> pairs.
     */
    public Long2IntRBTreeMap treeMap;

    public MinKRows(final int maxSize) {
        this.maxSize = maxSize;
        this.treeMap = new Long2IntRBTreeMap();
        this.curSize = 0;
        this.cutoff = 0;
    }

    public void push(long hashKey, int row) {
        if (this.curSize == 0) {
            this.treeMap.put(hashKey, row);
            this.cutoff = hashKey;
            this.curSize += 1;
        }
        if (!treeMap.containsKey(hashKey)) { //Does not have it already
            if (hashKey < this.cutoff) {  //It is below cutoff
                this.treeMap.put(hashKey, row);
                if (this.curSize == this.maxSize) {
                    this.treeMap.remove(this.cutoff);
                    this.cutoff = this.treeMap.lastLongKey();
                } else
                    this.curSize += 1;
            } else { // we ignore hash collisions.
                if (this.curSize < this.maxSize) {
                    this.curSize += 1;
                    this.treeMap.put(hashKey, row);
                    this.cutoff = hashKey;
                }
            }
        }
    }

    public MinKSet getMinKSet(IColumn iCol) {
        Long2ObjectRBTreeMap<Object> data = new Long2ObjectRBTreeMap();
        for (long hashKey: this.treeMap.keySet())
            data.put(hashKey, iCol.getObject(treeMap.get(hashKey)));
        return new MinKSet(this.maxSize, data);
    }

}
