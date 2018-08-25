package org.hillview.sketches;

import it.unimi.dsi.fastutil.longs.Long2IntRBTreeMap;

/**
 * A data structure that stores a set of row indices. These row indices represent a
 * sample of distinct elements from some (group of) columns.
 *
 * Currently this is done by storing those rows that hash to the minimum values. We store
 * the row indices of the k rows that hash to the minimum value (for some column to which the hash
 * is applied). We assume no collisions in the hash function. Finally, we will replace the row
 * indices by the values of the column at those indices. This structure is used to iterate over the
 * table (and avoid boxing/unboxing).
 */
public class MinKRows {
    /**
     * The parameter k in min k hash values.
     */
    public final int maxSize;
    /**
     * The actual number, which can be smaller if there are fewer distinct keys.
     */
    private int curSize;
    /**
     * The largest hash value currently in the min k.
     */
    private long cutoff;
    /**
     * A hashmap consisting of <hashValue, rowIndex> pairs.
     */
    Long2IntRBTreeMap treeMap;

    MinKRows(final int maxSize) {
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
        if (!treeMap.containsKey(hashKey)) { // Does not have it already
            if (hashKey < this.cutoff) {  // It is below cutoff
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
}
