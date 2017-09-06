package org.hillview.sketches;

import org.hillview.dataset.api.IJson;

import javax.annotation.Nullable;
import java.util.TreeSet;

/**
 * A class that would hold a the set of distinct strings from a column bounded in size by maxSize.
 * If maxSize == 0 it holds all distinct strings in the column.
 */
public class DistinctStrings implements IJson {
    private final int maxSize;
    private final TreeSet<String> uniqueStrings;
    private final boolean bounded;
    private boolean truncated;  // if true we are missing some data
    private long columnSize;

    public DistinctStrings(final int maxSize) {
        if (maxSize < 0)
            throw new IllegalArgumentException("size of DistinctString should be positive");
        this.maxSize = maxSize;
        this.bounded = maxSize != 0;
        this.uniqueStrings = new TreeSet<String>();
        this.columnSize = 0;
        this.truncated = false;
    }

    public void add(@Nullable String string) {
        if (this.truncated)
            return;
        if ((this.bounded) && (this.uniqueStrings.size() == this.maxSize)) {
            if (!this.uniqueStrings.contains(string))
                this.truncated = true;
            return;
        }
        this.uniqueStrings.add(string);
    }

    public void setColumnSize(int columnSize) {
        this.columnSize = columnSize;
    }

    public int size() { return this.uniqueStrings.size(); }

    /**
     * @return the union of two sets. The maxSize is the larger of the two. If one
     * of them allow for unbounded size (maxSize = 0) then so does the union.
     */
    public DistinctStrings union(final DistinctStrings otherSet) {
        DistinctStrings result = new DistinctStrings(Math.max(this.maxSize, otherSet.maxSize));
        result.columnSize = this.columnSize + otherSet.columnSize;
        result.truncated = this.truncated || otherSet.truncated;

        for (String item: this.uniqueStrings)
            result.add(item);
        for (String item : otherSet.uniqueStrings)
            result.add(item);
        return result;
    }

    public Iterable<String> getStrings() {
        return this.uniqueStrings;
    }
}
