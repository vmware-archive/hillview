package org.hillview.sketches;

import org.hillview.dataset.api.IJson;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;

import javax.annotation.Nullable;
import java.util.TreeSet;

/**
 * A class that would hold a the set of distinct strings from a column bounded in size by maxSize.
 * If maxSize == 0 it holds all distinct strings in the column.
 */
public class DistinctStrings implements IJson {
    private final int maxSize;
    private final TreeSet<String> mySet;
    private final boolean bounded;
    private boolean truncated;  // if true we are missing some data
    private long rowCount;
    private long missingCount;

    public DistinctStrings(final int maxSize) {
        if (maxSize < 0)
            throw new IllegalArgumentException("size of DistinctString should be positive");
        this.maxSize = maxSize;
        this.bounded = maxSize != 0;
        this.mySet = new TreeSet<String>();
        this.rowCount = 0;
        this.missingCount = 0;
        this.truncated = false;
    }

    private void add(@Nullable String string) {
        if (string == null)
            this.missingCount++;
        if (this.truncated)
            return;
        if ((this.bounded) && (this.mySet.size() == this.maxSize)) {
            if (!this.mySet.contains(string))
                this.truncated = true;
            return;
        }
        this.mySet.add(string);
    }

    public void addStrings(final IColumn column, final IMembershipSet membershipSet) {
        this.rowCount = membershipSet.getSize();
        IRowIterator iter = membershipSet.getIterator();
        int row = iter.getNextRow();
        while (row >= 0) {
            String s = column.getString(row);
            this.add(s);
            row = iter.getNextRow();
        }
    }

    public int size() { return this.mySet.size(); }

    /**
     * @return the union of two sets. The maxSize is the larger of the two. If one
     * of them allow for unbounded size (maxSize = 0) then so does the union.
     */
    public DistinctStrings union(final DistinctStrings otherSet) {
        DistinctStrings result = new DistinctStrings(Math.max(this.maxSize, otherSet.maxSize));
        result.rowCount = this.rowCount + otherSet.rowCount;
        result.missingCount = this.missingCount + otherSet.missingCount;

        for (String item: this.mySet)
            result.add(item);
        for (String item : otherSet.mySet)
            result.add(item);
        return result;
    }

    public Iterable<String> getStrings() {
        return this.mySet;
    }
}
