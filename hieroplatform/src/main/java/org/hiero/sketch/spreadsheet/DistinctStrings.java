package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.table.SemiExplicitConverter;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;

import java.util.TreeSet;

/**
 * A class that would hold a the set of distinct strings from a column bounded in size by maxSize.
 * If maxSize == 0 it holds all distinct strings in the column.
 */
public class DistinctStrings {
    private final int maxSize;
    private final TreeSet<String> mySet;
    private final boolean bounded;

    public DistinctStrings(final int maxSize) {
        if (maxSize < 0)
            throw new IllegalArgumentException("size of DistinctString should be positive");
        this.maxSize = maxSize;
        this.bounded = maxSize != 0;
        this.mySet = new TreeSet<String>();
    }

    public DistinctStrings(final int maxSize, final TreeSet<String> mySet) {
        if (maxSize < 0)
            throw new IllegalArgumentException("size of DistinctString should be positive");
        this.maxSize = maxSize; // if maxSize == 0 there would be no bound on the size
        this.bounded = maxSize != 0;
        this.mySet = mySet;
    }

    public void addStrings(final IColumn column, final IMembershipSet membershipSet) {
        if (!column.getDescription().kind.equals(ContentsKind.String))
            throw new IllegalArgumentException("DistinctStrings requires a String column");
        IRowIterator iter = membershipSet.getIterator();
        int row = iter.getNextRow();
        while (row >= 0) {
            if ((this.bounded) && (this.mySet.size() == this.maxSize))
                return;
            this.mySet.add(column.getString(row));
            row = iter.getNextRow();
        }
    }

    public int size() { return this.mySet.size(); }


    /**
     * @return the union of two sets. The maxSize is the larger of the two. If one
     * of them allow for unbounded size (maxSize = 0) then so does the union.
     */
    public DistinctStrings union(final DistinctStrings otherSet) {
        TreeSet<String> result  = new TreeSet<String>();
        int newMaxSize;
        if (Integer.min(this.maxSize, otherSet.maxSize) == 0)
            newMaxSize = 0;
        else
            newMaxSize = Integer.max(this.maxSize, otherSet.maxSize);
        result.addAll(this.mySet);
        for (String item : otherSet.mySet) {
            if ((result.size() == newMaxSize) && (newMaxSize > 0)) {
                return new DistinctStrings(newMaxSize, result);
            }
            result.add(item);
        }
        return new DistinctStrings(newMaxSize, result);
    }

    public SemiExplicitConverter  getStringConverter() {
        SemiExplicitConverter converter = new SemiExplicitConverter();
        int i = 0;
        for (String item : this.mySet) {
            converter.set(item, i);
            i++;
        }
        return converter;
    }
}
