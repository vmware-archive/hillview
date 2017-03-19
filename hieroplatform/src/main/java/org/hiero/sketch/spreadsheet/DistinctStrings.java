package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.table.SemiExplicitConverter;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import java.util.TreeSet;

/**
 * A class that would hold a the set of distinct strings from a column
 */
public class DistinctStrings {
    private final int maxSize;
    private final TreeSet<String> mySet;

    public DistinctStrings(final int maxSize) {
        this.maxSize = maxSize;
        this.mySet = new TreeSet<String>();
    }

    public DistinctStrings(final int maxSize, final TreeSet<String> mySet) {
        this.maxSize = maxSize;
        this.mySet = mySet;
    }

    public void getStrings(final IColumn column, final IMembershipSet membershipSet) {
        IRowIterator iter = membershipSet.getIterator();
        int row = iter.getNextRow();
        while (row >= 0) {
            if (this.mySet.size() == this.maxSize)
                return;
            this.mySet.add(column.getString(row));
            row = iter.getNextRow();
        }
    }

    public int size() { return this.mySet.size(); }

    public DistinctStrings union(final DistinctStrings otherSet) {
        TreeSet<String> result  = new TreeSet<String>();
        int newMaxSize = Integer.max(this.maxSize, otherSet.maxSize);
        result.addAll(this.mySet);
        result.addAll(otherSet.mySet);
        while(result.size() > newMaxSize) {
            result.pollLast();
        }
        return new DistinctStrings(newMaxSize, result);
    }

    public SemiExplicitConverter  getStringConverter() {
        SemiExplicitConverter converter = new SemiExplicitConverter(this.mySet.size() + 1);
        int i = 1;
        while(!this.mySet.isEmpty()) {
            converter.set(this.mySet.pollFirst(), i);
            i = i + 1;
        }
        return converter;
    }
}
