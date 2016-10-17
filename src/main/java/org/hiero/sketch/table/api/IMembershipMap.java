package org.hiero.sketch.table.api;

import org.hiero.sketch.table.api.IRowIterator;

import java.util.function.Function;

/**
 * A IMembershipMap is a representation of a set of integers.
 * These integers represent row indexes in a table.  If an integer
 * is in a IMembershipMap, then it is present in the table.
 */
public interface IMembershipMap {
    /**
     * @param rowIndex A positive row index.
     * @return True if the given rowIndex is a member of the set.
     */
    boolean isMember(int rowIndex);
    /**
     * @return Total number of elements in this membership map.
     */
    public int getSize();
    /**
     * @return An iterator over all the rows in the membership map.
     * The iterator is initialized to point at the "first" row.
     */
    IRowIterator getIterator();

    IMembershipMap subset(Function<Integer, Boolean> isMember);
    IMembershipMap sample(double percentage);
}
