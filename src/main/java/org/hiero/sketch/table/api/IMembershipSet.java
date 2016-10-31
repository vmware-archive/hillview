package org.hiero.sketch.table.api;

/**
 * A IMembershipSet is a representation of a set of integers.
 * These integers represent row indexes in a table.  If an integer
 * is in a IMembershipSet, then it is present in the table.
 */
public interface IMembershipSet {
    /**
     * @param rowIndex A positive row index.
     * @return True if the given rowIndex is a member of the set.
     */
    boolean isMember(int rowIndex);
    /**
     * @return Total number of elements in this membership map.
     */
    int getSize();

    /**
     * @return an ImembershipSet containing k samples from the membership map. The samples are made
     * with replacement so may contain less than k distinct values.
     */
    IMembershipSet sample(int k);

    /**
     * @return an ImembershipSet containing k samples from the membership map. The samples are made
     * with replacement so may contain less than k distinct values. The pseudo-random generated
     * is seeded with parameter seed.
     */
    IMembershipSet sample(int k, long seed);
    /**
     * @return An iterator over all the rows in the membership map.
     * The iterator is initialized to point at the "first" row.
     */
    IRowIterator getIterator();
}
