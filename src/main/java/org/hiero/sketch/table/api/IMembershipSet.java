package org.hiero.sketch.table.api;

/**
 * A IMembershipSet is a representation of a set of integers.
 * These integers represent row indexes in a table.  If an integer
 * is in a IMembershipSet, then it is present in the table.
 */
public interface IMembershipSet extends IRowOrder {
    /**
     * @param rowIndex A non-negative row index.
     * @return True if the given rowIndex is a member of the set.
     */
    boolean isMember(int rowIndex);

    /**
     * @return If exact==true returns the total umber of elements in this membership map. If exact==false may
     * return an approximation of that number. An approximation may be much quicker to compute so if an approximation
     * is sufficient it is recommended to set exact==false.
     */
    int getSize(boolean exact);
    /**
     * @return an IMembershipSet containing k samples from the membership map. The samples are made
     * with replacement so may contain less than k distinct values. There is no guarantee that two subsequent samples
     * return the same sample set.
     */
    IMembershipSet sample(int k);

    /**
     * @return an IMembershipSet containing k samples from the membership map. The samples are made
     * with replacement so may contain less than k distinct values. The pseudo-random generator
     * is seeded with parameter seed, so subsequent calls with the same seed are guaranteed to return the same sample.
     */
    IMembershipSet sample(int k, long seed);

}
