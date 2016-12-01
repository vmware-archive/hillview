package org.hiero.sketch.table.api;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * A IMembershipSet is a representation of a set of integers.
 * These integers represent row indexes in a table.  If an integer
 * is in an IMembershipSet, then it is present in the table.
 */
public interface IMembershipSet extends IRowOrder {
    /**
     * @param rowIndex A non-negative row index.
     * @return True if the given rowIndex is a member of the set.
     */
    boolean isMember(int rowIndex);

    /**
     * @return Total number of elements in this membership map.
     */
    int getSize();

    /**
     * @return an IMembershipSet containing k samples from the membership map. The samples are made
     * with replacement so may contain less than k distinct values. There is no guarantee that
     * two subsequent samples return the same sample set.
     */
    IMembershipSet sample(int k);

    /**
     * @return an IMembershipSet containing k samples from the membership map. The samples are made
     * with replacement so may contain less than k distinct values. The pseudo-random generated
     * is seeded with parameter seed, so subsequent calls with the same seed are guaranteed to
     * return the same sample.
     */
    IMembershipSet sample(int k, long seed);

    /**
     * @return the union of current map and otherMap.
     * FIXME: currentMap is destroyed.
     */
    IMembershipSet union(@NonNull IMembershipSet otherMap);

    IMembershipSet intersection(@NonNull IMembershipSet otherMap);

    IMembershipSet setMinus(@NonNull IMembershipSet otherMap);

    /**
     * @return a deep copy of this.
     * FIXME: This should be removed.
     */
    IMembershipSet copy();
}
