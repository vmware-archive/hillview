package org.hiero.sketch.table.api;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.utils.Randomness;

import java.util.Random;


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
     * @return an IMembershipSet containing k samples from the membership map. The samples are made
     * without replacement. Returns the full set if its size is smaller than k. There is no guarantee that
     * two subsequent samples return the same sample set.
     */
    IMembershipSet sample(int k);

    /**
     * @return an IMembershipSet containing k samples from the membership map. The samples are made
     * without replacement. Returns the full set if its size is smaller than k. The pseudo-random
     * generator is seeded with parameter seed.
     */
    IMembershipSet sample(int k, long seed);

    /**
     * @return a sample of size (rate * rowCount). randomizes between the floor and ceiling of this expression.
     */
    default IMembershipSet sample(double rate) {
        return this.sample(this.getSampleSize(rate, 0, false));
    }

    /**
     * @return same as sample(double rate) but with the seed for randomness specified by the caller.
     */
    default IMembershipSet sample(double rate, long seed) {
        return this.sample(this.getSampleSize(rate, seed, true), seed);
    }

    /**
     * @return a new map which is the union of current map and otherMap.
     */
    IMembershipSet union(@NonNull IMembershipSet otherMap);

    IMembershipSet intersection(@NonNull IMembershipSet otherMap);

    IMembershipSet setMinus(@NonNull IMembershipSet otherMap);

    default int getSampleSize(double rate, long seed, boolean useSeed) {
        Randomness r = Randomness.getInstance();
        if (useSeed)
            r.setSeed(seed);
        final int sampleSize;
        final double appSampleSize = rate * this.getSize();
        if (r.nextDouble() < (appSampleSize - Math.floor(appSampleSize)))
            sampleSize = (int) Math.floor(appSampleSize);
        else sampleSize = (int) Math.ceil(appSampleSize);
        return sampleSize;
    }
}
