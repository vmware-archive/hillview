package org.hiero.sketch.table;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import java.util.Random;

/**
 * A IMembershipSet which contains all rows.
 */
public class FullMembership implements IMembershipSet {
    private final int rowCount;

    public FullMembership(final int rowCount) throws NegativeArraySizeException {
        if (rowCount > 0)
            this.rowCount = rowCount;
        else
            throw (new NegativeArraySizeException("Can't initialize FullMembership with " +
                        "negative rowCount"));
    }

    @Override
    public boolean isMember(final int rowIndex) {
        return rowIndex < this.rowCount;
    }

    @Override
    public int getSize() {
        return this.rowCount;
    }

    @Override
    public int getSize(final boolean exact) { return this.rowCount; }

    @Override
    public IRowIterator getIterator() {
        return new FullMemebershipIterator(this.rowCount);
    }

    /**
     * Samples k items. Generator is seeded using its default method. Sampled items are first placed in a Set.
     * The procedure samples k times with replacement so it may return a set with less than k distinct items.
     * @param k the number of samples with replacement
     * @return IMembershipSet instantiated as a Partial Sparse
     */
    @Override
    public IMembershipSet sample(final int k) {
        final Random randomGenerator = new Random();
        return this.sampleUtil(randomGenerator, k);
    }

    /**
     * Same as sample(k) but with the seed of the generator given as a parameter. The procedure
     * samples k times with replacement so it return a set with less than k distinct items
     * @param k the number of samples taken with replacement
     * @param seed the seed for the randomness generator
     * @return IMembershipSet instantiated as a partial sparse
     */
    @Override
    public IMembershipSet sample (final int k, final long seed) {
        final Random randomGenerator = new Random(seed);
        return this.sampleUtil(randomGenerator, k);
    }

    private IMembershipSet sampleUtil(final Random randomGenerator, final int k) {
        final IntOpenHashSet S = new IntOpenHashSet();
        for (int i=0; i < k; i++)
            S.add(randomGenerator.nextInt(this.rowCount));
        return new PartialMembershipSparse(S);
    }

    private static class FullMemebershipIterator implements IRowIterator {
        private int cursor = 0;
        private final int range;

        private FullMemebershipIterator(final int range) {
            this.range = range;
        }

        @Override
        public int getNextRow() {
            if (this.cursor < this.range) {
                this.cursor++;
                return this.cursor-1;
            }
            else return -1;
        }
    }
}
