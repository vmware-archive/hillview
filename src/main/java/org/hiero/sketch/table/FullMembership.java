package org.hiero.sketch.table;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * A IMembershipSet which contains all rows.
 */
public class FullMembership implements IMembershipSet {
    private final int rowCount;

    public FullMembership(int rowCount) throws NegativeArraySizeException {
        if (rowCount > 0)
            this.rowCount = rowCount;
        else
            throw (new NegativeArraySizeException("Can't initialize FullMembership with " +
                        "negative rowCount"));
    }

    @Override
    public boolean isMember(int rowIndex) {
        return rowIndex < this.rowCount && rowIndex >= 0;
    }

    @Override
    public int getSize() {
        return this.rowCount;
    }

    @Override
    public int getSize(boolean exact) { return this.rowCount; }

    @Override
    public IRowIterator getIterator() {
        return new FullMemebershipIterator(this.rowCount);
    }

    /**
     * Samples k items. Generator is seeded using its default method. Sampled items are first placed in a Set.
     * The procedure samples k times with replacement so it may return a set with less than k distinct items.
     * @param k
     * @return ImembershipSet instantiated as a Partial Sparse
     */
    @Override
    public IMembershipSet sample(int k) {
        Random randomGenerator = new Random();
        return sampleUtil(randomGenerator, k);
    }

    /**
     * Same as sample(k) but with the seed of the generator given as a parameter. The procedure
     * samples k times with replacement so it return a set with less than k distinct items
     * @param k
     * @param seed
     * @return ImembershipSet instantiated as a partial sparse
     */
    @Override
    public IMembershipSet sample (int k, long seed) {
        Random randomGenerator = new Random(seed);
        return sampleUtil(randomGenerator, k);
    }

    //todo: use the best set implementation.
    private IMembershipSet sampleUtil(Random randomGenerator, int k) {
        IntOpenHashSet S = new IntOpenHashSet();
        for (int i=0; i < k; i++)
            S.add(randomGenerator.nextInt(this.rowCount));
        return new PartialMembershipSparse(S);
    }

    private static class FullMemebershipIterator implements IRowIterator {
        private int cursor = 0;
        private final int range;

        public  FullMemebershipIterator(int range) {
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
