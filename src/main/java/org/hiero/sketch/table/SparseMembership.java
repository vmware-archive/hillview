package org.hiero.sketch.table;

import it.unimi.dsi.fastutil.ints.*;
import org.apache.commons.lang.NullArgumentException;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;

import java.util.*;
import java.util.function.Predicate;


/**
 * This implementation uses a Set data structure to store the membership. It uses the Set's
 * membership and iterator methods. The upside is that it is efficient in space and that the
 * iterator is very efficient. So this implementation is best when the set is sparse.
 * The downside is the constructor that runs in linear time.
 */
public class SparseMembership implements IMembershipSet {
    private final int rowCount;
    private final IntOpenHashSet membershipMap;
    private final int sizeEstimationSampleSize = 20;

    /**
     * Standard way to construct this map is by supplying a membershipSet (perhaps the full one),
     * and the filter function passed as a lambda expression.
     * @param baseMap the base IMembershipSet map on which the filter will be applied
     * @param filter  the additional filter to be applied on the base map
     */
    public SparseMembership(final IMembershipSet baseMap, final Predicate<Integer> filter)
            throws NullArgumentException {
        if (baseMap == null)
            throw new NullArgumentException("PartialMembershipDense cannot be instantiated " +
                    "without a base MembershipSet");
        if (filter == null)
            throw new NullArgumentException("Predicate for PartialMembershipDense cannot be null");
        final IRowIterator baseIterator = baseMap.getIterator();
        this.membershipMap = new IntOpenHashSet(this.estimateSize(baseMap, filter));
        int tmp = baseIterator.getNextRow();
        while (tmp >= 0) {
            if (filter.test(tmp))
                this.membershipMap.add(tmp);
            tmp = baseIterator.getNextRow();
        }
        this.rowCount = this.membershipMap.size();
    }

    /**
     * Instantiates the class without a  predicate. Effectively converts the implementation of
     * the baseMap into that of a sparse map.
     * @param baseMap of type IMembershipSet
     */
    public SparseMembership(final IMembershipSet baseMap) throws NullArgumentException {
        if (baseMap == null)
            throw new NullArgumentException("PartialMembershipDense cannot be instantiated " +
                    "without a base MembershipSet");
        final IRowIterator baseIterator = baseMap.getIterator();
        this.membershipMap = new IntOpenHashSet(baseMap.getSize(false));
        int tmp = baseIterator.getNextRow();
        while (tmp >= 0) {
            this.membershipMap.add(tmp);
            tmp = baseIterator.getNextRow();
        }
        this.rowCount = this.membershipMap.size();
    }

    /**
     * Essentially wraps a Set interface by IMembershipSet
     * @param baseSet of type Set
     */
    public SparseMembership(final IntOpenHashSet baseSet) throws NullArgumentException {
        if (baseSet == null)
            throw new NullArgumentException("PartialMembershipDense cannot be instantiated " +
                    "without a base Set");
        this.membershipMap = baseSet;
        this.rowCount = this.membershipMap.size();
    }

    @Override
    public boolean isMember(final int rowIndex) {
        return this.membershipMap.contains(rowIndex);
    }

    @Override
    public int getSize() {
        return this.rowCount;
    }

    @Override
    public int getSize(final boolean exact) { return this.rowCount; }
    
    private IMembershipSet sample(final int k, final long seed, boolean useSeed) {
        final IntOpenHashSet sampleSet = new IntOpenHashSet(k);
        final Random psg;
        if (useSeed)
            psg = new Random(seed);
        else
            psg = new Random();
        final int offset = psg.nextInt(Integer.max((this.rowCount - k) + 1, 1));
        final IntIterator iter = this.membershipMap.iterator();
        /* TODO: Currently IntIterator.skip(n) takes O(n). */
        iter.skip(offset - 1);
        int tmp;
        for (int i = 0; i < k; i++) {
            if (iter.hasNext()) {
                tmp = iter.nextInt();
                sampleSet.add(tmp);
            }
        }
        return new SparseMembership(sampleSet);
    }

    /**
     * Returns the k items from a random location in the map. Note that the k items are not
     * completely independent but also depend on the placement done by
     * the hash function of IntOpenHashSet.
     */
    @Override
    public IMembershipSet sample(final int k) {
        return sample(k, 0, false);
    }

    /**
     * Returns the k items from a random location in the map. The random location determined
     * by a seed. Note that the k items are not completely independent but also depend on the
     * placement done by the hash function of IntOpenHashSet.
     */
    @Override
    public IMembershipSet sample(final int k, final long seed) {
        return sample(k, seed, true);
    }

    @Override
    public IRowIterator getIterator() {
        return new SparseIterator(this.membershipMap);
    }

    private int estimateSize(final IMembershipSet baseMap, final Predicate<Integer> filter) {
        final IMembershipSet sampleSet = baseMap.sample(sizeEstimationSampleSize);
        int esize = 0;
        final IRowIterator iter= sampleSet.getIterator();
        int curr = iter.getNextRow();
        while (curr >= 0) {
            if (filter.test(curr))
                esize++;
            curr = iter.getNextRow();
        }
        return Integer.max((baseMap.getSize(false) * esize) / sampleSet.getSize(true), 1);
    }

    // Implementing the Iterator
    private static class SparseIterator implements IRowIterator {
        private final IntOpenHashSet membershipMap;
        private final IntIterator myIterator;

        private SparseIterator(final IntOpenHashSet membershipMap) {
            this.membershipMap = membershipMap;
            this.myIterator = this.membershipMap.iterator();
        }

        public int getNextRow() {
            if (this.myIterator.hasNext())
                return this.myIterator.next();
            return -1;
        }
    }
}

