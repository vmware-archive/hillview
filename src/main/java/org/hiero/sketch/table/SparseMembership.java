package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.utils.IntSet;
import java.util.function.Predicate;


/**
 * This implementation uses a Set data structure to store the membership. It uses the Set's
 * membership and iterator methods. The upside is that it is efficient in space and that the
 * iterator is very efficient. So this implementation is best when the set is sparse.
 * The downside is the constructor that runs in linear time.
 */
public class SparseMembership implements IMembershipSet {
    private final int rowCount;
    @NonNull
    private final IntSet membershipMap;
    private static final int sizeEstimationSampleSize = 20;

    /**
     * Standard way to construct this map is by supplying a membershipSet (perhaps the full one),
     * and the filter function passed as a lambda expression.
     * @param baseMap the base IMembershipSet map on which the filter will be applied
     * @param filter  the additional filter to be applied on the base map
     */
    public SparseMembership(@NonNull final IMembershipSet baseMap,
                            @NonNull final Predicate<Integer> filter) {
        final IRowIterator baseIterator = baseMap.getIterator();
        this.membershipMap = new IntSet(this.estimateSize(baseMap, filter));
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
    public SparseMembership(@NonNull final IMembershipSet baseMap) {
        final IRowIterator baseIterator = baseMap.getIterator();
        final int expectedSize;
        if (baseMap instanceof LazyMembership)
            expectedSize = ((LazyMembership) baseMap).getApproxSize();
        else
            expectedSize = baseMap.getSize();
        this.membershipMap = new IntSet(expectedSize);
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
    public SparseMembership(@NonNull final IntSet baseSet) {
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

    /**
     * Returns the k items from a random location in the map. Note that the k items are not
     * completely independent but also depend on the placement done by the hash function.
     * Remark: Slight difference from the contract in the interface, which calls for the sample to
     * be independent with replacement. In order to obtain that call sample(1,seed) k times with
     * different seeds.
     */
    @Override
    public IMembershipSet sample(final int k) {
        return new SparseMembership(this.membershipMap.sample(k, 0, false));
    }

    /**
     * Returns the k items from a random location in the map determined by a seed provided.
     * Note that the k items are not completely independent but also depend on the placement
     * done by the hash function.
     * Remark: Slight difference from the contract in the interface, which calls for the sample to
     * be independent with replacement. In order to obtain that call sample(1,seed) k times with
     * different seeds.
     */
    @Override
    public IMembershipSet sample(final int k, final long seed) {
        return new SparseMembership(this.membershipMap.sample(k, seed, true));
    }

    @Override
    public IRowIterator getIterator() {
        return new SparseIterator(this.membershipMap);
    }

    @Override
    public IMembershipSet union(@NonNull final IMembershipSet otherSet) {
        final IntSet unionSet = this.membershipMap.copy();
        final IRowIterator iter = otherSet.getIterator();
        int curr = iter.getNextRow();
        while (curr >=0) {
            unionSet.add(curr);
            curr = iter.getNextRow();
        }
        return new SparseMembership(unionSet);
    }

    @Override
    public IMembershipSet intersection(@NonNull final IMembershipSet otherSet) {
        final IntSet intersectSet = new IntSet();
        final IRowIterator iter = otherSet.getIterator();
        int curr = iter.getNextRow();
        while (curr >=0) {
            if (this.isMember(curr))
                intersectSet.add(curr);
            curr = iter.getNextRow();
        }
        return new SparseMembership(intersectSet);
    }

    @Override
    public IMembershipSet setMinus(@NonNull final IMembershipSet otherSet) {
        final IntSet setMinusSet = new IntSet();
        final IRowIterator iter = this.getIterator();
        int curr = iter.getNextRow();
        while (curr >=0) {
            if (!otherSet.isMember(curr))
                setMinusSet.add(curr);
            curr = iter.getNextRow();
        }
        return new SparseMembership(setMinusSet);
    }

    /**
     * Estimates the size of a filter applied to an IMembershipSet
     * @return an approximation of the size, based on a sample of size 20. May return 0.
     * There are no strict guarantees on the quality of the approximation, but is good enough for
     * initialization of a hash table sizes.
     */
    private int estimateSize(@NonNull final IMembershipSet baseMap,
                             @NonNull final Predicate<Integer> filter) {
        final IMembershipSet sampleSet = baseMap.sample(sizeEstimationSampleSize);
        if (sampleSet.getSize() == 0)
            return 0;
        int esize = 0;
        final IRowIterator iter= sampleSet.getIterator();
        int curr = iter.getNextRow();
        while (curr >= 0) {
            if (filter.test(curr))
                esize++;
            curr = iter.getNextRow();
        }
        if (baseMap instanceof LazyMembership)
            return (((LazyMembership) baseMap).getApproxSize() * esize) /
                    sampleSet.getSize();
        return (baseMap.getSize() * esize) / sampleSet.getSize();
    }

    private class SparseIterator implements IRowIterator {
        final private IntSet.IntSetIterator mysetIterator;

        private SparseIterator(@NonNull IntSet mySet) {
            this.mysetIterator = mySet.getIterataor();
        }

        @Override
        public int getNextRow() {
            return this.mysetIterator.getNext();
        }
    }
}
