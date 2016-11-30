package org.hiero.sketch.table;

import org.scalactic.exceptions.NullArgumentException;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import java.util.function.Predicate;

/**
 * This implementation uses a full membership data structure plus a list of filter functions that
 * are used for the isMember and for the iterator functions. Upside is that construction is quick,
 * adding a filter function is quick. The downside is that the isMember can take a long time if
 * there are many filters, and iterator takes a long time if it's sparse. Also, each first call for
 * getSize after a new filter is a linear scan.
 */

public class LazyMembership implements IMembershipSet {
    private final IMembershipSet baseMap;
    private int rowCount;
    private boolean rowCountCorrect;
    private final Predicate<Integer> filter;
    private static final int sizeEstimationSampleSize = 20;
    private static final int samplingAttempts = 10;

    public LazyMembership(final IMembershipSet baseMap) throws NullArgumentException {
        if (baseMap == null) throw new NullArgumentException("PartialMembershipDense cannot be " +
                "instantiated without a base MembershipSet");
        this.baseMap = baseMap;
        this.rowCount = baseMap.getSize();
        this.filter = Integer -> true;
        this.rowCountCorrect = true;
    }

    /**
     * instantiated with a a membershipSet, possibly the full one, and a filter predicate.
     */
    public LazyMembership(final IMembershipSet baseMap, final Predicate<Integer> filter) throws
            NullArgumentException {
        if (baseMap == null) throw new NullArgumentException("PartialMembershipDense cannot be " +
                "instantiated without a base MembershipSet");
        if (filter == null) throw new NullArgumentException("PartialMembershipDense cannot be " +
                "instantiated with a null filter");
        this.baseMap = baseMap;
        this.rowCount = 0;
        this.filter = filter;
        this.rowCountCorrect = false;
    }

    @Override
    public boolean isMember(final int rowIndex) {
        return this.baseMap.isMember(rowIndex) && this.filter.test(rowIndex);
    }

    @Override
    public int getSize() {
        if (this.rowCountCorrect) return this.rowCount;
        else {
            int counter = 0;
            final IRowIterator it = this.baseMap.getIterator();
            int tmp = it.getNextRow();
            while (tmp >= 0) {
                if (this.filter.test(tmp))
                    counter++;
                tmp = it.getNextRow();
            }
            this.rowCount = counter;
            this.rowCountCorrect = true;
            return this.rowCount;
        }
    }

    private IMembershipSet sample(final int k, final long seed, final boolean useSeed) {
        int samples = 0;
        IMembershipSet batchSet;
        final IntSet sampleSet = new IntSet(k);
        for (int attempt = 0; attempt < samplingAttempts; attempt++) {
            if (useSeed)
                batchSet = this.baseMap.sample(k, seed + attempt);
            else
                batchSet = this.baseMap.sample(k);
            final IRowIterator it = batchSet.getIterator();
            int tmprow = it.getNextRow();
            while (tmprow >= 0) {
                if (this.isMember(tmprow)) {
                    sampleSet.add(tmprow);
                    samples++;
                    if (samples == k)
                        return new SparseMembership(sampleSet);
                }
                tmprow = it.getNextRow();
            }
        }
        return new SparseMembership(sampleSet);
    }

    /**
     * @return A sample of k items from the membership set. The sample is with replacement so may
     * contain less than k distinct elements. The sample is obtained by sampling k items from the
     * base map and filtering it. This is done samplingAttempts times, if k samples had been found
     * the function gives up and returns whatever was found.
     * This will happen if the membership is sparse.
     */
    @Override
    public IMembershipSet sample(final int k) {
        return sample(k, 0, false);
    }

    /**
     * Samples the base map for k items and then applies the filter on that set.
     * Makes samplingAttempts attempts to reach k samples
     * this way and then gives up and returns whatever was sampled.
     * @param k the number of samples without replacement taken
     * @param seed the seed for the random generator
     * @return
     */
    @Override
    public IMembershipSet sample(final int k, final long seed) {
        return sample(k, seed, true);
    }

    /**
     *
     * @return An approximation of the size based on a sample of sizeEstimationSampleSize.
     * function may return 0.
     * Exact size given by getSize() is expensive and takes linear time the first time it is called
     */
    public int getApproxSize() {
        if (this.rowCountCorrect)
            return this.rowCount;
        final IMembershipSet sampleSet = this.sample(sizeEstimationSampleSize);
        if (sampleSet.getSize() == 0)
            return 0;
        int snumber = 0;
        final IRowIterator it = sampleSet.getIterator();
        int curr = it.getNextRow();
        while (curr >= 0) {
            if (this.filter.test(curr))
                snumber++;
            curr = it.getNextRow();
        }
        if (this.baseMap instanceof LazyMembership)
            return (((LazyMembership) this.baseMap).getApproxSize() * snumber)
                    / sampleSet.getSize();
        else
            return (this.baseMap.getSize() * snumber) / sampleSet.getSize();
    }

    @Override
    public IRowIterator getIterator() {
        return new DenseIterator(this.baseMap, this.filter);
    }

    @Override
    public IMembershipSet union(IMembershipSet otherMap) throws NullArgumentException {
        if (otherMap == null)
            throw new NullArgumentException("can not perform union with a null");
        if (otherMap instanceof LazyMembership) {
            IMembershipSet newBase = this.baseMap.union(((LazyMembership) otherMap).baseMap);
            Predicate newFilter =  this.filter.or(((LazyMembership) otherMap).filter);
            return new LazyMembership(newBase, newFilter);
        }
        if (otherMap instanceof FullMembership) {
            IMembershipSet newBase = this.baseMap.union(otherMap);
            Predicate newFilter =  this.filter.or(p -> ((FullMembership) otherMap).isMember(p));
            return new LazyMembership(newBase, newFilter);
        }
        return otherMap.union(this);
    }

    @Override
    public IMembershipSet intersection(IMembershipSet otherMap) throws NullArgumentException {
        if (otherMap == null)
            throw new NullArgumentException("can not perform intersection with a null");
        if (otherMap instanceof LazyMembership) {
            IMembershipSet newBase = this.baseMap.intersection(((LazyMembership) otherMap).baseMap);
            Predicate newFilter =  this.filter.and(((LazyMembership) otherMap).filter);
            return new LazyMembership(newBase, newFilter);
        }
        if (otherMap instanceof FullMembership) {
            IMembershipSet newBase = this.baseMap.intersection(otherMap);
            Predicate newFilter =  this.filter;
            return new LazyMembership(newBase, newFilter);
        }
        return otherMap.intersection(this);
    }

    @Override
    public IMembershipSet setMinus(IMembershipSet otherMap) throws NullArgumentException {
        if (otherMap == null)
            throw new NullArgumentException("can not perform setMinus with a null");
        IntSet setMinusSet = new IntSet();
        IRowIterator iter = this.getIterator();
        int curr = iter.getNextRow();
        while (curr >=0 ) {
            if (!otherMap.isMember(curr))
                setMinusSet.add(curr);
            curr = iter.getNextRow();
        }
        return new SparseMembership(setMinusSet);
    }


    @Override
    public IMembershipSet copy() {
        IMembershipSet newBase = this.baseMap.copy();
        LazyMembership newMap =  new LazyMembership(newBase,this.filter);
        newMap.rowCountCorrect = this.rowCountCorrect;
        newMap.rowCount = this.rowCount;
        return newMap;
    }

    private static class DenseIterator implements IRowIterator {
        private final IRowIterator baseIterator;
        private final Predicate<Integer> filter;
        private DenseIterator(final IMembershipSet baseMap, final Predicate<Integer> filter) {
            this.baseIterator = baseMap.getIterator();
            this.filter = filter;
        }

        @Override
        public int getNextRow() {
            int tmp = this.baseIterator.getNextRow();
            while (tmp >= 0) {
                if (this.filter.test(tmp))
                    return tmp;
                tmp = this.baseIterator.getNextRow();
            }
            return -1;
        }
    }
}
