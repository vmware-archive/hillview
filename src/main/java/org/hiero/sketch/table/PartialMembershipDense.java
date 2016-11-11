package org.hiero.sketch.table;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.scalactic.exceptions.NullArgumentException;
import java.util.function.Predicate;

/**
 * This implementation uses a full membership data structure plus a list of filter functions that
 * are used for the isMember and for the iterator functions. Upside is that construction is quick,
 * adding a filter function is quick. The downside is that the isMember can take a long time if
 * there are many filters, and iterator takes a long time if it's sparse. Also, each first call for
 * getSize after a new filter is a linear scan.
 */
@SuppressWarnings("ALL")
public class PartialMembershipDense implements IMembershipSet {
    private final IMembershipSet baseMap;
    private int rowCount;
    private boolean rowCountCorrect;
    private final Predicate<Integer> filter;

    public PartialMembershipDense(final IMembershipSet baseMap) throws NullArgumentException {
        if (baseMap == null) throw new NullArgumentException("PartialMembershipDense cannot be " +
                "instantiated without a base MembershipSet");
        this.baseMap = baseMap;
        this.rowCount = baseMap.getSize();
        this.filter = Integer -> true;
        this.rowCountCorrect = false;
    }

    /**
     * instantiated with a a membershipSet, possibly the full one, and a filter predicate.
     * @param baseMap
     * @param filter
     * @throws NullArgumentException
     */
    public PartialMembershipDense(final IMembershipSet baseMap, final Predicate<Integer> filter) throws
            NullArgumentException {
        if (baseMap == null) throw new NullArgumentException("PartialMembershipDense cannot be " +
                "instantiated without a base MembershipSet");
        if (filter == null) throw new NullArgumentException("PartialMembershipDense cannot be " +
                "instantiated with a null filter");
        this.baseMap = baseMap;
        this.rowCount = baseMap.getSize(false);
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
            final IRowIterator IT = this.baseMap.getIterator();
            int tmp = IT.getNextRow();
            while (tmp >= 0) {
                if (this.filter.test(tmp))
                    counter++;
                tmp = IT.getNextRow();
            }
            this.rowCount = counter;
            this.rowCountCorrect = true;
            return this.rowCount;
        }
    }

    @Override
    public int getSize(final boolean exact) {
        if (this.rowCountCorrect)
            return this.rowCount;
        if (exact)
            return this.getSize();
        final IMembershipSet sampleSet = this.sample(20);
        int snumber = 0;
        final IRowIterator it = sampleSet.getIterator();
        int curr = it.getNextRow();
        while (curr >= 0) {
            if (this.filter.test(curr))
                snumber++;
            curr = it.getNextRow();
        }
        return (this.baseMap.getSize(false) * snumber) / sampleSet.getSize(true);
    }

    /**
     * @return A sample of k items from the membership set. The sample is with replacement so may
     * contain less than k distinct elements. The sample is obtained by sampling k items from the
     * base map and filtering it. This is done 10 times, if k samples had been found the function
     * gives up and returns whatever was found. This will happen if the membership is sparse.
     */
    @Override
    public IMembershipSet sample(final int k) {
        int samples = 0;
        IMembershipSet batchSet;
        final IntOpenHashSet sampleSet = new IntOpenHashSet();
        for (int attempt = 0; attempt < 10; attempt++) {
            batchSet = this.baseMap.sample(k);
            final IRowIterator it = batchSet.getIterator();
            int tmprow = it.getNextRow();
            while (tmprow >= 0) {
                if (this.isMember(tmprow)) {
                    sampleSet.add(tmprow);
                    samples++;
                    if (samples == k)
                        return new PartialMembershipSparse(sampleSet);
                }
                tmprow = it.getNextRow();
            }
        }
        return new PartialMembershipSparse(sampleSet);
    }

    /**
     * Samples the base map for k items and then applies the filter on that set. Makes 10 attempts to reach k samples
     * this way and then gives up and returns whatever was sampled.
     * @param k
     * @param seed
     * @return
     */
    @Override
    public IMembershipSet sample(final int k, @SuppressWarnings("LocalCanBeFinal") final long seed) {
        int samples = 0;
        IMembershipSet batchSet;
        final IntOpenHashSet sampleSet = new IntOpenHashSet();;
        for (int attempt = 0; attempt < 10; attempt++) {
            batchSet = this.baseMap.sample(k, seed + attempt);
            final IRowIterator it = batchSet.getIterator();
            int tmprow = it.getNextRow();
            while (tmprow >= 0) {
                if (this.isMember(tmprow)) {
                    sampleSet.add(tmprow);
                    samples++;
                    if (samples == k)
                        return new PartialMembershipSparse(sampleSet);
                }
                tmprow = it.getNextRow();
            }
        }
        return new PartialMembershipSparse(sampleSet);
    }

    @Override
    public IRowIterator getIterator() {
        return new DenseIterator(this.baseMap, this.filter);
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
