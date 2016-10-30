package org.hiero.sketch.table;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.scalactic.exceptions.NullArgumentException;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * This implementation uses a full membership data structure plus a list of filter functions that
 * are used for the isMemeber and for the iterator functions. Upside is that construction is quick,
 * adding a filter function is quick. The downside is that the isMemeber can take a long time if
 * there are many filters, and iterator takes a long time if it's sparse. Also, each first call for
 * getSize after a new filter is a linear scan.
 */
public class PartialMembershipDense implements IMembershipSet {
    private IMembershipSet baseMap;
    private int rowCount;
    private boolean rowCountCorrect;
    private Predicate<Integer> filter;

    public PartialMembershipDense(IMembershipSet baseMap) throws NullArgumentException {
        if (baseMap == null) throw new NullArgumentException("PartialMembershipDense cannot be " +
                "instantiated without a base MembershipSet");
        this.baseMap = baseMap;
        this.rowCount = baseMap.getSize();
        this.filter = Integer -> { return true; };
        this.rowCountCorrect = false;
    }

    /* class is instantiated by supplying a membershipSet (perhaps the full one), and a filter
    function. If a filter is not supplied it is defaulted to be true */
    public PartialMembershipDense(IMembershipSet baseMap, Predicate<Integer> filter) throws
            NullArgumentException {
        if (baseMap == null) throw new NullArgumentException("PartialMembershipDense cannot be " +
                "instantiated without a base MembershipSet");
        this.baseMap = baseMap;
        this.rowCount = baseMap.getSize();
        if (filter == null)
            filter = Integer -> true;
        this.filter = filter;
        this.rowCountCorrect = false;
    }

    @Override
    public boolean isMember(int rowIndex) {
        return ( baseMap.isMember(rowIndex) && filter.test(rowIndex) );
    }

    @Override
    public int getSize() {
        if (rowCountCorrect) return this.rowCount;
        else {
            int counter = 0;
            IRowIterator IT = this.baseMap.getIterator();
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

    /**
     * @return A sample of k items from the membership set. The sample is with replacement so may
     * contain less than k distinct elements. The sample is obtained by sampling k items from the
     * base map and filtering it. This is done 10 times, if k samples had been found the function
     * gives up and returns whatever was found. This will happen if the membership is sparse.
     */
    @Override
    public IMembershipSet sample(int k) {
        int samples = 0;
        IMembershipSet batchSet;
        IntOpenHashSet sampleSet = new IntOpenHashSet();
        for (int attempt = 0; attempt < 10; attempt++) {
            batchSet = this.baseMap.sample(k);
            IRowIterator it = batchSet.getIterator();
            int tmprow = it.getNextRow();
            while (tmprow >= 0) {
                if (isMember(tmprow)) {
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
    public IMembershipSet sample(int k, long seed) {
        int samples = 0;
        IMembershipSet batchSet;
        IntOpenHashSet sampleSet = new IntOpenHashSet();;
        for (int attempt = 0; attempt < 10; attempt++) {
            batchSet = this.baseMap.sample(k, seed + attempt);
            IRowIterator it = batchSet.getIterator();
            int tmprow = it.getNextRow();
            while (tmprow >= 0) {
                if (isMember(tmprow)) {
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
        private IRowIterator baseIterator;
        private Predicate<Integer> filter;
        public DenseIterator(IMembershipSet baseMap, Predicate<Integer> filter) {
            this.baseIterator = baseMap.getIterator();
            this.filter = filter;
        }

        @Override
        public int getNextRow() {
            int tmp = baseIterator.getNextRow();
            while (tmp >= 0) {
                if (this.filter.test(tmp))
                    return tmp;
                tmp = this.baseIterator.getNextRow();
            }
            return -1;
        }
    }
}
