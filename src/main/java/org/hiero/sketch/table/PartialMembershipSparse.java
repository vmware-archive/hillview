package org.hiero.sketch.table;

import it.unimi.dsi.fastutil.ints.*;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.scalactic.exceptions.NullArgumentException;
import java.util.*;
import java.util.function.Predicate;
import it.unimi.dsi.*;

/**
 * This implementation uses a Set data structure to store the membership. It uses the Set's
 * membership and iterator methods. The upside is that it is efficient in space and that the
 * iterator is very efficient. So this implementation is best when the set is sparse.
 * The downside is the constructor that runs in linear time.
 */
public class PartialMembershipSparse implements IMembershipSet {

    private int rowCount;
    private IntOpenHashSet membershipMap;
    /**
     * Standard way to construct this map is by supplying a membershipSet (perhaps the full one),
     * and the filter function passed as a lambda expression.
     * @param baseMap
     * @param filter
     * @throws NullArgumentException
     */
    public PartialMembershipSparse(IMembershipSet baseMap, Predicate<Integer> filter)
            throws NullArgumentException {
        if (baseMap == null)
            throw new NullArgumentException("PartialMembershipDense cannot be instantiated " +
                    "without a base MembershipSet");
        if (filter == null)
            throw new NullArgumentException("Predicate for PartialMembershipDense cannot be null");
        IRowIterator baseIterator = baseMap.getIterator();
        this.membershipMap = new IntOpenHashSet(estimateSize(baseMap, filter));
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
     * the basemap into that of a sparse map.
     * @param baseMap of type ImembershipSet
     * @throws NullArgumentException
     */
    public PartialMembershipSparse(IMembershipSet baseMap) throws NullArgumentException {
        if (baseMap == null)
            throw new NullArgumentException("PartialMembershipDense cannot be instantiated " +
                    "without a base MembershipSet");
        IRowIterator baseIterator = baseMap.getIterator();
        this.membershipMap = new IntOpenHashSet(baseMap.getSize(false));
        int tmp = baseIterator.getNextRow();
        while (tmp >= 0) {
            this.membershipMap.add(tmp);
            tmp = baseIterator.getNextRow();
        }
        this.rowCount = this.membershipMap.size();
    }

    /**
     * Essentially wraps a Set interface by ImembershipSet
     * @param baseSet of type Set
     * @throws NullArgumentException
     */
    public PartialMembershipSparse(IntOpenHashSet baseSet) throws NullArgumentException {
        if (baseSet == null)
            throw new NullArgumentException("PartialMembershipDense cannot be instantiated " +
                    "without a base Set");
        this.membershipMap = baseSet;
        this.rowCount = this.membershipMap.size();
    }

    @Override
    public boolean isMember(int rowIndex) {
        return this.membershipMap.contains(rowIndex);
    }

    @Override
    public int getSize() {
        return this.rowCount;
    }

    @Override
    public int getSize(boolean exact) { return this.rowCount; }

    /**
     * Returns the k items from a random location in the map. Note that the k items are not completely idependent
     * but also depend on the placement done by the hash function of IntOpenHashSet
     * todo: Currently iter.skip(n) takes O(n). Could be made O(1) but requires changing the library
     */
    @Override
    public IMembershipSet sample(int k) {
        IntOpenHashSet sampleSet = new IntOpenHashSet(k);
        Random psg = new Random();
        int offset = psg.nextInt(Integer.max(this.rowCount - k + 1, 1));
        IntIterator iter = this.membershipMap.iterator();
        iter.skip(offset - 1);
        int tmp;
        for (int i = 0; i < k; i++ ) {
            if (iter.hasNext()) {
                tmp = iter.nextInt();
                sampleSet.add(tmp);
            }
        }
        return new PartialMembershipSparse(sampleSet);
    }

    /**
     * Returns the k items from a random location in the map. The random location determined by a seed.
     * Note that the k items are not completely independent but also depend on the placement done by
     * the hash function of IntOpenHashSet.
     * todo: Currently iter.skip(n) takes O(n). Could be made O(1) but requires changing the library
     */
    @Override
    public IMembershipSet sample(int k, long seed) {
        IntOpenHashSet sampleSet = new IntOpenHashSet(k);
        Random psg = new Random(seed);
        int offset = psg.nextInt(Integer.max(this.rowCount - k + 1, 1));
        IntIterator iter = this.membershipMap.iterator();
        iter.skip(offset - 1);
        int tmp;
        for (int i = 0; i < k; i++ ) {
            if (iter.hasNext()) {
                tmp = iter.nextInt();
                sampleSet.add(tmp);
            }
        }
        return new PartialMembershipSparse(sampleSet);
    }

    @Override
    public IRowIterator getIterator() {
        return new SparseIterator(this.membershipMap);
    }

    private int estimateSize(IMembershipSet baseMap, Predicate<Integer> filter) {
        IMembershipSet sampleSet = baseMap.sample(20);
        int esize = 0;
        IRowIterator iter= sampleSet.getIterator();
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
        private IntOpenHashSet membershipMap;
        private IntIterator myIterator;

        public SparseIterator(IntOpenHashSet membershipMap) {
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

