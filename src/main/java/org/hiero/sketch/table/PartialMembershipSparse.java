package org.hiero.sketch.table;

import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenCustomHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
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
 *
 * TODO: Use a Set that is specialized for integers, instead of a generic.
 */
public class PartialMembershipSparse implements IMembershipSet {

    private int rowCount;
    private Set membershipMap;
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
        this.membershipMap = new IntOpenHashSet();
        int tmp = baseIterator.getNextRow();
        while (tmp >= 0) {
            if (filter.test(tmp))
                this.membershipMap.add(tmp);
            tmp = baseIterator.getNextRow();
        }
        this.rowCount = this.membershipMap.size();
    }

    /**
     * Instantiates the class without a new predicate. Effectively converts the implemenation of
     * the basemap into that of a sparse map.
     * @param baseMap of type ImembershipSet
     * @throws NullArgumentException
     */
    public PartialMembershipSparse(IMembershipSet baseMap) throws NullArgumentException {
        if (baseMap == null)
            throw new NullArgumentException("PartialMembershipDense cannot be instantiated " +
                    "without a base MembershipSet");
        IRowIterator baseIterator = baseMap.getIterator();
        this.membershipMap = new IntOpenHashSet();
        int tmp = baseIterator.getNextRow();
        while (tmp >= 0) {
            this.membershipMap.add(tmp);
            tmp = baseIterator.getNextRow();
        }
        this.rowCount = this.membershipMap.size();
    }

    /**
     * Essentially wraps a Set interface by  ImembershipSet
     * @param baseSet of type Set
     * @throws NullArgumentException
     */
    public PartialMembershipSparse(Set baseSet) throws NullArgumentException {
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

    /** TODO This is a PLACEHOLDER
     * Returns the first k items from the map. Actually may make sense dependeing on the
     * implementation of the OpenHashSet
     */
    @Override
    public IMembershipSet sample(int k) {
        Set<Integer> sampleSet = new IntOpenHashSet();
        IRowIterator it = this.getIterator();
        for (int i = 0; i < k; i++ ) {
            int tmp = it.getNextRow();
            if (tmp < 0)
                return new PartialMembershipSparse(sampleSet);
            sampleSet.add(tmp);
        }
        return new PartialMembershipSparse(sampleSet);
    }

    /** TODO This is a PLACEHOLDER
     * Returns the first k items from the map. Actually may make sense dependeing on the
     * implementation of the OpenHashSet, though not clear how to pass the seed.
     */
    @Override
    public IMembershipSet sample(int k, long seed) {
        Set<Integer> sampleSet = new IntOpenHashSet();
        IRowIterator it = this.getIterator();
        for (int i = 0; i < k; i++ ) {
            int tmp = it.getNextRow();
            if (tmp < 0)
                return new PartialMembershipSparse(sampleSet);
            sampleSet.add(tmp);
        }
        return new PartialMembershipSparse(sampleSet);
    }

    @Override
    public IRowIterator getIterator() {
        return new SparseIterator(this.membershipMap);
    }

    // Implementing the Iterator
    private static class SparseIterator implements IRowIterator {
        private Set<Integer> membershipMap;
        private Iterator<Integer> myIterator;

        public SparseIterator(Set<Integer> membershipMap) {
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

