package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.scalactic.exceptions.NullArgumentException;
import java.util.*;
import java.util.function.Predicate;

/**
 * This implementation uses a Set data structure to store the membership. It uses the Set's
 * membership and iterator methods. The upside is that it is efficient in space and that the
 * iterator is very efficient. So this implementation is best when the set is sparse.
 * The downside is the constructor that runs in linear time.
 *
 * TODO: Use a Set that is specialized for integers, instead of a generic.
 */
public class PartialMembershipSparse implements IMembershipSet {

    private final int rowCount;
    private final Set<Integer> membershipMap;

    /* Standard way to construct this map is by supplying a membershipSet (perhaps the full one),
    and the filter function passed as a lambda expression*/
    public PartialMembershipSparse(final IMembershipSet baseMap, Predicate<Integer> filter)
            throws NullArgumentException {
        if (baseMap == null)
            throw new NullArgumentException("PartialMembershipDense cannot be instantiated " +
                    "without a base MembershipSet");
        if (filter == null)
            filter = Integer -> true;
        final IRowIterator baseIterator = baseMap.getIterator();
        this.membershipMap = new HashSet<Integer>();
        int tmp = baseIterator.getNextRow();
        while (tmp >= 0) {
            if (filter.test(tmp))
                this.membershipMap.add(tmp);
            tmp = baseIterator.getNextRow();
        }
        this.rowCount = this.membershipMap.size();
    }

    public PartialMembershipSparse(final IMembershipSet baseMap) throws NullArgumentException {
        if (baseMap == null)
            throw new NullArgumentException("PartialMembershipDense cannot be instantiated " +
                    "without a base MembershipSet");
        final IRowIterator baseIterator = baseMap.getIterator();
        this.membershipMap = new HashSet<Integer>();
        int tmp = baseIterator.getNextRow();
        while (tmp >= 0) {
            this.membershipMap.add(tmp);
            tmp = baseIterator.getNextRow();
        }
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
    public IRowIterator getIterator() {
        return new SparseIterator(this.membershipMap);
    }

    // Implementing the Iterator
    private static class SparseIterator implements IRowIterator {
        private final Set<Integer> mempershipMap;
        private final Iterator<Integer> myIterator;

        private SparseIterator(final Set<Integer> membershipMap) {
            this.mempershipMap = membershipMap;
            this.myIterator = membershipMap.iterator();
        }

        public int getNextRow() {
            if (this.myIterator.hasNext())
                return this.myIterator.next();
            return -1;
        }
    }
}

