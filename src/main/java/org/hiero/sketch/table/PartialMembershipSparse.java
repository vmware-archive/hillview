package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.scalactic.exceptions.NullArgumentException;
import java.util.*;
import java.util.function.Predicate;


/**
 * Created by uwieder on 10/20/16.
 *
 * This implementation uses a Set data structure to store the membership. It uses the Set's
 * membership and iterator methods. The upside is that it is efficient in space and that the
 * iterator is very efficient. So this implementation is best when the set is sparse.
 * The downside is the constructor that runs in linear time.
 *
 * TODO: Use a Set that is specialized for integers, instead of a generic.
 */
public class PartialMembershipSparse implements IMembershipSet {

    private int rowCount;
    private Set<Integer> membershipMap;

    /* Standard way to construct this map is by supplying a memshipSet (perhaps the full one),
    and the filter function passed as a lambda expression*/
    public PartialMembershipSparse(IMembershipSet baseMap, Predicate<Integer> filter)
            throws NullArgumentException {
        if (baseMap == null)
            throw new NullArgumentException("PartialMembershipDense cannot be instantiated " +
                    "without a base MembershipSet");
        if (filter == null)
            filter = Integer -> { return true; };
        IRowIterator baseIterator = baseMap.getIterator();
        membershipMap = new HashSet<Integer>();
        int tmp = baseIterator.getNextRow();
        while(tmp >= 0) {
            if (filter.test(tmp))
                membershipMap.add(tmp);
            tmp = baseIterator.getNextRow();
        }
        this.rowCount = membershipMap.size();
    }

    public PartialMembershipSparse(IMembershipSet baseMap) throws NullArgumentException {
        if (baseMap == null)
            throw new NullArgumentException("PartialMembershipDense cannot be instantiated " +
                    "without a base MembershipSet");
        IRowIterator baseIterator = baseMap.getIterator();
        membershipMap = new HashSet<Integer>();
        int tmp = baseIterator.getNextRow();
        while(tmp >= 0) {
            membershipMap.add(tmp);
            tmp = baseIterator.getNextRow();
        }
        this.rowCount = membershipMap.size();
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
    public IRowIterator getIterator() {
        return new sparseIterator(membershipMap);
    }


    // Implementing the Iterator
    private static class sparseIterator implements IRowIterator {
        private Set<Integer> mempershipMap;
        private Iterator<Integer> myIterator;

        public sparseIterator(Set<Integer> membershipMap) {
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

