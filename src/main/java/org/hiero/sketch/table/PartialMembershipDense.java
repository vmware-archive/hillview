package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.scalactic.exceptions.NullArgumentException;
import java.util.function.Predicate;


/**
 * Created by uwieder on 10/20/16.
 *
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

    /* class is instantiated by supplying a memshipSet (perhaps the full one), and a filter
    function. If a filter is not supplied it is defaulted to be true
    */
    public PartialMembershipDense(IMembershipSet baseMap) throws NullArgumentException {
        if (baseMap == null) throw new NullArgumentException("PartialMembershipDense cannot be " +
                "instantiated without a base MembershipSet");
        this.baseMap = baseMap;
        this.rowCount = baseMap.getSize();
        this.filter = Integer -> { return true; };
        this.rowCountCorrect = false;
    }

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
            while (tmp >=0 ) {
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
        public int getNextRow(){
            int tmp = baseIterator.getNextRow();
            while (tmp >= 0){
                if (filter.test(tmp))
                    return tmp;
                tmp = baseIterator.getNextRow();
            }
            return -1;
        }
    }
}
