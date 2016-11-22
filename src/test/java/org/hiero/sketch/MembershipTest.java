package org.hiero.sketch;

import org.hiero.sketch.table.FullMembership;
import org.hiero.sketch.table.LazyMembership;
import org.hiero.sketch.table.SparseMembership;
import org.hiero.sketch.table.api.IRowIterator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/* Tests for the three Membership Classes:
 * FullMembership, PartialMembershipSparse, PartialMembershipDense
 */
public class MembershipTest {
    private final int size = 10;

    @Test
    public void TestFullMembership() {
        final FullMembership FM = new FullMembership(this.size);
        final IRowIterator IT = FM.getIterator();
        int tmp = IT.getNextRow();
        while (tmp >= 0) {
            //System.out.print(tmp+",");
            tmp = IT.getNextRow();
        }
        assertEquals(this.size, FM.getSize());
        assertTrue(FM.isMember(7));
        assertFalse(FM.isMember(20));
    }

    @Test
    public void TestPartialDense() {
        final FullMembership FM = new FullMembership(this.size);
        final LazyMembership PMD = new LazyMembership(FM, row -> (row % 2) == 0);
        assertTrue(PMD.isMember(6));
        assertFalse(PMD.isMember(7));
        assertEquals(PMD.getSize(), 5);
        final IRowIterator IT = PMD.getIterator();
        int tmp = IT.getNextRow();
        while (tmp >= 0) {
            //System.out.print(tmp+",");
            tmp = IT.getNextRow();
        }
    }

    @Test
    public void TestPartialSparse() {
        final FullMembership FM = new FullMembership(this.size);
        final SparseMembership PMS = new SparseMembership(FM, row -> (row % 2) == 0);
        assertTrue(PMS.isMember(6));
        assertFalse(PMS.isMember(7));
        assertEquals(PMS.getSize(), 5);
        final IRowIterator IT = PMS.getIterator();
        int tmp = IT.getNextRow();
        while (tmp >= 0)
            tmp = IT.getNextRow();
    }
}
