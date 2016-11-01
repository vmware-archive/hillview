package org.hiero.sketch;

import org.hiero.sketch.table.FullMembership;
import org.hiero.sketch.table.PartialMembershipDense;
import org.hiero.sketch.table.PartialMembershipSparse;
import org.hiero.sketch.table.api.IRowIterator;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

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
        final PartialMembershipDense PMD = new PartialMembershipDense(FM, row -> (row % 2) == 0);
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
        final PartialMembershipSparse PMS = new PartialMembershipSparse(FM, row -> (row % 2) == 0);
        assertTrue(PMS.isMember(6));
        assertFalse(PMS.isMember(7));
        assertEquals(PMS.getSize(), 5);
        final IRowIterator IT = PMS.getIterator();
        int tmp = IT.getNextRow();
        while (tmp >= 0)
            tmp = IT.getNextRow();
    }
}