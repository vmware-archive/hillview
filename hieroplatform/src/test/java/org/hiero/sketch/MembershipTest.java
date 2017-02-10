package org.hiero.sketch;

import org.hiero.sketch.table.*;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.utils.IntSet;
import org.junit.Test;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;


/* Tests for the three Membership Classes:
 * FullMembership, LazyMembership, SparseMembership
 */
public class MembershipTest {
    private final int size = 10;

    @Test
    public void TestFullMembership() {
        final FullMembership FM = new FullMembership(this.size);
        final IRowIterator IT = FM.getIterator();
        int tmp = IT.getNextRow();
        while (tmp >= 0) {
            tmp = IT.getNextRow();
        }
        assertEquals(this.size, FM.getSize());
        assertTrue(FM.isMember(7));
        assertFalse(FM.isMember(20));
        final FullMembership FM1 = new FullMembership(1000);
        final IMembershipSet FM2 = FM1.sample(0.1);
        assertEquals(100, FM2.getSize());
        final IMembershipSet FM3 = FM2.sample(0.1);
        assertEquals(10, FM3.getSize());
    }

    @Test
    public void TestLazyMembership() {
        final FullMembership FM = new FullMembership(this.size);
        final LazyMembership PMD = new LazyMembership(FM, row -> (row % 2) == 0);
        assertTrue(PMD.isMember(6));
        assertFalse(PMD.isMember(7));
        assertEquals(PMD.getSize(), 5);
        final IRowIterator IT = PMD.getIterator();
        int tmp = IT.getNextRow();
        while (tmp >= 0) {
            tmp = IT.getNextRow();
        }
        final FullMembership FM1 = new FullMembership(1000);
        final LazyMembership LM = new LazyMembership(FM1, row -> (row % 2) == 0);
        assertEquals(500, LM.getSize());
        LM.sample(0.1);
    }

    @Test
    public void TestSparseMembership() {
        final FullMembership FM = new FullMembership(this.size);
        final SparseMembership PMS = new SparseMembership(FM, row -> (row % 2) == 0);
        assertTrue(PMS.isMember(6));
        assertFalse(PMS.isMember(7));
        assertEquals(PMS.getSize(), 5);
        final IntSet testSet = new IntSet();
        final IRowIterator IT = PMS.getIterator();
        int tmp = IT.getNextRow();
        while (tmp >= 0) {
            testSet.add(tmp);
            tmp = IT.getNextRow();
        }
        final SparseMembership PMS1 = new SparseMembership(testSet);
        assertEquals(PMS.getSize(), PMS1.getSize());
    }

    @Test
    public void TestMembershipSparse() {
        final IntSet IS = new IntSet(10);
        for (int i = 5; i < 100; i += 2)
            IS.add(i);
        final SparseMembership MS = new SparseMembership(IS);
        final IRowIterator iter = MS.getIterator();
        int tmp = iter.getNextRow();
        final IntSet IS1 = new IntSet();
        while (tmp >= 0) {
            tmp = iter.getNextRow();
            IS1.add(tmp);
        }
        assertTrue(IS.size() == IS1.size());
        final IMembershipSet mySample = MS.sample(20);
        final IRowIterator siter = mySample.getIterator();
        int curr = siter.getNextRow();
        while (curr >= 0) {
            curr = siter.getNextRow();
        }
    }

    @Test
    public void TestUnion() {
        final IntSet IS = new IntSet(10);
        for (int i = 5; i < 100; i += 2)
            IS.add(i);
        final SparseMembership MS = new SparseMembership(IS);
        final FullMembership FM = new FullMembership(60);
        final IMembershipSet UnionSet = FM.union(MS);
        assertTrue(UnionSet.isMember(67));
        assertTrue(UnionSet.isMember(38));
        assertFalse(UnionSet.isMember(68));
        final LazyMembership MD = new LazyMembership(FM, p -> (p % 2) == 1);
        assertTrue(MD.isMember(35));
        assertFalse(MD.isMember(36));
        final LazyMembership MD1 = new LazyMembership(FM, p -> (p % 3) == 0);
        assertTrue(MD1.isMember(36));
        assertFalse(MD1.isMember(37));
        final IMembershipSet UnionSet1 = MD.union(MD1);
        assertTrue(UnionSet1.isMember(36));
        assertFalse(UnionSet1.isMember(8));
        final IMembershipSet LazyFull = MD.union(FM);
        assertTrue(LazyFull.isMember(50));
    }

    @Test
    public void TestIntersect() {
        final IntSet IS = new IntSet(10);
        for (int i = 5; i < 100; i += 2)
            IS.add(i);
        final SparseMembership MS = new SparseMembership(IS);
        final FullMembership FM = new FullMembership(60);
        final IMembershipSet IntersectSet = FM.intersection(MS);
        assertFalse(IntersectSet.isMember(67));
        assertFalse(IntersectSet.isMember(38));
        assertTrue(IntersectSet.isMember(17));
        final LazyMembership MD = new LazyMembership(FM, p -> (p % 2) == 1);
        assertTrue(MD.isMember(35));
        assertFalse(MD.isMember(36));
        final LazyMembership MD1 = new LazyMembership(FM, p -> (p % 3) == 0);
        assertTrue(MD1.isMember(36));
        assertFalse(MD1.isMember(37));
        final IMembershipSet IntersectSet1 = MD.intersection(MD1);
        assertFalse(IntersectSet1.isMember(36));
        assertTrue(IntersectSet1.isMember(9));
    }

    @Test
    public void TestSetMinus() {
        final IntSet IS = new IntSet(10);
        for (int i = 5; i < 100; i += 2)
            IS.add(i);
        final SparseMembership MS = new SparseMembership(IS);
        final FullMembership FM = new FullMembership(60);
        final IMembershipSet SetMinusSet = FM.setMinus(MS);
        assertFalse(SetMinusSet.isMember(67));
        assertTrue(SetMinusSet.isMember(38));
        assertFalse(SetMinusSet.isMember(13));
        final LazyMembership MD = new LazyMembership(FM, p -> (p % 2) == 1);
        assertTrue(MD.isMember(35));
        assertFalse(MD.isMember(36));
        final LazyMembership MD1 = new LazyMembership(FM, p -> (p % 3) == 0);
        assertTrue(MD1.isMember(36));
        assertFalse(MD1.isMember(37));
        final IMembershipSet SetMinusSet1 = MD.setMinus(MD1);
        assertTrue(SetMinusSet1.isMember(19));
        assertFalse(SetMinusSet1.isMember(21));
    }
}
