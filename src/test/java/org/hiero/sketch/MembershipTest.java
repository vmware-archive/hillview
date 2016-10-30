package org.hiero.sketch;

import org.hiero.sketch.table.FullMembership;
import org.hiero.sketch.table.PartialMembershipDense;
import org.hiero.sketch.table.PartialMembershipSparse;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;
import org.junit.Test;

/*
Test for the three Membership Classes: FullMemebers, PartialMemership{Sparse,Dense}
 */
public class MembershipTest {
    private int size = 10;
    @Test
    public void TestFullMembership() {
        System.out.println("Testing FullMemebership");
        FullMembership FM = new FullMembership(size);
        IRowIterator IT = FM.getIterator();
        int tmp = IT.getNextRow();
        while (tmp >=0 ) {
            System.out.print(tmp+",");
            tmp = IT.getNextRow();
        }
        assert(size == FM.getSize());
        assert(FM.isMember(7));
        assert(!FM.isMember(-2));
        assert(!FM.isMember(-10));
        IMembershipSet mysample = FM.sample(5);
        System.out.println("printing sample of 5 elements: ");
        PrintMemebership(mysample);

    }

    private void PrintMemebership(IMembershipSet myset) {
        IRowIterator it = myset.getIterator();
        int tmp = it.getNextRow();
        while (tmp >=0 ) {
            System.out.print(tmp + ",");
            tmp = it.getNextRow();
        }
    }

    @Test
    public void TestPartialDense() {
        System.out.println("Testing ParialMemebershipDense");
        FullMembership FM = new FullMembership(size);
        PartialMembershipDense PMD = new PartialMembershipDense(FM, row -> {return row%2 == 0;});
        assert(PMD.isMember(6));
        assert(!PMD.isMember(7));
        System.out.println("Size of Dense Membership is: " + PMD.getSize());
        PrintMemebership(PMD);
        System.out.println();
        IMembershipSet mysample = PMD.sample(5);
        System.out.println("printing sample of 5 elements: ");
        PrintMemebership(mysample);
    }

    @Test
    public void TestPartialSparse() {
        System.out.println("Testing ParialMemebershipSparse");
        FullMembership FM = new FullMembership(size);
        PartialMembershipSparse PMS = new PartialMembershipSparse(FM, row -> {return row%2 == 0;});
        assert(PMS.isMember(6));
        assert(!PMS.isMember(7));
        System.out.println("Size of Sparse Membership is: " + PMS.getSize());
        PrintMemebership(PMS);
        System.out.println();
        IMembershipSet mysample = PMS.sample(5);
        System.out.println("printing sample of 5 elements: ");
        PrintMemebership(mysample);
    }
}