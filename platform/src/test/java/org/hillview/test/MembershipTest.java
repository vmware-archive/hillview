/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.test;

import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IMutableMembershipSet;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ISampledRowIterator;
import org.hillview.table.membership.DenseMembershipSet;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.table.membership.MembershipSetFactory;
import org.hillview.utils.IntSet;
import org.hillview.utils.Randomness;
import org.junit.Test;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;


/* Tests for the three Membership Classes:
 * FullMembership, LazyMembership, SparseMembership
 */
public class MembershipTest extends BaseTest {
    private final int size = 10;

    @Test
    public void TestFullMembership() {
        final FullMembershipSet FM = new FullMembershipSet(this.size);
        final IRowIterator IT = FM.getIterator();
        int tmp = IT.getNextRow();
        while (tmp >= 0) {
            tmp = IT.getNextRow();
        }
        assertEquals(this.size, FM.getSize());
        assertTrue(FM.isMember(7));
        assertFalse(FM.isMember(20));
        final FullMembershipSet FM1 = new FullMembershipSet(10000);
        final IMembershipSet FM2 = FM1.sample(0.1, 0);
        assertTrue(FM2.getSize() > 0.9 * 1000);
        assertTrue(FM2.getSize() < 1.1 * 1000);
        final IMembershipSet FM3 = FM2.sample(0.1, 0);
        assertTrue(FM3.getSize() > 50);
        assertTrue(FM3.getSize() < 150);
    }

    @Test
    public void TestSparseMembership() {
        final FullMembershipSet fm = new FullMembershipSet(this.size);
        final IMembershipSet PMS = fm.filter(row -> (row % 2) == 0);
        assertTrue(PMS.isMember(6));
        assertFalse(PMS.isMember(7));
        assertEquals(PMS.getSize(), 5);
        IMutableMembershipSet mms = MembershipSetFactory.create(fm.getMax(), fm.getSize());
        final IRowIterator IT = PMS.getIterator();
        int tmp = IT.getNextRow();
        while (tmp >= 0) {
            mms.add(tmp);
            tmp = IT.getNextRow();
        }
        assertEquals(PMS.getSize(), mms.seal().getSize());
    }

    @Test
    public void TestMembershipSparse() {
        IMutableMembershipSet mms = MembershipSetFactory.create(100, 10);
        for (int i = 5; i < 100; i += 2)
            mms.add(i);
        IMembershipSet MS = mms.seal();
        final IRowIterator iter = MS.getIterator();
        int tmp = iter.getNextRow();
        final IntSet IS1 = new IntSet();
        while (tmp >= 0) {
            tmp = iter.getNextRow();
            IS1.add(tmp);
        }
        assertTrue(mms.size() == IS1.size());
        final IMembershipSet mySample = MS.sample(20, 0);
        final IRowIterator sIter = mySample.getIterator();
        int curr = sIter.getNextRow();
        while (curr >= 0) {
            curr = sIter.getNextRow();
        }
    }

    @Test
    public void TestDenseMembership() {
        DenseMembershipSet dms = new DenseMembershipSet(10000, 10000);
        for (int i = 0; i < 10000; i++)
            if (i % 7 != 0) dms.add(i);
        IMembershipSet sample = dms.sample(8000, 12345);
        assertEquals(sample.getSize(), 8000);
        IMembershipSet smallSample = dms.sample(40, 12345);
        assertEquals(smallSample.getSize(), 40);
    }

    @Test
    public void TestFMSIterator() {
        FullMembershipSet fm = new FullMembershipSet(1000);
        double rate = 0.1;
        ISampledRowIterator it = fm.getIteratorOverSample(rate, 162545, false);
        int i = 0;
        while (it.getNextRow() >= 0)
            i++;
        assertTrue(i > 1000 * it.rate() * 0.9);
        assertTrue(i < it.rate() * 1000 * 1.1);
    }

    @Test
    public void TestSparseSampleIterator() {
        IMutableMembershipSet mms = MembershipSetFactory.create(100, 10);
        for (int i = 5; i < 100; i += 2)
            mms.add(i);
        IMembershipSet MS = mms.seal();
        double rate = 0.5;
        ISampledRowIterator iter = MS.getIteratorOverSample(rate, 12345, false);
        int i = 0;
        while (iter.getNextRow() >= 0)
            i++;
        assertTrue(i < 1.2 * 48 * iter.rate());
        assertTrue(i > 0.8 * 48 * iter.rate());
    }

    @Test
    public void TestDenseSampledIterator() {
        DenseMembershipSet dms = new DenseMembershipSet(10000, 10000);
        for (int i = 0; i < 10000; i++)
            if (i % 7 != 0) dms.add(i);
        double rate = 0.2;
        ISampledRowIterator iter = dms.getIteratorOverSample(rate, 123, false);
        int counter = 0;
        while (iter.getNextRow() > 0)
            counter++;
        assertTrue( counter > 0.9 * iter.rate() * dms.getSize());
        assertTrue( counter < 1.1 * iter.rate() * dms.getSize());
    }
}
