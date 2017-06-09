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
 *
 */

package org.hillview.sketch;

import org.hillview.table.*;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.IRowIterator;
import org.hillview.utils.IntSet;
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
}
