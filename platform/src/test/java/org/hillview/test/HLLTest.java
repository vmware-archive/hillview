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

import org.hillview.dataset.ParallelDataSet;
import org.hillview.sketches.*;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.table.columns.IntArrayColumn;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ITable;
import org.hillview.utils.IntArrayGenerator;
import org.hillview.utils.Randomness;
import org.hillview.utils.TestTables;
import org.junit.Test;

import static org.junit.Assert.*;


public class HLLTest extends BaseTest {
    @Test
    public void testHLL() {
        final int size = 2000000;
        final int range = 20000;
        final Randomness rn = this.getRandomness();
        final IntArrayColumn col = IntArrayGenerator.getRandIntArray(size, range, "Test", rn);
        final int accuracy = 14;
        final long seed = 0; //deterministic seed for testing
        final HLogLog hll = new HLogLog(accuracy, seed);
        final FullMembershipSet memSet = new FullMembershipSet(size);
        hll.createHLL(col, memSet);
        long alsoResult = hll.distinctItemCount;
        long result = hll.distinctItemsEstimator();
        assertTrue(alsoResult == result);
        assertTrue((result > (0.9 * range)) && (result < (1.1 * range)));
        assertTrue((result > (0.9 * range)) && (result < (1.1 * range)));
    }

    @Test
    public void testHLLSketch() {
        final int numCols = 1;
        final int bigSize = 100000;
        final SmallTable bigTable = TestTables.getIntTable(bigSize, numCols); //range is 5 * bigSize
        final String colName = bigTable.getSchema().getColumnNames().get(0);
        final ParallelDataSet<ITable> all = TestTables.makeParallel(bigTable, bigSize / 10);
        final HLogLog hll = all.blockingSketch(new HLogLogSketch(colName,16,12345678));
        assertTrue(hll.distinctItemsEstimator() > 85000);
    }
}
