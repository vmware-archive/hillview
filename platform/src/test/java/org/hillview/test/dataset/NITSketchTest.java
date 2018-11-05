/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

package org.hillview.test.dataset;

import org.hillview.dataset.ParallelDataSet;
import org.hillview.sketches.NIThresholdSketch;
import org.hillview.sketches.NumItemsThreshold;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.IntArrayColumn;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.test.BaseTest;
import org.hillview.utils.IntArrayGenerator;
import org.hillview.utils.Randomness;
import org.hillview.utils.TestTables;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class NITSketchTest extends BaseTest {
    @Test
    public void testNIT() {
        final int size = 2000000;
        final int range = 8700;
        final Randomness rn = this.getRandomness();
        final IntArrayColumn col = IntArrayGenerator.getRandIntArray(size, range, "Test", rn);
        final int accuracy = 14;
        final long seed = 0; // deterministic seed for testing
        final NumItemsThreshold nit = new NumItemsThreshold(13, 123456);
        final FullMembershipSet memSet = new FullMembershipSet(size);
        nit.createBits(col, memSet);
        assertTrue(nit.exceedThreshold());
    }

    @Test
    public void testNITSketch() {
        final int numCols = 1;
        final int bigSize = 100000;
        final SmallTable bigTable = TestTables.getIntTable(bigSize, numCols, 8700);
        final String colName = bigTable.getSchema().getColumnNames().get(0);
        final ParallelDataSet<ITable> all = TestTables.makeParallel(bigTable, bigSize / 10);
        final NumItemsThreshold nit = all.blockingSketch(new NIThresholdSketch(colName, 13, 12345678));
        assertTrue(nit.exceedThreshold());

        final SmallTable bigTable1 = TestTables.getIntTable(bigSize, numCols, 8000);
        final String colName1= bigTable1.getSchema().getColumnNames().get(0);
        final ParallelDataSet<ITable> all1 = TestTables.makeParallel(bigTable1, bigSize / 10);
        final NumItemsThreshold nit1 = all1.blockingSketch(new NIThresholdSketch(colName1,13,12345678));
        assertTrue(!nit1.exceedThreshold());

        final SmallTable bigTable2 = TestTables.getIntTable(bigSize, numCols, 150);
        final String colName2= bigTable2.getSchema().getColumnNames().get(0);
        final ParallelDataSet<ITable> all2 = TestTables.makeParallel(bigTable2, bigSize / 10);
        final NumItemsThreshold nit2 = all2.blockingSketch(new NIThresholdSketch(colName2, 7, 12345678));
        assertTrue(nit2.exceedThreshold());
    }

}
