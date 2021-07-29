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

import org.hillview.dataset.api.IDataSet;
import org.hillview.sketches.BasicColStatSketch;
import org.hillview.sketches.results.BasicColStats;
import org.hillview.sketches.results.HLogLog;
import org.hillview.table.SmallTable;
import org.hillview.table.Table;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.utils.JsonList;
import org.hillview.utils.Pair;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class BasicStatSketchTest extends BaseTest {
    private static final long seed = 0;

    @Test
    public void StatSketchTest() {
        final int numCols = 1;
        final int tableSize = 1000;
        final Table myTable = TestTables.getRepIntTable(tableSize, numCols);
        final BasicColStatSketch mySketch = new BasicColStatSketch(myTable.getSchema().getColumnNames().get(0), 0, seed);
        JsonList<Pair<BasicColStats, HLogLog>> result = mySketch.create(myTable);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(result.get(0).first.getPresentCount(), 1000);
    }

    @Test
    public void StatSketchTest2() {
        final int numCols = 1;
        final int bigSize = 100000;
        final SmallTable bigTable = TestTables.getIntTable(bigSize, numCols);
        final String colName = bigTable.getSchema().getColumnNames().get(0);

        IDataSet<ITable> all = TestTables.makeParallel(bigTable, bigSize / 10);
        JsonList<Pair<BasicColStats, HLogLog>> result = all.blockingSketch(
                new BasicColStatSketch(colName, 1, seed));
        BasicColStatSketch mySketch = new BasicColStatSketch(
                bigTable.getSchema().getColumnNames().get(0), 1, seed);
        JsonList<Pair<BasicColStats, HLogLog>> result1 = mySketch.create(bigTable);
        Assert.assertNotNull(result);
        Assert.assertNotNull(result1);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result1.size());
        Assert.assertEquals(result.get(0).first.getMoment(1), result1.get(0).first.getMoment(1), 0.001);
        assertTrue(result.get(0).second.distinctItemsEstimator() > 85000);
        assertTrue(result1.get(0).second.distinctItemsEstimator() > 85000);
    }

    @Test
    public void StatSketchTest3() {
        ITable bigTable = TestTables.testTable();
        String colName = bigTable.getSchema().getColumnNames().get(0);

        IDataSet<ITable> all = TestTables.makeParallel(bigTable, 5);
        JsonList<Pair<BasicColStats, HLogLog>> result = all.blockingSketch(
                new BasicColStatSketch(colName, 1, seed));
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("Tom", result.get(0).first.maxString);
        Assert.assertEquals("Bill", result.get(0).first.minString);
    }
}
