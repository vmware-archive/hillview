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
import org.hillview.sketches.*;
import org.hillview.sketches.results.BasicColStats;
import org.hillview.test.BaseTest;
import org.hillview.utils.JsonList;
import org.hillview.utils.TestTables;
import org.hillview.table.SmallTable;
import org.hillview.table.Table;
import org.hillview.table.api.ITable;
import org.junit.Assert;
import org.junit.Test;

public class BasicStatSketchTest extends BaseTest {
    @Test
    public void StatSketchTest() {
        final int numCols = 1;
        final int tableSize = 1000;
        final Table myTable = TestTables.getRepIntTable(tableSize, numCols);
        final BasicColStatSketch mySketch = new BasicColStatSketch(
                myTable.getSchema().getColumnNames().get(0),
                0);
        JsonList<BasicColStats> result = mySketch.create(myTable);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(result.get(0).getPresentCount(), 1000);
    }

    @Test
    public void StatSketchTest2() {
        final int numCols = 1;
        final int bigSize = 100000;
        final SmallTable bigTable = TestTables.getIntTable(bigSize, numCols);
        final String colName = bigTable.getSchema().getColumnNames().get(0);

        IDataSet<ITable> all = TestTables.makeParallel(bigTable, bigSize / 10);
        JsonList<BasicColStats> result = all.blockingSketch(
                new BasicColStatSketch(colName, 1));
        BasicColStatSketch mySketch = new BasicColStatSketch(
                bigTable.getSchema().getColumnNames().get(0), 1);
        JsonList<BasicColStats> result1 = mySketch.create(bigTable);
        Assert.assertNotNull(result);
        Assert.assertNotNull(result1);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(1, result1.size());
        Assert.assertEquals(result.get(0).getMoment(1), result1.get(0).getMoment(1), 0.001);
    }

    @Test
    public void StatSketchTest3() {
        ITable bigTable = TestTables.testTable();
        String colName = bigTable.getSchema().getColumnNames().get(0);

        IDataSet<ITable> all = TestTables.makeParallel(bigTable, 5);
        JsonList<BasicColStats> result = all.blockingSketch(
                new BasicColStatSketch(colName, 1));
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("Tom", result.get(0).maxString);
        Assert.assertEquals("Bill", result.get(0).minString);
    }
}
