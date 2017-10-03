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

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.sketches.FreqKList;
import org.hillview.sketches.FreqKSketch;
import org.hillview.table.HashSubSchema;
import org.hillview.table.SmallTable;
import org.hillview.table.Table;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class FreqKTest extends BaseTest {
    private void filterTest(@Nullable FreqKList fkList) {
        Converters.checkNull(fkList);
        fkList.filter(Boolean.TRUE);
        fkList.getList().forEach(rss -> assertTrue(fkList.hMap.get(rss) >=
                fkList.totalRows*fkList.epsilon - fkList.getErrBound()));
    }
    @Test
    public void testTopK1() {
        final int numCols = 2;
        final double epsilon = 0.01;
        final int size = 1000;
        Table leftTable = TestTables.getRepIntTable(size, numCols);
        FreqKSketch fk = new FreqKSketch(leftTable.getSchema(), epsilon);
        FreqKList fkList= fk.create(leftTable);
        filterTest(fkList);

    }

    @Test
    public void testTopKSq() {
        final int range = 15;
        double eps = 0.2;
        SmallTable leftTable = TestTables.getSqIntTable(range);
        FreqKSketch fk = new FreqKSketch(leftTable.getSchema(), eps);
        FreqKList fkList= fk.create(leftTable);
        filterTest(fkList);
    }

    @Test
    public void testTopK2() {
        final int numCols = 2;
        final double epsilon = 0.1;
        final int size = 1000;
        Table leftTable = TestTables.getRepIntTable(size, numCols);
        Table rightTable = TestTables.getRepIntTable(size, numCols);
        FreqKSketch fk = new FreqKSketch(leftTable.getSchema(), epsilon);
        FreqKList fkList = fk.add(fk.create(leftTable), fk.create(rightTable));
        filterTest(fkList);
    }

    @Test
    public void testTopK3() {
        final int numCols = 2;
        final double epsilon = 0.04;
        final double base = 2.0;
        final int range = 14;
        final int size = 20000;
        SmallTable leftTable = TestTables.getHeavyIntTable(numCols, size, base, range);
        FreqKSketch fk = new FreqKSketch(leftTable.getSchema(), epsilon);
        FreqKList fkList= fk.create(leftTable);
        filterTest(fkList);
    }

    @Test
    public void testTopK4() {
        final int numCols = 2;
        final double epsilon = 0.02;
        final double base = 2.0;
        final int range = 10;
        final int size = 20000;
        SmallTable leftTable = TestTables.getHeavyIntTable(numCols, size, base, range);
        SmallTable rightTable = TestTables.getHeavyIntTable(numCols, size, base, range);
        FreqKSketch fk = new FreqKSketch(leftTable.getSchema(), epsilon);
        FreqKList freqKList = Converters.checkNull(fk.add(fk.create(leftTable),
                fk.create(rightTable)));
        filterTest(freqKList);
    }

    @Test
    public void testTopK5() {
        final int numCols = 2;
        final double epsilon = 0.04;
        final double base = 2.0;
        final int range = 16;
        final int size = 100000;
        SmallTable bigTable = TestTables.getHeavyIntTable(numCols, size, base, range);
        List<ITable> tabList = TestTables.splitTable(bigTable, 1000);
        ArrayList<IDataSet<ITable>> a = new ArrayList<IDataSet<ITable>>();
        tabList.forEach(t -> a.add(new LocalDataSet<ITable>(t)));
        ParallelDataSet<ITable> all = new ParallelDataSet<ITable>(a);
        FreqKSketch fk = new FreqKSketch(bigTable.getSchema(), epsilon);
        Assert.assertNotNull(all.blockingSketch(fk).toString());
    }

    @Test
    public void testTopK6() {
        Table t = TestTables.testRepTable();
        HashSubSchema hss = new HashSubSchema();
        hss.add("Age");
        FreqKSketch fk = new FreqKSketch(t.getSchema().project(hss), 5);
        String s = "10: (3-4)\n20: (3-4)\n30: (2-3)\n40: (1-2)\nError bound: 1\n";
        //Assert.assertEquals(fk.create(t).toString(), s);
    }
}
