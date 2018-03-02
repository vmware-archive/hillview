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

import junit.framework.TestCase;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.sketches.*;
import org.hillview.table.HashSubSchema;
import org.hillview.table.Schema;
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

    public void checkList(FreqKList fkList, Schema schema) {
        NextKList nkList = fkList.getTop(schema);
        for (int i = 1; i < Converters.checkNull(nkList.count).size(); i++) {
            TestCase.assertTrue(nkList.count.get(i - 1) >= nkList.count.get(i));
        }
        for (int i = 0; i < Converters.checkNull(nkList.count).size(); i++) {
            TestCase.assertTrue(nkList.count.get(i) >= 0);
        }
    }

    private void filterTest(@Nullable FreqKListMG fkList) {
        Converters.checkNull(fkList);
        fkList.filter();
        fkList.getList().forEach(rss -> assertTrue(fkList.hMap.getInt(rss) >=
                fkList.totalRows*fkList.epsilon - fkList.getErrBound()));
    }

    private void shhAdd(ITable left, ITable right, double epsilon) {
        SampleHeavyHittersSketch shh = new SampleHeavyHittersSketch(left.getSchema(), epsilon,
                left.getNumOfRows() + right.getNumOfRows(), 135078971);
        FreqKListSample leftList  = shh.create(left);
        FreqKListSample rightList  = shh.create(right);
        FreqKList shhList = shh.add(leftList, rightList);
        Assert.assertNotNull(shhList);
        checkList(shhList, left.getSchema());
    }

    private void fkAdd(ITable left, ITable right, double epsilon) {
        FreqKSketchMG fk = new FreqKSketchMG(left.getSchema(), epsilon);
        FreqKListMG leftList = fk.create(left);
        FreqKListMG rightList = fk.create(right);
        FreqKListMG fkList = fk.add(leftList, rightList);
        Assert.assertNotNull(fkList);
        fkList.filter();
        checkList(fkList, left.getSchema());

        //System.out.println(fkList.toString());
        //return fkList;
    }

    private void shhCreate(ITable table, double epsilon) {
        SampleHeavyHittersSketch shh = new SampleHeavyHittersSketch(table.getSchema(), epsilon,
                table.getNumOfRows(), 135078971);
        FreqKListSample shhList  = shh.create(table);
        shhList.rescale();
        //System.out.println(shhList.toString());
        //return shhList;
    }

    private void fkCreate(ITable table, double epsilon) {
        FreqKSketchMG fk = new FreqKSketchMG(table.getSchema(), epsilon);
        FreqKListMG fkList= fk.create(table);
        fkList.filter();
        System.out.println(fkList.toString());
        //return fkList;
    }

    @Test
    public void testTopK1() {
        final int numCols = 2;
        final double epsilon = 0.005;
        final int size = 2000;
        Table leftTable = TestTables.getRepIntTable(size, numCols);
        fkCreate(leftTable, epsilon);
        shhCreate(leftTable, epsilon);
    }

    @Test
    public void testTopKSq() {
        final int range = 10;
        double epsilon = 0.02;
        SmallTable leftTable = TestTables.getSqIntTable(range);
        fkCreate(leftTable, epsilon);
        shhCreate(leftTable, epsilon);
    }

    @Test
    public void testTopK2() {
        final int size = 10000;
        final int numCols = 2;
        Table leftTable = TestTables.getRepIntTable(size, numCols);
        Table rightTable = TestTables.getRepIntTable(size, numCols);
        final double epsilon = 0.05;
        fkCreate(leftTable, epsilon);
        shhCreate(leftTable, epsilon);
        shhAdd(leftTable, rightTable, epsilon);
        fkAdd(leftTable, rightTable, epsilon);
    }

    @Test
    public void testTopK3() {
        final int numCols = 2;
        final double epsilon = 0.04;
        final double base = 2.0;
        final int range = 14;
        final int size = 20000;
        SmallTable leftTable = TestTables.getHeavyIntTable(numCols, size, base, range);
        fkCreate(leftTable, epsilon);
        shhCreate(leftTable, epsilon);
    }

    @Test
    public void testTopK4() {
        final int numCols = 2;
        final double epsilon = 0.02;
        final double base = 2.0;
        final int range = 10;
        final int size = 20000;
        SmallTable leftTable = TestTables.getHeavyIntTable(numCols, size, base, range);
        SmallTable rightTable = TestTables.getHeavyIntTable(numCols, 2*size, base, range);
        shhAdd(leftTable, rightTable, epsilon);
        fkAdd(leftTable, rightTable, epsilon);
    }

    @Test
    public void testTopK5() {
        final int numCols = 2;
        final double epsilon = 0.01;
        final double base = 2.0;
        final int range = 16;
        final int size = 100000;
        SmallTable bigTable = TestTables.getHeavyIntTable(numCols, size, base, range);
        List<ITable> tabList = TestTables.splitTable(bigTable, 1000);
        ArrayList<IDataSet<ITable>> a = new ArrayList<IDataSet<ITable>>();
        tabList.forEach(t -> a.add(new LocalDataSet<ITable>(t)));
        ParallelDataSet<ITable> all = new ParallelDataSet<ITable>(a);
        FreqKSketchMG fk = new FreqKSketchMG(bigTable.getSchema(), epsilon);
        //Assert.assertNotNull(all.blockingSketch(fk).toString());
        FreqKListMG fkList = all.blockingSketch(fk);
        fkList.filter();
        System.out.println(fkList.toString());
        SampleHeavyHittersSketch shh = new SampleHeavyHittersSketch(bigTable.getSchema(), epsilon,
                bigTable.getNumOfRows(), 184764);
        FreqKListSample shhList = all.blockingSketch(shh);
        shhList.rescale();
        System.out.println(shhList.toString());
    }

    @Test
    public void testTopK6() {
        Table t = TestTables.testRepTable();
        HashSubSchema hss = new HashSubSchema();
        hss.add("Age");
        FreqKSketchMG fk = new FreqKSketchMG(t.getSchema().project(hss), 5);
        String s = "10: (3-4)\n20: (3-4)\n30: (2-3)\n40: (1-2)\nError bound: 1\n";
        //Assert.assertEquals(fk.create(t).toString(), s);
    }
}
