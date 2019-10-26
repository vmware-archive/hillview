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

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.sketches.results.ColumnSortOrientation;
import org.hillview.sketches.results.NextKList;
import org.hillview.sketches.NextKSketch;
import org.hillview.table.*;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;
import org.hillview.table.api.IndexComparator;
import org.hillview.table.membership.EmptyMembershipSet;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.test.BaseTest;
import org.hillview.utils.Converters;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class NextKSketchTest extends BaseTest {
    boolean printOn = false;
    @Test
    public void testTopK1() {
        final int numCols = 2;
        final int maxSize = 5;
        final int rightSize = 1000;
        final int leftSize = 1000;
        final Table leftTable = TestTables.getRepIntTable(leftSize, numCols);
        RecordOrder cso = new RecordOrder();
        for (String colName : leftTable.getSchema().getColumnNames())
            cso.append(new ColumnSortOrientation(leftTable.getSchema().getDescription(colName), true));
        final RowSnapshot topRow = new RowSnapshot(leftTable, 10);
        Assert.assertEquals(topRow.toString(), "14,4");

        final NextKSketch nk = new NextKSketch(cso, null, topRow, maxSize);
        final NextKList leftK = nk.create(leftTable);
        Assert.assertNotNull(leftK);
        IndexComparator leftComp = cso.getIndexComparator(leftK.rows);
        for (int i = 0; i < (leftK.rows.getNumOfRows() - 1); i++)
            Assert.assertTrue(leftComp.compare(i, i + 1) <= 0);
        Assert.assertEquals(leftK.toLongString(maxSize), "Table[2x5]\n" +
                "14,4: 6\n" +
                "14,5: 5\n" +
                "14,6: 4\n" +
                "14,7: 3\n" +
                "14,8: 5\n...");

        final RowSnapshot topRow2 = new RowSnapshot(leftTable, 100);
        Assert.assertEquals(topRow2.toString(), "4,4");

        final NextKSketch nk2 = new NextKSketch(cso, null, topRow2, maxSize);
        final NextKList leftK2 = nk2.create(leftTable);
        Assert.assertNotNull(leftK2);
        IndexComparator leftComp2 = cso.getIndexComparator(leftK2.rows);
        for (int i = 0; i < (leftK2.rows.getNumOfRows() - 1); i++)
            Assert.assertTrue(leftComp2.compare(i, i + 1) <= 0);
        Assert.assertEquals(leftK2.toLongString(maxSize), "Table[2x5]\n" +
                "4,4: 12\n" +
                "4,5: 2\n" +
                "4,6: 5\n" +
                "4,7: 4\n" +
                "4,8: 5\n...");
        final Table rightTable = TestTables.getRepIntTable(rightSize, numCols);
        final NextKList rightK = nk.create(rightTable);
        Assert.assertNotNull(rightK);
        IndexComparator rightComp = cso.getIndexComparator(rightK.rows);
        for (int i = 0; i < (rightK.rows.getNumOfRows() - 1); i++)
            Assert.assertTrue(rightComp.compare(i, i + 1) <= 0);

        Assert.assertEquals(rightK.toLongString(maxSize), "Table[2x5]\n" +
                "14,4: 6\n" +
                "14,5: 5\n" +
                "14,6: 4\n" +
                "14,7: 3\n" +
                "14,8: 5\n...");

        NextKList tK = nk.add(leftK, rightK);
        Assert.assertNotNull(tK);
        IndexComparator tComp = cso.getIndexComparator(tK.rows);
        for (int i = 0; i < (tK.rows.getNumOfRows() - 1); i++)
            Assert.assertTrue(tComp.compare(i, i + 1) <= 0);
        Assert.assertEquals(tK.toLongString(maxSize), "Table[2x5]\n" +
                "14,4: 12\n" +
                "14,5: 10\n" +
                "14,6: 8\n" +
                "14,7: 6\n" +
                "14,8: 10\n...");
    }

    @Test
    public void testTopK2() {
        final int numCols = 2;
        final int maxSize = 5;
        final int leftSize = 1000;
        final Table leftTable = TestTables.getRepIntTable(leftSize, numCols);
        final RowSnapshot topRow = new RowSnapshot(leftTable, 10);
        Assert.assertEquals(leftTable.toLongString(5), "Table[2x1000]\n" +
                "9,10\n" +
                "5,4\n" +
                "8,10\n" +
                "0,3\n" +
                "7,8\n");
        RecordOrder cso = new RecordOrder();
        final NextKSketch nk= new NextKSketch(cso, null, topRow, maxSize);
        final NextKList leftK = nk.create(leftTable);
        Assert.assertNotNull(leftK);
        Assert.assertEquals(leftK.rows.getNumOfRows(), 0);
    }

    @Test
    public void testTopK3() {
        final int numCols = 5;
        final int maxSize = 5;
        final int bigSize = 1000000;
        final SmallTable bigTable = TestTables.getIntTable(bigSize, numCols);
        final RowSnapshot topRow = new RowSnapshot(bigTable, 1000);
        RecordOrder cso = new RecordOrder();
        for (String colName : bigTable.getSchema().getColumnNames())
            cso.append(new ColumnSortOrientation(bigTable.getSchema().getDescription(colName),
                    true));
        ParallelDataSet<ITable> all = TestTables.makeParallel(bigTable, 500000);

        NextKList nk = all.blockingSketch(new NextKSketch(cso, null, topRow, maxSize));
        assert nk != null;
        IndexComparator mComp = cso.getIndexComparator(nk.rows);
        for (int i = 0; i < (nk.rows.getNumOfRows() - 1); i++)
            Assert.assertTrue(mComp.compare(i, i + 1) <= 0);
        Assert.assertEquals(nk.toLongString(maxSize), "Table[5x5]\n" +
                "39,32,67,53,10: 1\n" +
                "39,32,68,4,65: 1\n" +
                "39,32,68,14,50: 1\n" +
                "39,32,68,28,29: 1\n" +
                "39,32,69,9,47: 1\n" +
                "...");
    }

    @Test
    public void testNextList() {
        ColumnDescription cd = new ColumnDescription("X", ContentsKind.Integer);
        Schema schema = new Schema();
        schema.append(cd);
        NextKList nkl = new NextKList(schema, null);
        String s = nkl.toLongString(Integer.MAX_VALUE);
        Assert.assertEquals(s, "Table[1x0]" + System.lineSeparator());
    }

    @Test
    public void testTopK4() {
        Table t = TestTables.testTable();
        final int parts = 2;
        List<IDataSet<ITable>> fragments = new ArrayList<IDataSet<ITable>>();
        for (int i = 0; i < parts; i++) {
            LocalDataSet<ITable> data = new LocalDataSet<ITable>(t);
            fragments.add(data);
        }
        IDataSet<ITable> big = new ParallelDataSet<ITable>(fragments);
        RecordOrder ro = new RecordOrder();
        ro.append(new ColumnSortOrientation(t.getSchema().getDescription("Name"), true));
        NextKSketch nk = new NextKSketch(ro, null, null, 10);
        NextKList nkl = big.blockingSketch(nk);
        Assert.assertNotNull(nkl);
        Assert.assertEquals(nkl.rows.toString(), "Table[1x10]");
    }

    @Test
    public  void TestTopK5() {
        Table t = TestTables.testTable();
        RecordOrder ro = new RecordOrder();
        ro.append(new ColumnSortOrientation(t.getSchema().getDescription("Age"), true));
        ro.append(new ColumnSortOrientation(t.getSchema().getDescription("Name"), true));
        String sb = "Table[2x13]\n1,Bill: 1\n2,Bill: 1\n" +
                    "3,Smith: 1\n4,Donald: 1\n5,Bruce: 1\n...";
        NextKSketch nks = new NextKSketch(ro, null, null, 20);
        Assert.assertEquals(sb, Converters.checkNull(nks.create(t)).toLongString(5));
    }

    @Test
    public  void TestAggregate() {
        Table t = TestTables.testTable();
        RecordOrder ro = new RecordOrder();
        ColumnDescription nameCol = t.getSchema().getDescription("Name");
        ColumnDescription ageCol = t.getSchema().getDescription("Age");
        ro.append(new ColumnSortOrientation(nameCol, true));
        AggregateDescription[] agg = new AggregateDescription[2];
        agg[0] = new AggregateDescription(ageCol, AggregateDescription.AggregateKind.Sum);
        agg[1] = new AggregateDescription(ageCol, AggregateDescription.AggregateKind.Count);
        NextKSketch nks = new NextKSketch(ro, agg, null, 20);

        String sb = "Table[1x12]\n" +
                "Bill: 2\n" +
                "Bob: 1\n" +
                "Bruce: 1\n" +
                "Dave: 1\n" +
                "Donald: 1\n" +
                "...Table[2x12]\n" +
                "3.0,2.0\n" +
                "6.0,1.0\n" +
                "5.0,1.0\n" +
                "10.0,1.0\n" +
                "4.0,1.0\n" +
                "...";
        Assert.assertEquals(sb, Converters.checkNull(nks.create(t)).toLongString(5));
    }

    /**
     * Test involving some missing values
     */
    @Test
    public void TestTopK6() {
        final int numCols = 2;
        final int maxSize = 10;
        final int leftSize = 100;
        final ITable leftTable = TestTables.getMissingIntTable(leftSize, numCols);
        RecordOrder cso = new RecordOrder();
        for (String colName : leftTable.getSchema().getColumnNames())
            cso.append(new ColumnSortOrientation(leftTable.getSchema().getDescription(colName), true));
        final RowSnapshot topRow = new RowSnapshot(leftTable, 80);
        final NextKSketch nk = new NextKSketch(cso, null, topRow, maxSize);
        final NextKList leftK = nk.create(leftTable);
        String exp = "Table[2x10]\n" +
                "80,80: 1\n" +
                "81,81: 1\n" +
                "82,82: 1\n" +
                "83,83: 1\n" +
                "84,84: 1\n" +
                "85,85: 1\n" +
                "86,86: 1\n" +
                "87,87: 1\n" +
                "88,88: 1\n" +
                "89,89: 1\n";
        Assert.assertNotNull(leftK);
        Assert.assertEquals(exp, leftK.toLongString(100));
    }

    @Test
    public void testTopKEmptyPartition() {
        Table t = TestTables.testTable();
        final int parts = 1;
        List<IDataSet<ITable>> fragments = new ArrayList<IDataSet<ITable>>();
        LocalDataSet<ITable> data = new LocalDataSet<ITable>(t);
        fragments.add(data);
        Table norows = new Table(t.getColumns(),
                new EmptyMembershipSet(t.getMembershipSet().getMax()), null, null);
        LocalDataSet<ITable> data1 = new LocalDataSet<ITable>(norows);
        fragments.add(data1);

        IDataSet<ITable> big = new ParallelDataSet<ITable>(fragments);
        RecordOrder ro = new RecordOrder();
        ro.append(new ColumnSortOrientation(t.getSchema().getDescription("Name"), true));
        NextKSketch nk = new NextKSketch(ro, null, null, 10);
        NextKList nkl = big.blockingSketch(nk);
        Assert.assertNotNull(nkl);
        Assert.assertEquals(nkl.rows.toString(), "Table[1x10]");
    }

    @Test
    public void testTopKEmpty() {
        Table t = TestTables.testTable();
        List<IDataSet<ITable>> fragments = new ArrayList<IDataSet<ITable>>();
        IDataSet<ITable> big = new ParallelDataSet<ITable>(fragments); // empty dataset
        RecordOrder ro = new RecordOrder();
        ro.append(new ColumnSortOrientation(t.getSchema().getDescription("Name"), true));
        NextKSketch nk = new NextKSketch(ro, null, null, 10);
        NextKList nkl = big.blockingSketch(nk);
        Assert.assertNotNull(nkl);
        Assert.assertEquals(nkl.rows.toString(), "Table[1x0]");
    }
}
