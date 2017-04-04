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

package org.hiero.sketch;

import org.hiero.sketch.dataset.LocalDataSet;
import org.hiero.sketch.dataset.ParallelDataSet;
import org.hiero.sketch.dataset.api.IDataSet;
import org.hiero.sketch.spreadsheet.ColumnSortOrientation;
import org.hiero.sketch.spreadsheet.NextKList;
import org.hiero.sketch.spreadsheet.NextKSketch;
import org.hiero.sketch.table.*;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.ITable;
import org.hiero.sketch.table.api.IndexComparator;
import org.hiero.utils.Converters;
import org.hiero.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class NextKSketchTest {
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
        Assert.assertEquals(topRow.toString(), "14, 4");

        final NextKSketch nk = new NextKSketch(cso, topRow, maxSize);
        final NextKList leftK = nk.create(leftTable);
        IndexComparator leftComp = cso.getComparator(leftK.table);
        for (int i = 0; i < (leftK.table.getNumOfRows() - 1); i++)
            Assert.assertTrue(leftComp.compare(i, i + 1) <= 0);
        Assert.assertEquals(leftK.toLongString(maxSize), "Table, 2 columns, 5 rows\n" +
                "14, 4: 6\n" +
                "14, 5: 5\n" +
                "14, 6: 4\n" +
                "14, 7: 3\n" +
                "14, 8: 5\n");

        final RowSnapshot topRow2 = new RowSnapshot(leftTable, 100);
        Assert.assertEquals(topRow2.toString(), "4, 4");

        final NextKSketch nk2 = new NextKSketch(cso, topRow2, maxSize);
        final NextKList leftK2 = nk2.create(leftTable);
        IndexComparator leftComp2 = cso.getComparator(leftK2.table);
        for (int i = 0; i < (leftK2.table.getNumOfRows() - 1); i++)
            Assert.assertTrue(leftComp2.compare(i, i + 1) <= 0);
        Assert.assertEquals(leftK2.toLongString(maxSize), "Table, 2 columns, 5 rows\n" +
                "4, 4: 12\n" +
                "4, 5: 2\n" +
                "4, 6: 5\n" +
                "4, 7: 4\n" +
                "4, 8: 5\n");
        final Table rightTable = TestTables.getRepIntTable(rightSize, numCols);
        final NextKList rightK = nk.create(rightTable);
        IndexComparator rightComp = cso.getComparator(rightK.table);
        for (int i = 0; i < (rightK.table.getNumOfRows() - 1); i++)
            Assert.assertTrue(rightComp.compare(i, i + 1) <= 0);

        Assert.assertEquals(rightK.toLongString(maxSize), "Table, 2 columns, 5 rows\n" +
                "14, 4: 6\n" +
                "14, 5: 5\n" +
                "14, 6: 4\n" +
                "14, 7: 3\n" +
                "14, 8: 5\n");

        NextKList tK = nk.add(leftK, rightK);
        tK = Converters.checkNull(tK);
        IndexComparator tComp = cso.getComparator(tK.table);
        for (int i = 0; i < (tK.table.getNumOfRows() - 1); i++)
            Assert.assertTrue(tComp.compare(i, i + 1) <= 0);
        Assert.assertEquals(tK.toLongString(maxSize), "Table, 2 columns, 5 rows\n" +
                "14, 4: 12\n" +
                "14, 5: 10\n" +
                "14, 6: 8\n" +
                "14, 7: 6\n" +
                "14, 8: 10\n");
    }

    @Test
    public void testTopK2() {
        final int numCols = 2;
        final int maxSize = 5;
        final int leftSize = 1000;
        final Table leftTable = TestTables.getRepIntTable(leftSize, numCols);
        final RowSnapshot topRow = new RowSnapshot(leftTable, 10);
        Assert.assertEquals(leftTable.toLongString(5), "Table, 2 columns, 1000 rows\n" +
                "9, 10\n" +
                "5, 4\n" +
                "8, 10\n" +
                "0, 3\n" +
                "7, 8\n");
        RecordOrder cso = new RecordOrder();
        final NextKSketch nk= new NextKSketch(cso, topRow, maxSize);
        final NextKList leftK = nk.create(leftTable);
        Assert.assertEquals(leftK.table.getNumOfRows(), 0);
    }

    @Test
    public void testTopK3() {
        final int numCols = 3;
        final int maxSize = 5;
        final int bigSize = 100000;
        final SmallTable bigTable = TestTables.getIntTable(bigSize, numCols);
        final RowSnapshot topRow = new RowSnapshot(bigTable, 1000);
        Assert.assertEquals(topRow.toString(), "44, 95, 56");
        RecordOrder cso = new RecordOrder();
        for (String colName : bigTable.getSchema().getColumnNames())
            cso.append(new ColumnSortOrientation(bigTable.getSchema().getDescription(colName),
                    true));
        ParallelDataSet<ITable> all = TestTables.makeParallel(bigTable, 10000);
        NextKList nk = all.blockingSketch(new NextKSketch(cso, topRow, maxSize));
        IndexComparator mComp = cso.getComparator(nk.table);
        for (int i = 0; i < (nk.table.getNumOfRows() - 1); i++)
            Assert.assertTrue(mComp.compare(i, i + 1) <= 0);
        Assert.assertEquals(nk.toLongString(maxSize), "Table, 3 columns, 5 rows\n" +
                "44, 95, 56: 1\n" +
                "44, 95, 119: 1\n" +
                "44, 95, 126: 1\n" +
                "44, 95, 151: 1\n" +
                "44, 96, 65: 1\n");
    }

    @Test
    public void testNextList() {
        ColumnDescription cd = new ColumnDescription("X", ContentsKind.Integer, false);
        Schema schema = new Schema();
        schema.append(cd);
        NextKList nkl = new NextKList(schema);
        String s = nkl.toLongString(Integer.MAX_VALUE);
        Assert.assertEquals(s, "Table, 1 columns, 0 rows" + System.lineSeparator());
    }

    @Test
    public void testTopK4() {
        Table t = TestTables.testTable();
        final int parts = 1;
        List<IDataSet<ITable>> fragments = new ArrayList<IDataSet<ITable>>();
        for (int i = 0; i < parts; i++) {
            LocalDataSet<ITable> data = new LocalDataSet<ITable>(t);
            fragments.add(data);
        }
        IDataSet<ITable> big = new ParallelDataSet<ITable>(fragments);
        RecordOrder ro = new RecordOrder();
        ro.append(new ColumnSortOrientation(t.getSchema().getDescription("Name"), true));
        NextKSketch nk = new NextKSketch(ro, null, 10);
        NextKList nkl = big.blockingSketch(nk);
        Assert.assertEquals(nkl.table.toString(), "Table, 1 columns, 10 rows");
    }

    @Test
    public  void TestTopK5() {
        Table t = TestTables.testTable();
        RecordOrder ro = new RecordOrder();
        ro.append(new ColumnSortOrientation(t.getSchema().getDescription("Age"), true));
        ro.append(new ColumnSortOrientation(t.getSchema().getDescription("Name"), true));
        String sb = new String("Table, 2 columns, 13 rows\nBill, 1: 1\nBill, 2: 1\n");
        sb += "Smith, 3: 1\nDonald, 4: 1\nBruce, 5: 1\n";
        NextKSketch nks = new NextKSketch(ro, null, 20);
        Assert.assertEquals(sb,nks.create(t).toLongString(5));
    }
}
