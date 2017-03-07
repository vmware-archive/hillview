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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.hiero.sketch.TableTest.*;

public class NextKSketchTest {
    @Test
    public void testTopK1() {
        final int numCols = 2;
        final int maxSize = 50;
        final int rightSize = 1000;
        final int leftSize = 1000;
        final Table leftTable = getRepIntTable(leftSize, numCols);
        //System.out.println(leftTable.toLongString(50));
        RecordOrder cso = new RecordOrder();
        for (String colName : leftTable.getSchema().getColumnNames()) {
            cso.append(new ColumnSortOrientation(leftTable.getSchema().getDescription(colName), true));
        }
        final RowSnapshot topRow = new RowSnapshot(leftTable, 10);
        //System.out.printf("Top Row %s. %n", topRow.toString());
        final NextKSketch nk = new NextKSketch(cso, topRow, maxSize);
        final NextKList leftK = nk.create(leftTable);
        IndexComparator leftComp = cso.getComparator(leftK.table);
        for (int i = 0; i < (leftK.table.getNumOfRows() - 1); i++)
            assertTrue(leftComp.compare(i, i + 1) <= 0);
        //System.out.println(leftK.toLongString(maxSize));

        final RowSnapshot topRow2 = new RowSnapshot(leftTable, 100);
        //System.out.printf("Top Row %s. %n", topRow2.toString());
        final NextKSketch nk2 = new NextKSketch(cso, topRow2, maxSize);
        final NextKList leftK2 = nk2.create(leftTable);
        IndexComparator leftComp2 = cso.getComparator(leftK2.table);
        for (int i = 0; i < (leftK2.table.getNumOfRows() - 1); i++)
            assertTrue(leftComp2.compare(i, i + 1) <= 0);
        //System.out.println(leftK2.toLongString(maxSize));
        final Table rightTable = getRepIntTable(rightSize, numCols);
        final NextKList rightK = nk.create(rightTable);
        IndexComparator rightComp = cso.getComparator(rightK.table);
        for (int i = 0; i < (rightK.table.getNumOfRows() - 1); i++)
            assertTrue(rightComp.compare(i, i + 1) <= 0);
        //System.out.println(rightK.toLongString(maxSize));
        NextKList tK = nk.add(leftK, rightK);
        tK = Converters.checkNull(tK);
        IndexComparator tComp = cso.getComparator(tK.table);
        for (int i = 0; i < (tK.table.getNumOfRows() - 1); i++)
            assertTrue(tComp.compare(i, i + 1) <= 0);
        //System.out.println(tK.toLongString(maxSize));
    }

    @Test
    public void testTopK2() {
        final int numCols = 2;
        final int maxSize = 50;
        final int leftSize = 1000;
        final Table leftTable = getRepIntTable(leftSize, numCols);
        final RowSnapshot topRow = new RowSnapshot(leftTable, 10);
        //System.out.println(leftTable.toLongString(50));
        RecordOrder cso = new RecordOrder();
        final NextKSketch nk= new NextKSketch(cso, topRow, maxSize);
        final NextKList leftK = nk.create(leftTable);
        assertEquals(leftK.table.getNumOfRows(), 0);
    }

    @Test
    public void testTopK3() {
        //printTime("start");
        final int numCols = 3;
        final int maxSize = 50;
        final int bigSize = 100000;
        final SmallTable bigTable = getIntTable(bigSize, numCols);
        final RowSnapshot topRow = new RowSnapshot(bigTable, 1000);
        //System.out.printf("Top Row %s. %n", topRow.toString());
        //printTime("created");
        RecordOrder cso = new RecordOrder();
        for (String colName : bigTable.getSchema().getColumnNames()) {
            cso.append(new ColumnSortOrientation(bigTable.getSchema().getDescription(colName), true));
        }
        List<SmallTable> tabList = SplitTable(bigTable, 10000);
        //printTime("split");
        ArrayList<IDataSet<ITable>> a = new ArrayList<IDataSet<ITable>>();
        for (SmallTable t : tabList) {
            LocalDataSet<ITable> ds = new LocalDataSet<ITable>(t);
            a.add(ds);
        }
        ParallelDataSet<ITable> all = new ParallelDataSet<ITable>(a);
        //printTime("Parallel");
        NextKList nk = all.blockingSketch(new NextKSketch(cso, topRow, maxSize));
        IndexComparator mComp = cso.getComparator(nk.table);
        for (int i = 0; i < (nk.table.getNumOfRows() - 1); i++)
            assertTrue(mComp.compare(i, i + 1) <= 0);
        //System.out.println(nk.toLongString(maxSize));
    }

    @Test
    public void testNextList() {
        ColumnDescription cd = new ColumnDescription("X", ContentsKind.Int, false);
        Schema schema = new Schema();
        schema.append(cd);
        NextKList nkl = new NextKList(schema);
        String s = nkl.toLongString(Integer.MAX_VALUE);
        assertEquals(s, "Table, 1 columns, 0 rows" + System.lineSeparator());
    }

    @Test
    public void testTopK4() {
        Table t = Table.testTable();
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
        assertEquals(nkl.table.toString(), "Table, 1 columns, 10 rows");
    }
}
