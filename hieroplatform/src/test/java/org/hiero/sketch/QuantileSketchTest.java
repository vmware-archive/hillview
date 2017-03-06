/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0 2
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
import org.hiero.sketch.spreadsheet.QuantileList;
import org.hiero.sketch.spreadsheet.QuantileSketch;
import org.hiero.sketch.table.*;
import org.hiero.sketch.table.api.IRowOrder;
import org.hiero.sketch.table.api.ITable;
import org.hiero.sketch.table.api.IndexComparator;
import org.hiero.utils.Converters;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.hiero.sketch.TableTest.SplitTable;
import static org.hiero.sketch.TableTest.getIntTable;

public class QuantileSketchTest {
    @Test
    public void testQuantile() {
        final int numCols = 2;
        final int leftSize = 100;
        final SmallTable leftTable = getIntTable(leftSize, numCols);
        RecordOrder rso = new RecordOrder();
        for (String colName : leftTable.getSchema().getColumnNames())
            rso.append(
                    new ColumnSortOrientation(leftTable.getSchema().getDescription(colName), true));

        final int rightSize = 200;
        final SmallTable rightTable = getIntTable(rightSize, numCols);
        final int resolution = 100;
        final QuantileSketch qSketch = new QuantileSketch(rso, resolution);
        final QuantileList leftQ = qSketch.getQuantile(leftTable);
        final IndexComparator leftComp = rso.getComparator(leftQ.quantile);
        //System.out.println(leftQ);
        for (int i = 0; i < (leftQ.getQuantileSize() - 1); i++)
            assertTrue(leftComp.compare(i, i + 1) <= 0);
        final QuantileList rightQ = qSketch.getQuantile(rightTable);
        //System.out.println(rightQ);
        final IndexComparator rightComp = rso.getComparator(rightQ.quantile);
        for (int i = 0; i < (rightQ.getQuantileSize() - 1); i++)
            assertTrue(rightComp.compare(i, i + 1) <= 0);
        QuantileList mergedQ = qSketch.add(leftQ, rightQ);
        mergedQ = Converters.checkNull(mergedQ);
        IndexComparator mComp = rso.getComparator(mergedQ.quantile);
        for (int i = 0; i < (mergedQ.getQuantileSize() - 1); i++)
            assertTrue(mComp.compare(i, i + 1) <= 0);
        int newSize = 20;
        final QuantileList approxQ = mergedQ.compressApprox(newSize);
        IndexComparator approxComp = rso.getComparator(approxQ.quantile);
        for (int i = 0; i < (approxQ.getQuantileSize() - 1); i++)
            assertTrue(approxComp.compare(i, i + 1) <= 0);
        final QuantileList exactQ = mergedQ.compressExact(newSize);
        IndexComparator exactComp = rso.getComparator(exactQ.quantile);
        for (int i = 0; i < (exactQ.getQuantileSize() - 1); i++)
            assertTrue(exactComp.compare(i, i + 1) <= 0);
    }

    private long last = 0;

    private void printTime(String when) {
        long now = System.currentTimeMillis();
        if (this.last > 0)
            System.out.println(when + " " + (now - this.last));
        this.last = now;
    }

    @Test
    public void testQuantile1() {
        //printTime("start");
        final int numCols = 3;
        final int resolution = 49;
        final int bigSize = 100000;
        final SmallTable bigTable = getIntTable(bigSize, numCols);
        //printTime("created");
        RecordOrder cso = new RecordOrder();
        for (String colName : bigTable.getSchema().getColumnNames()) {
            cso.append(new ColumnSortOrientation(bigTable.getSchema().getDescription(colName), true));
        }
        List<SmallTable> tabList = SplitTable(bigTable, 10000);
        //printTime("split");
        // Create a big parallel data set containing all table fragments
        ArrayList<IDataSet<ITable>> a = new ArrayList<IDataSet<ITable>>();
        for (SmallTable t : tabList) {
            LocalDataSet<ITable> ds = new LocalDataSet<ITable>(t);
            a.add(ds);
        }
        ParallelDataSet<ITable> all = new ParallelDataSet<ITable>(a);
        //printTime("Parallel");
        QuantileList ql = all.blockingSketch(new QuantileSketch(cso, resolution)).
                compressExact(resolution);
        IndexComparator mComp = cso.getComparator(ql.quantile);
        for (int i = 0; i < (ql.getQuantileSize() - 1); i++)
            assertTrue(mComp.compare(i, i + 1) <= 0);
        //printTime("Quantile");
        /*
        IRowOrder order = new ArrayRowOrder(cso.getSortedRowOrder(bigTable));
        printTime("sort");
        Table sortTable = bigTable.compress(order);
        //System.out.println(sortTable.toLongString(50));
        //System.out.println(ql.quantile.toLongString(50));
        printTime("Compress");
        int j =0, lastMatch = 0;
        for (int i =0; i < ql.getQuantileSize(); i++) {
            boolean match = false;
            while (match == false) {
                match = true;
                for (String colName : ql.getSchema().getColumnNames()) {
                    if (ql.getColumn(colName).getInt(i) !=
                            sortTable.getColumn(colName).getInt(j))
                        match = false;
                }
                if (match == true) {
                    System.out.printf("Rank %f, target %f %n",
                            ((j+1)/ ((double) bigSize +1)), (i + 1)/ (double) (resolution + 1));
                    lastMatch = j;
                }
                j++;
                if (j >= bigSize) {
                    System.out.printf("Error! No match for %s %n", new RowSnapshot(ql.quantile, i).toString());
                    //System.out.println(sortTable.toLongString(lastMatch,50));
                    j = 0;
                    break;
                }
            }
        }
        printTime("done");
        */
    }


    @Test
    public void testQuantileSample() {
        //printTime("start");
        final int numCols = 3;
        final int resolution = 99;
        final int bigSize = 1000;
        final SmallTable bigTable = getIntTable(bigSize, numCols);
        //printTime("created");
        RecordOrder cso = new RecordOrder();
        for (String colName : bigTable.getSchema().getColumnNames()) {
            cso.append(new ColumnSortOrientation(bigTable.getSchema().getDescription(colName), true));
        }
        QuantileList ql = new QuantileSketch(cso, resolution).getQuantile(bigTable).compressExact(resolution);
        //printTime("Quantile");
        IRowOrder order = new ArrayRowOrder(cso.getSortedRowOrder(bigTable));
        //printTime("sort");
        SmallTable sortTable = bigTable.compress(order);
        //printTime("compressed");
        int j =0;
        for (int i =0; i < resolution; i++) {
            boolean match = false;
            while (!match) {
                match = true;
                for (String colName : ql.getSchema().getColumnNames()) {
                    if (ql.getColumn(colName).getObject(i) != sortTable.getColumn(colName).getObject(j))
                        match = false;
                }
                j++;
                if (j >= bigSize) {
                    System.out.printf("Error! No match for %d%n", i + 1);
                    j = 0;
                    break;
                }
            }
        }
        //printTime("done");
    }
}