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

package org.hillview.sketch;
import org.hillview.dataset.ParallelDataSet;
import org.hillview.sketches.*;
import org.hillview.utils.TestTables;
import org.hillview.table.SmallTable;
import org.hillview.table.Table;
import org.hillview.table.api.ITable;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;

/**
 * Test class for the sketches of all types of histograms.
 */
public class HistSketchTest {
    @Test
    public void Hist1DLightTest() {
        final int numCols = 1;
        final int tableSize = 1000;
        final Table myTable = TestTables.getRepIntTable(tableSize, numCols);
        final BucketsDescriptionEqSize buckets = new BucketsDescriptionEqSize(1, 50, 10);
        final HistogramSketch mySketch = new HistogramSketch(buckets,
                myTable.getSchema().getColumnNames().iterator().next(), null);
        Histogram result = mySketch.create(myTable);
        int size = 0;
        int bucketNum = result.getNumOfBuckets();
        for (int i = 0; i < bucketNum; i++)
            size += result.getCount(i);
        assertEquals(size + result.getMissingData() + result.getOutOfRange(),
                myTable.getMembershipSet().getSize());
    }

    @Test
    public void Hist1DLightTest2() {
        final int numCols = 1;
        final int maxSize = 50;
        final int bigSize = 100000;
        final BucketsDescriptionEqSize buckets = new BucketsDescriptionEqSize(1, 50, 10);
        final SmallTable bigTable = TestTables.getIntTable(bigSize, numCols);
        final String colName = bigTable.getSchema().getColumnNames().iterator().next();
        final ParallelDataSet<ITable> all = TestTables.makeParallel(bigTable, bigSize / 10);
        final Histogram hdl = all.blockingSketch(new HistogramSketch(buckets, colName,
                null, 0.5));
        int size = 0;
        int bucketNum = hdl.getNumOfBuckets();
        for (int i = 0; i < bucketNum; i++)
            size += hdl.getCount(i);
        assertEquals(size + hdl.getMissingData() + hdl.getOutOfRange(), (int)
                (bigTable.getMembershipSet().getSize() * 0.5));
    }

   @Test
    public void HeatMapSketchTest() {
        final int numCols = 2;
        final int maxSize = 50;
        final int bigSize = 100000;
        final double rate = 0.5;
        final BucketsDescriptionEqSize buckets1 = new BucketsDescriptionEqSize(1, 50, 10);
        final BucketsDescriptionEqSize buckets2 = new BucketsDescriptionEqSize(1, 50, 15);
        final SmallTable bigTable = TestTables.getIntTable(bigSize, numCols);
        final Iterator<String> iter = bigTable.getSchema().getColumnNames().iterator();
        final String colName1 = iter.next();
        final String colName2 = iter.next();
        final ParallelDataSet<ITable> all = TestTables.makeParallel(bigTable, bigSize/10);
        final HeatMap hm = all.blockingSketch(new HeatMapSketch(buckets1, buckets2, null, null,
                                                            colName1, colName2, rate));
        HistogramTest.basicTestHeatMap(hm, (long) (bigSize * rate));
    }
}
