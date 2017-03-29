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
import org.hiero.sketch.dataset.ParallelDataSet;
import org.hiero.sketch.spreadsheet.*;
import org.hiero.sketch.table.SmallTable;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.ITable;
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
        final Table myTable = TableTest.getRepIntTable(tableSize, numCols);
        final BucketsDescriptionEqSize buckets = new BucketsDescriptionEqSize(1, 50, 10);
        final Hist1DLightSketch mySketch = new Hist1DLightSketch(buckets,
                myTable.getSchema().getColumnNames().iterator().next(), null);
        Histogram1DLight result = mySketch.create(myTable);
        int size = 0;
        int bucketnum = result.getNumOfBuckets();
        for (int i = 0; i < bucketnum; i++)
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
        final SmallTable bigTable = TableTest.getIntTable(bigSize, numCols);
        final String colName = bigTable.getSchema().getColumnNames().iterator().next();
        final ParallelDataSet<ITable> all = TableTest.makeParallel(bigTable, bigSize / 10);
        final Histogram1DLight hdl = all.blockingSketch(new Hist1DLightSketch(buckets, colName,
                null, 0.5));
        int size = 0;
        int bucketnum = hdl.getNumOfBuckets();
        for (int i = 0; i < bucketnum; i++)
            size += hdl.getCount(i);
        assertEquals(size + hdl.getMissingData() + hdl.getOutOfRange(), (int)
                (bigTable.getMembershipSet().getSize() * 0.5));
    }

    @Test
    public void Hist1DTest2() {
        final int numCols = 1;
        final int maxSize = 50;
        final int bigSize = 100000;
        final double rate = 0.1;
        final BucketsDescriptionEqSize buckets = new BucketsDescriptionEqSize(1, 50, 10);
        final SmallTable bigTable = TableTest.getIntTable(bigSize, numCols);
        final String colName = bigTable.getSchema().getColumnNames().iterator().next();
        final ParallelDataSet<ITable> all = TableTest.makeParallel(bigTable, bigSize / 10);
        final Histogram1D hd = all.blockingSketch(new Hist1DSketch(buckets, colName, null, rate));
        int size = 0;
        int bucketnum = hd.getNumOfBuckets();
        for (int i = 0; i < bucketnum; i++)
            size += hd.getBucket(i).getCount();
        assertEquals(size + hd.getMissingData() + hd.getOutOfRange(), (int)
                (bigTable.getMembershipSet().getSize() * rate));
    }

    @Test
    public void HeatMapSketchTest() {
        final int numCols = 2;
        final int maxSize = 50;
        final int bigSize = 100000;
        final double rate = 0.5;
        final BucketsDescriptionEqSize buckets1 = new BucketsDescriptionEqSize(1, 50, 10);
        final BucketsDescriptionEqSize buckets2 = new BucketsDescriptionEqSize(1, 50, 15);
        final SmallTable bigTable = TableTest.getIntTable(bigSize, numCols);
        final Iterator<String> iter = bigTable.getSchema().getColumnNames().iterator();
        final String colName1 = iter.next();
        final String colName2 = iter.next();
        final ParallelDataSet<ITable> all = TableTest.makeParallel(bigTable, bigSize/10);
        final HeatMap hm = all.blockingSketch(new HeatMapSketch(buckets1, buckets2, null, null,
                                                            colName1, colName2, rate));
        HistogramTest.basicTestHeatMap(hm, (long) (bigSize * rate));
    }

    @Test
    public void Hist2DSketchTest() {
        final int numCols = 2;
        final int maxSize = 50;
        final int bigSize = 100000;
        final double rate = 0.5;
        final BucketsDescriptionEqSize buckets1 = new BucketsDescriptionEqSize(1, 50, 10);
        final BucketsDescriptionEqSize buckets2 = new BucketsDescriptionEqSize(1, 50, 15);
        final SmallTable bigTable = TableTest.getIntTable(bigSize, numCols);
        final Iterator<String> iter = bigTable.getSchema().getColumnNames().iterator();
        final String colName1 = iter.next();
        final String colName2 = iter.next();
        final ParallelDataSet<ITable> all = TableTest.makeParallel(bigTable, bigSize/10);
        final Histogram2DHeavy hm = all.blockingSketch(new Hist2DSketch(buckets1, buckets2,
                null, null, colName1, colName2, rate));
        HistogramTest.basicTest2DHeavy(hm, (long) (bigSize * rate));
    }
}
