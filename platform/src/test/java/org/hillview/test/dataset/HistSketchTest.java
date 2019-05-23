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
import org.hillview.dataset.ParallelDataSet;
import org.hillview.sketches.*;
import org.hillview.table.api.IColumn;
import org.hillview.test.BaseTest;
import org.hillview.utils.TestTables;
import org.hillview.table.SmallTable;
import org.hillview.table.Table;
import org.hillview.table.api.ITable;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for the sketches of all types of histograms.
 */
public class HistSketchTest extends BaseTest {
    @Test
    public void Histogram1DTest() {
        final int numCols = 1;
        final int tableSize = 1000;
        final Table myTable = TestTables.getRepIntTable(tableSize, numCols);
        final IHistogramBuckets buckets = new DoubleHistogramBuckets(1, 50, 10);
        final HistogramSketch mySketch = new HistogramSketch(buckets,
                myTable.getSchema().getColumnNames().get(0),
                1, 0);
        Histogram result = mySketch.create(myTable);
        int size = 0;
        Assert.assertNotNull(result);
        int bucketNum = result.getNumOfBuckets();
        for (int i = 0; i < bucketNum; i++)
            size += result.getCount(i);
    }

    @Test
    public void Histogram1DTest2() {
        final int numCols = 1;
        final int bigSize = 100000;
        final SmallTable bigTable = TestTables.getIntTable(bigSize, numCols);
        double min, max;
        final String colName = bigTable.getSchema().getColumnNames().get(0);
        IColumn col = bigTable.getColumn(colName);
        min = col.getInt(0);
        max = col.getInt(0);
        for (int i=0; i < col.sizeInRows(); i++) {
            int e = col.getInt(i);
            if (e < min)
                min = e;
            if (e > max)
                max = e;
        }

        final IHistogramBuckets buckets = new DoubleHistogramBuckets(min, max, 10);
        final ParallelDataSet<ITable> all = TestTables.makeParallel(bigTable, bigSize / 10);
        final Histogram hdl = all.blockingSketch(
                new HistogramSketch(buckets, colName, 0.5, 0));
        Assert.assertNotNull(hdl);
        int size = 0;
        int bucketNum = hdl.getNumOfBuckets();
        for (int i = 0; i < bucketNum; i++)
            size += hdl.getCount(i);
    }

   @Test
    public void HeatmapSketchTest() {
        final int numCols = 2;
        final int maxSize = 50;
        final int bigSize = 100000;
        final double rate = 0.5;
        final IHistogramBuckets buckets1 = new DoubleHistogramBuckets(1, 50, 10);
        final IHistogramBuckets buckets2 = new DoubleHistogramBuckets(1, 50, 15);
        final SmallTable bigTable = TestTables.getIntTable(bigSize, numCols);
        final String colName1 = bigTable.getSchema().getColumnNames().get(0);
        final String colName2 = bigTable.getSchema().getColumnNames().get(1);
        final ParallelDataSet<ITable> all = TestTables.makeParallel(bigTable, bigSize/10);
        all.blockingSketch(
                new HeatmapSketch(buckets1, buckets2, colName1, colName2, rate, 0));
    }
}
