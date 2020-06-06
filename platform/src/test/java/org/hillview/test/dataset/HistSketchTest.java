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
import org.hillview.dataset.api.TableSketch;
import org.hillview.sketches.*;
import org.hillview.sketches.results.*;
import org.hillview.table.api.IIntColumn;
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
        Table myTable = TestTables.getRepIntTable(tableSize, numCols);
        IHistogramBuckets buckets = new DoubleHistogramBuckets(
                myTable.getSchema().getColumnNames().get(0),1, 50, 10);
        TableSketch<Groups<Count>> sk = new HistogramSketch(buckets).sampled(1, 0);
        Groups<Count> result = sk.create(myTable);
        int size = 0;
        Assert.assertNotNull(result);
        int bucketNum = result.size();
        for (int i = 0; i < bucketNum; i++)
            size += result.getBucket(i).count;
        Assert.assertTrue(tableSize > size + result.perMissing.count);
    }

    @Test
    public void testHighOrderHistogram() {
        final int numCols = 1;
        final int tableSize = 1000;
        Table myTable = TestTables.getRepIntTable(tableSize, numCols);
        IHistogramBuckets buckets = new DoubleHistogramBuckets(
                myTable.getSchema().getColumnNames().get(0), 1, 50, 10);
        HistogramSketch mySketch = new HistogramSketch(buckets);
        LocalDataSet<ITable> local = new LocalDataSet<ITable>(myTable);
        Groups<Count> result = local.blockingSketch(mySketch);
        int size = 0;
        Assert.assertNotNull(result);
        int bucketNum = result.perBucket.size();
        for (int i = 0; i < bucketNum; i++)
            size += result.perBucket.get(i).count;
        Assert.assertTrue(tableSize > size + result.perMissing.count);
    }

    @Test
    public void HistogramGeneric1DTest2() {
        final int numCols = 1;
        final int bigSize = 100000;
        final SmallTable bigTable = TestTables.getIntTable(bigSize, numCols);
        double min, max;
        final String colName = bigTable.getSchema().getColumnNames().get(0);
        IIntColumn col = bigTable.getColumn(colName).to(IIntColumn.class);
        min = col.minInt();
        max = col.maxInt();

        IHistogramBuckets buckets = new DoubleHistogramBuckets(colName, min, max, 10);
        ParallelDataSet<ITable> all = TestTables.makeParallel(bigTable, bigSize / 10);
        HistogramSketch mySketch = new HistogramSketch(buckets);
        Groups<Count> h = all.blockingSketch(mySketch);
        Assert.assertNotNull(h);
        int size = 0;
        int bucketNum = h.perBucket.size();
        for (int i = 0; i < bucketNum; i++)
            size += h.perBucket.get(i).count;
        Assert.assertEquals(bigSize, size);
    }

    @Test
    public void HeatmapSketchTest() {
        final int numCols = 2;
        final int bigSize = 100000;
        final double rate = 0.5;
        SmallTable bigTable = TestTables.getIntTable(bigSize, numCols);
        String colName1 = bigTable.getSchema().getColumnNames().get(0);
        String colName2 = bigTable.getSchema().getColumnNames().get(1);
        IHistogramBuckets buckets1 = new DoubleHistogramBuckets(colName1, 1, 50, 10);
        IHistogramBuckets buckets2 = new DoubleHistogramBuckets(colName2, 1, 50, 15);
        ParallelDataSet<ITable> data0 = TestTables.makeParallel(bigTable, bigSize/10);
        IDataSet<ITable> data1 = new LocalDataSet<ITable>(bigTable);
        Groups<Groups<Count>> h0 = data0.blockingSketch(
                new Histogram2DSketch(buckets2, buckets1).sampled(rate, 0));
        Assert.assertNotNull(h0);
        Groups<Groups<Count>> h1 = data1.blockingSketch(
                new Histogram2DSketch(buckets2, buckets1).sampled(rate, 0));
        Assert.assertNotNull(h1);
        Assert.assertEquals(h0.toString(), h1.toString());
    }

    @Test
    public void HeatmapGenericSketchTest() {
        int numCols = 2;
        int bigSize = 100000;
        int xBuckets = 10;
        int yBuckets = 15;
        int fragments = 10;
        SmallTable bigTable = TestTables.getIntTable(bigSize, numCols);
        String colName1 = bigTable.getSchema().getColumnNames().get(0);
        String colName2 = bigTable.getSchema().getColumnNames().get(1);
        IIntColumn col1 = bigTable.getColumn(colName1).to(IIntColumn.class);
        IIntColumn col2 = bigTable.getColumn(colName2).to(IIntColumn.class);
        IHistogramBuckets buckets1 = new DoubleHistogramBuckets(colName1, col1.minInt(), col1.maxInt(), xBuckets);
        IHistogramBuckets buckets2 = new DoubleHistogramBuckets(colName2, col2.minInt(), col2.maxInt(), yBuckets);
        ParallelDataSet<ITable> data0 = TestTables.makeParallel(bigTable, bigSize/fragments);
        Assert.assertEquals(fragments, data0.size());

        IDataSet<ITable> data1 = new LocalDataSet<ITable>(bigTable);
        Histogram2DSketch s = new Histogram2DSketch(buckets2, buckets1);
        Groups<Groups<Count>> h0 = data0.blockingSketch(s);
        Assert.assertNotNull(h0);
        Groups<Groups<Count>> h1 = data1.blockingSketch(s);
        Assert.assertNotNull(h1);
        Groups<Groups<Count>> h = data0.blockingSketch(
                new Histogram2DSketch(buckets2, buckets1).sampled(1.0, 0));
        Assert.assertNotNull(h);
        Assert.assertEquals(h.toString(), h1.toString());
        Assert.assertEquals(h.toString(), h0.toString());
        Assert.assertEquals(h1.toString(), h0.toString());
        Assert.assertEquals(h0, h1);
    }

    @Test
    public void HeatmapArrayGenericSketchTest() {
        int numCols = 3;
        int bigSize = 100000;
        int xBuckets = 10;
        int yBuckets = 15;
        int zBuckets = 20;
        int fragments = 10;
        SmallTable bigTable = TestTables.getIntTable(bigSize, numCols);
        String colName1 = bigTable.getSchema().getColumnNames().get(0);
        String colName2 = bigTable.getSchema().getColumnNames().get(1);
        String colName3 = bigTable.getSchema().getColumnNames().get(2);
        IIntColumn col1 = bigTable.getColumn(colName1).to(IIntColumn.class);
        IIntColumn col2 = bigTable.getColumn(colName2).to(IIntColumn.class);
        IIntColumn col3 = bigTable.getColumn(colName3).to(IIntColumn.class);
        IHistogramBuckets buckets1 = new DoubleHistogramBuckets(colName1, col1.minInt(), col1.maxInt(), xBuckets);
        IHistogramBuckets buckets2 = new DoubleHistogramBuckets(colName2, col2.minInt(), col2.maxInt(), yBuckets);
        IHistogramBuckets buckets3 = new DoubleHistogramBuckets(colName3, col3.minInt(), col3.maxInt(), zBuckets);
        ParallelDataSet<ITable> data0 = TestTables.makeParallel(bigTable, bigSize/fragments);

        IDataSet<ITable> data1 = new LocalDataSet<ITable>(bigTable);
        Histogram3DSketch s = new Histogram3DSketch(buckets1, buckets2, buckets3);
        Groups<Groups<Groups<Count>>> h0 = data0.blockingSketch(s);
        Assert.assertNotNull(h0);
        Groups<Groups<Groups<Count>>> h1 = data1.blockingSketch(s);
        Assert.assertNotNull(h1);
        Assert.assertEquals(h0, h1);
    }
}
