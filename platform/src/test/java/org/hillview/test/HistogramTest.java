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

package org.hillview.test;

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.dataset.api.ISketch;
import org.hillview.sketches.*;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.table.FullMembership;
import org.hillview.table.Table;
import org.hillview.table.api.ColumnAndConverter;
import org.hillview.table.api.ColumnNameAndConverter;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class HistogramTest {
    long time(Runnable runnable) {
        //System.gc();
        long start = System.currentTimeMillis();
        runnable.run();
        long end = System.currentTimeMillis();
        return end - start;
    }

    String twoDigits(double d) {
        return String.format("%.2f", d);
    }

    void runNTimes(Runnable runnable, int count, String message, int elemCount) {
        long[] times = new long[count];
        for (int i=0; i < count; i++) {
            long t = time(runnable);
            times[i] = t;
        }
        int minIndex = 0;
        for (int i=0; i < count; i++)
            if (times[i] < times[minIndex])
                minIndex = i;
        System.out.println(message);
        System.out.println("Time (ms),Melems/s,Percent slower");
        for (int i=0; i < count; i++) {
            double speed = (double)(elemCount) / (times[i] * 1000);
            double percent = 100 * ((double)times[i] - times[minIndex]) / times[minIndex];
            System.out.println(times[i] + "," + twoDigits(speed) + "," + twoDigits(percent) + "%");
        }
    }

    //@Test
    public void testHistogramPerf() throws Exception {
        // Testing the performance of histogram computations
        final int bucketNum = 40;
        final int mega = 1024 * 1024;
        final int colSize = 100 * mega;
        final int runCount = 10;

        BucketsDescriptionEqSize buckDes = new BucketsDescriptionEqSize(0, 100, bucketNum);
        final Histogram hist = new Histogram(buckDes);
        DoubleArrayColumn col = DoubleArrayTest.generateDoubleArray(colSize, 100);
        FullMembership fMap = new FullMembership(colSize);

        Runnable r = () -> hist.create(new ColumnAndConverter(col), fMap, 1.0);
        runNTimes(r, runCount, "Simple histogram", colSize);

        List<IColumn> cols = new ArrayList<IColumn>();
        cols.add(col);
        ITable table = new Table(cols, fMap);
        ISketch<ITable, Histogram> sk = new HistogramSketch(buckDes, new ColumnNameAndConverter(col
                .getName()));
        final IDataSet<ITable> ds = new LocalDataSet<ITable>(table, false);

        r = () -> ds.blockingSketch(sk);
        runNTimes(r, runCount, "Dataset histogram", colSize);

        final IDataSet<ITable> lds = new LocalDataSet<ITable>(table);
        r = () -> lds.blockingSketch(sk);
        runNTimes(r, runCount, "Dataset histogram (separate thread)", colSize);
    }

    @Test
    public void testHistogram() throws Exception {
        final int bucketNum = 110;
        final int colSize = 10000;
        BucketsDescriptionEqSize buckDes = new BucketsDescriptionEqSize(0, 100, bucketNum);
        Histogram hist = new Histogram(buckDes);
        DoubleArrayColumn col = DoubleArrayTest.generateDoubleArray(colSize, 100);
        FullMembership fMap = new FullMembership(colSize);
        hist.create(new ColumnAndConverter(col), fMap, 1.0);
        int size = 0;
        for (int i = 0; i < bucketNum; i++)
            size += hist.getCount(i);
        assertEquals(size + hist.getMissingData() + hist.getOutOfRange(), colSize);
        Histogram hist1 = new Histogram(buckDes);
        DoubleArrayColumn col1 = DoubleArrayTest.generateDoubleArray(2 * colSize, 100);
        FullMembership fMap1 = new FullMembership(2 * colSize);
        hist1.create(new ColumnAndConverter(col1), fMap1, 1.0);
        Histogram hist2 = hist1.union(hist);
        size = 0;
        for (int i = 0; i < bucketNum; i++)
            size += hist2.getCount(i);
        assertEquals(size + hist2.getMissingData() + hist2.getOutOfRange(), 3 * colSize);
        Histogram hist3 = new Histogram(buckDes);
        hist3.create(new ColumnAndConverter(col), fMap, 0.1);
        size = 0;
        for (int i = 0; i < bucketNum; i++)
            size += hist3.getCount(i);
        assertEquals(size + hist3.getMissingData() + hist3.getOutOfRange(), colSize);
    }

    @Test
    public void testHeatMap() {
        final int bucketNum = 110;
        final int colSize = 10000;
        BucketsDescriptionEqSize buckDes1 = new BucketsDescriptionEqSize(0, 100, bucketNum);
        BucketsDescriptionEqSize buckDes2 = new BucketsDescriptionEqSize(0, 100, bucketNum);
        HeatMap hm = new HeatMap(buckDes1, buckDes2);
        DoubleArrayColumn col1 = DoubleArrayTest.generateDoubleArray(colSize, 5);
        DoubleArrayColumn col2 = DoubleArrayTest.generateDoubleArray(colSize, 3);
        FullMembership fMap = new FullMembership(colSize);
        hm.createHeatMap(new ColumnAndConverter(col1), new ColumnAndConverter(col2), fMap);
        basicTestHeatMap(hm, colSize);
        HeatMap hm1 = new HeatMap(buckDes1, buckDes2);
        DoubleArrayColumn col3 = DoubleArrayTest.generateDoubleArray(2 * colSize, 100);
        DoubleArrayColumn col4 = DoubleArrayTest.generateDoubleArray(2 * colSize, 100);
        FullMembership fMap1 = new FullMembership(2 * colSize);
        hm1.createSampleHistogram(new ColumnAndConverter(col3), new ColumnAndConverter(col4),
                fMap1, 0.1);
        basicTestHeatMap(hm1, 2 * colSize);
        HeatMap hm2 = hm.union(hm1);
        basicTestHeatMap(hm2, 3 * colSize);
    }

    static void basicTestHeatMap(HeatMap hist, long expectedSize) {
        long size = 0;
        long size1 = 0;
        long size2 = 0;
        for (int i = 0; i < hist.getNumOfBucketsD1(); i++)
            for (int j = 0; j< hist.getNumOfBucketsD2(); j++)
                size += hist.getCount(i,j);
        size += hist.getMissingData();
        size += hist.getOutOfRange();
        for (int i = 0; i < hist.getNumOfBucketsD1(); i++)
            size1 += hist.getMissingHistogramD1().getCount(i);
        size1 += hist.getMissingHistogramD1().getOutOfRange();
        for (int i = 0; i < hist.getNumOfBucketsD2(); i++)
            size2 += hist.getMissingHistogramD2().getCount(i);
        size2 += hist.getMissingHistogramD2().getOutOfRange();
        assertEquals(size + size1 + size2, expectedSize);
    }
}
