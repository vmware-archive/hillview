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

import org.hillview.sketches.DoubleHistogramBuckets;
import org.hillview.sketches.HeatMap;
import org.hillview.sketches.Histogram;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IAppendableColumn;
import org.hillview.table.api.IColumn;
import org.hillview.table.columns.BaseListColumn;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.table.columns.DoubleArrayColumn;
import org.junit.Assert;
import org.junit.Test;

public class HistogramTest extends BaseTest {
    @Test
    public void testHistogram() {
        final int bucketNum = 110;
        final int colSize = 10000;
        DoubleHistogramBuckets buckDes = new DoubleHistogramBuckets(0, 100, bucketNum);
        Histogram hist = new Histogram(buckDes);
        DoubleArrayColumn col = DoubleArrayTest.generateDoubleArray(colSize, 100);
        FullMembershipSet fMap = new FullMembershipSet(colSize);
        hist.create(col, fMap, 1.0, 0, false);
        int size = 0;
        for (int i = 0; i < bucketNum; i++)
            size += hist.getCount(i);
        Assert.assertEquals(size + hist.getMissingData() + hist.getOutOfRange(), colSize);
        Histogram hist1 = new Histogram(buckDes);
        DoubleArrayColumn col1 = DoubleArrayTest.generateDoubleArray(2 * colSize, 100);
        FullMembershipSet fMap1 = new FullMembershipSet(2 * colSize);
        hist1.create(col1, fMap1, 1.0, 0, false);
        Histogram hist2 = hist1.union(hist);
        size = 0;
        for (int i = 0; i < bucketNum; i++)
            size += hist2.getCount(i);
        Assert.assertEquals(size + hist2.getMissingData() + hist2.getOutOfRange(), 3 * colSize);
        Histogram hist3 = new Histogram(buckDes);
        hist3.create(col, fMap, 0.1, 0, false);
        size = 0;
        for (int i = 0; i < bucketNum; i++)
            size += hist3.getCount(i);
        Assert.assertTrue(size + hist3.getMissingData() + hist3.getOutOfRange() > 0.9 * colSize);
        Assert.assertTrue(size + hist3.getMissingData() + hist3.getOutOfRange() < 1.1 * colSize);
    }

    @Test
    public void testHeatMap() {
        final int bucketNum = 110;
        final int colSize = 10000;
        DoubleHistogramBuckets buckDes1 = new DoubleHistogramBuckets(0, 100, bucketNum);
        DoubleHistogramBuckets buckDes2 = new DoubleHistogramBuckets(0, 100, bucketNum);
        HeatMap hm = new HeatMap(buckDes1, buckDes2);
        DoubleArrayColumn col1 = DoubleArrayTest.generateDoubleArray(colSize, 5);
        DoubleArrayColumn col2 = DoubleArrayTest.generateDoubleArray(colSize, 3);
        FullMembershipSet fMap = new FullMembershipSet(colSize);
        hm.createHeatMap(col1, col2, fMap, 1.0, 0, false);
        basicTestHeatMap(hm, colSize);
        HeatMap hm1 = new HeatMap(buckDes1, buckDes2);
        DoubleArrayColumn col3 = DoubleArrayTest.generateDoubleArray(2 * colSize, 100);
        DoubleArrayColumn col4 = DoubleArrayTest.generateDoubleArray(2 * colSize, 100);
        FullMembershipSet fMap1 = new FullMembershipSet(2 * colSize);
        hm1.createHeatMap(col3, col4, fMap1, 0.1, 0, false);
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
        for (int i = 0; i < hist.getNumOfBucketsD2(); i++)
            size1 += hist.getMissingHistogramD1().getCount(i);
        size1 += hist.getMissingHistogramD1().getOutOfRange();
        for (int i = 0; i < hist.getNumOfBucketsD1(); i++)
            size2 += hist.getMissingHistogramD2().getCount(i);
        size2 += hist.getMissingHistogramD2().getOutOfRange();
        Assert.assertTrue(size + size1 + size2 > 0.9 * expectedSize);
        Assert.assertTrue(size + size1 + size2 < 1.1 * expectedSize);
    }

    static void testMissing() {
        ColumnDescription c0 = new ColumnDescription("col0", ContentsKind.Double);
        ColumnDescription c1 = new ColumnDescription("col1", ContentsKind.Double);
        IAppendableColumn col0 = BaseListColumn.create(c0);
        IAppendableColumn col1 = BaseListColumn.create(c1);
        for (int i=0; i < 3; i++) {
            col0.append(0);
            col0.append(1);
            col0.append(2);
            col0.appendMissing();
        }

        for (int i = 0; i < 4; i++)
            col1.append(0);
        for (int i = 0; i < 4; i++)
            col1.append(1);
        for (int i = 0; i < 4; i++)
            col1.appendMissing();

        IColumn[] cols = new IColumn[2];
        cols[0] = col0;
        cols[1] = col1;
        Table t = new Table(cols, null, null);
        DoubleHistogramBuckets buckDes1 = new DoubleHistogramBuckets(0, 2, 3);
        DoubleHistogramBuckets buckDes2 = new DoubleHistogramBuckets(0, 1, 2);
        HeatMap hm = new HeatMap(buckDes1, buckDes2);
        hm.createHeatMap(col0, col1, new
                FullMembershipSet(col0.sizeInRows()), 1.0, 0, false);
        Histogram h1 = hm.getMissingHistogramD1();
        Histogram h2 = hm.getMissingHistogramD2();
        Assert.assertEquals(1, hm.getCount(0, 0));
        Assert.assertEquals(1, hm.getCount(0, 1));
        Assert.assertEquals(1, hm.getCount(1, 0));
        Assert.assertEquals(1, hm.getCount(1, 1));
        Assert.assertEquals(1, hm.getCount(2, 1));
        Assert.assertEquals(1, hm.getCount(2, 1));
        Assert.assertEquals(3, h1.getNumOfBuckets());
        Assert.assertEquals(1, h1.getCount(0));
        Assert.assertEquals(1, h1.getCount(1));
        Assert.assertEquals(1, h1.getCount(2));
        Assert.assertEquals(2, h2.getNumOfBuckets());
        Assert.assertEquals(1, h2.getCount(0));
        Assert.assertEquals(1, h2.getCount(1));
        Assert.assertEquals(1, hm.getMissingData());
    }
}
