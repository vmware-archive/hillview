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

import org.hiero.sketches.*;
import org.hiero.table.DoubleArrayColumn;
import org.hiero.table.FullMembership;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class HistogramTest {
    @Test
    public void testHistogram1D() throws Exception {
        final int bucketnum = 110;
        final int colSize = 10000;
        BucketsDescriptionEqSize buckDes = new BucketsDescriptionEqSize(0, 100, bucketnum);
        Histogram1D hist = new Histogram1D(buckDes);
        DoubleArrayColumn col = DoubleArrayTest.generateDoubleArray(colSize);
        FullMembership fmap = new FullMembership(colSize);
        hist.createHistogram(col, fmap, null);
        int size = 0;
        for (int i = 0; i < bucketnum; i++)
            size += hist.getBucket(i).getCount();
        assertEquals(size + hist.getMissingData() + hist.getOutOfRange(), colSize);
        Histogram1D hist1 = new Histogram1D(buckDes);
        DoubleArrayColumn col1 = DoubleArrayTest.generateDoubleArray(2 * colSize);
        FullMembership fmap1 = new FullMembership(2 * colSize);
        hist1.createHistogram(col1, fmap1, null);
        Histogram1D hist2 = hist1.union(hist);
        size = 0;
        for (int i = 0; i < bucketnum; i++)
            size += hist2.getBucket(i).getCount();
        assertEquals(size + hist2.getMissingData() + hist2.getOutOfRange(), 3 * colSize);
        Histogram1D hist3 = new Histogram1D(buckDes);
        hist3.createSampleHistogram(col, fmap, null, 0.1);
        size = 0;
        for (int i = 0; i < bucketnum; i++)
            size += hist3.getBucket(i).getCount();
        assertEquals(size + hist3.getMissingData() + hist3.getOutOfRange(), (int) (colSize * 0.1));
    }

    @Test
    public void testHistogram1DLight() throws Exception {
        final int bucketnum = 110;
        final int colSize = 10000;
        BucketsDescriptionEqSize buckDes = new BucketsDescriptionEqSize(0, 100, bucketnum);
        Histogram1DLight hist = new Histogram1DLight(buckDes);
        DoubleArrayColumn col = DoubleArrayTest.generateDoubleArray(colSize);
        FullMembership fmap = new FullMembership(colSize);
        hist.createHistogram(col, fmap, null);
        int size = 0;
        for (int i = 0; i < bucketnum; i++)
            size += hist.getCount(i);
        assertEquals(size + hist.getMissingData() + hist.getOutOfRange(), colSize);
        Histogram1DLight hist1 = new Histogram1DLight(buckDes);
        DoubleArrayColumn col1 = DoubleArrayTest.generateDoubleArray(2 * colSize);
        FullMembership fmap1 = new FullMembership(2 * colSize);
        hist1.createHistogram(col1, fmap1, null);
        Histogram1DLight hist2 = hist1.union(hist);
        size = 0;
        for (int i = 0; i < bucketnum; i++)
            size += hist2.getCount(i);
        assertEquals(size + hist2.getMissingData() + hist2.getOutOfRange(), 3 * colSize);
        Histogram1DLight hist3 = new Histogram1DLight(buckDes);
        hist3.createSampleHistogram(col, fmap, null, 0.1);
        size = 0;
        for (int i = 0; i < bucketnum; i++)
            size += hist3.getCount(i);
        assertEquals(size + hist3.getMissingData() + hist3.getOutOfRange(), (int) (colSize * 0.1));
    }

    @Test
    public void testHistogram2DHeavy() throws Exception {
        final int bucketnum = 110;
        final int colSize = 10000;
        BucketsDescriptionEqSize buckDes1 = new BucketsDescriptionEqSize(0, 100, bucketnum);
        BucketsDescriptionEqSize buckDes2 = new BucketsDescriptionEqSize(0, 100, bucketnum);
        Histogram2DHeavy hist = new Histogram2DHeavy(buckDes1, buckDes2);
        DoubleArrayColumn col1 = DoubleArrayTest.generateDoubleArray(colSize);
        DoubleArrayColumn col2 = DoubleArrayTest.generateDoubleArray(colSize);
        FullMembership fmap = new FullMembership(colSize);
        hist.createHistogram(col1, col2, null, null, fmap);
        basicTest2DHeavy(hist, colSize);
        Histogram2DHeavy hist1 = new Histogram2DHeavy(buckDes1, buckDes2);
        DoubleArrayColumn col3 = DoubleArrayTest.generateDoubleArray(2 * colSize);
        DoubleArrayColumn col4 = DoubleArrayTest.generateDoubleArray(2 * colSize);
        FullMembership fmap1 = new FullMembership(2 * colSize);
        hist1.createHistogram(col3, col4, null, null, fmap1);
        basicTest2DHeavy(hist1, 2 * colSize);
        Histogram2DHeavy hist2 = hist1.union(hist);
        basicTest2DHeavy(hist2, 3 * colSize);
        Histogram2DHeavy hist3 = new Histogram2DHeavy(buckDes1, buckDes2);
        hist3.createSampleHistogram(col3, col4, null, null, fmap1, 0.1);
        basicTest2DHeavy(hist3, (int) (colSize * 0.2));
    }

    public static void basicTest2DHeavy(Histogram2DHeavy hist, long expectedSize){
        long size = 0;
        long size1 = 0;
        long size2 = 0;
        for (int i = 0; i < hist.getNumOfBucketsD1(); i++)
            for (int j = 0; j< hist.getNumOfBucketsD2(); j++)
                size += hist.getBucket(i,j).getCount();
        size += hist.getMissingData();
        size += hist.getOutOfRange();
        for (int i = 0; i < hist.getNumOfBucketsD1(); i++)
            size1 += hist.getMissingHistogramD1().getBucket(i).getCount();
        size1 += hist.getMissingHistogramD1().getOutOfRange();
        for (int i = 0; i < hist.getNumOfBucketsD2(); i++)
            size2 += hist.getMissingHistogramD2().getBucket(i).getCount();
        size2 += hist.getMissingHistogramD2().getOutOfRange();
        assertEquals(size + size1 + size2, expectedSize);
    }

    @Test
    public void testHeatMap() {
        final int bucketnum = 110;
        final int colSize = 10000;
        BucketsDescriptionEqSize buckDes1 = new BucketsDescriptionEqSize(0, 100, bucketnum);
        BucketsDescriptionEqSize buckDes2 = new BucketsDescriptionEqSize(0, 100, bucketnum);
        HeatMap hm = new HeatMap(buckDes1, buckDes2);
        DoubleArrayColumn col1 = DoubleArrayTest.generateDoubleArray(colSize, 5);
        DoubleArrayColumn col2 = DoubleArrayTest.generateDoubleArray(colSize, 3);
        FullMembership fmap = new FullMembership(colSize);
        hm.createHeatMap(col1, col2, null, null, fmap);
        basicTestHeatMap(hm, colSize);
        HeatMap hm1 = new HeatMap(buckDes1, buckDes2);
        DoubleArrayColumn col3 = DoubleArrayTest.generateDoubleArray(2 * colSize);
        DoubleArrayColumn col4 = DoubleArrayTest.generateDoubleArray(2 * colSize);
        FullMembership fmap1 = new FullMembership(2 * colSize);
        hm1.createSampleHistogram(col3, col4, null, null, fmap1, 0.1);
        basicTestHeatMap(hm1, (long) (0.2 * colSize));
        HeatMap hm2 = hm.union(hm1);
        basicTestHeatMap(hm2, (long) (1.2 * colSize));
    }

    public static void basicTestHeatMap(HeatMap hist, long expectedSize) {
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
