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

import org.hillview.sketches.*;
import org.hillview.table.DoubleArrayColumn;
import org.hillview.table.FullMembership;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class HistogramTest {
    @Test
    public void testHistogram1DLight() throws Exception {
        final int bucketNum = 110;
        final int colSize = 10000;
        BucketsDescriptionEqSize buckDes = new BucketsDescriptionEqSize(0, 100, bucketNum);
        Histogram hist = new Histogram(buckDes);
        DoubleArrayColumn col = DoubleArrayTest.generateDoubleArray(colSize);
        FullMembership fMap = new FullMembership(colSize);
        hist.createHistogram(col, fMap, null);
        int size = 0;
        for (int i = 0; i < bucketNum; i++)
            size += hist.getCount(i);
        assertEquals(size + hist.getMissingData() + hist.getOutOfRange(), colSize);
        Histogram hist1 = new Histogram(buckDes);
        DoubleArrayColumn col1 = DoubleArrayTest.generateDoubleArray(2 * colSize);
        FullMembership fMap1 = new FullMembership(2 * colSize);
        hist1.createHistogram(col1, fMap1, null);
        Histogram hist2 = hist1.union(hist);
        size = 0;
        for (int i = 0; i < bucketNum; i++)
            size += hist2.getCount(i);
        assertEquals(size + hist2.getMissingData() + hist2.getOutOfRange(), 3 * colSize);
        Histogram hist3 = new Histogram(buckDes);
        hist3.createSampleHistogram(col, fMap, null, 0.1);
        size = 0;
        for (int i = 0; i < bucketNum; i++)
            size += hist3.getCount(i);
        assertEquals(size + hist3.getMissingData() + hist3.getOutOfRange(), (int) (colSize * 0.1));
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
        hm.createHeatMap(col1, col2, null, null, fMap);
        basicTestHeatMap(hm, colSize);
        HeatMap hm1 = new HeatMap(buckDes1, buckDes2);
        DoubleArrayColumn col3 = DoubleArrayTest.generateDoubleArray(2 * colSize);
        DoubleArrayColumn col4 = DoubleArrayTest.generateDoubleArray(2 * colSize);
        FullMembership fMap1 = new FullMembership(2 * colSize);
        hm1.createSampleHistogram(col3, col4, null, null, fMap1, 0.1);
        basicTestHeatMap(hm1, (long) (0.2 * colSize));
        HeatMap hm2 = hm.union(hm1);
        basicTestHeatMap(hm2, (long) (1.2 * colSize));
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
