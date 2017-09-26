/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.hillview.sketches.*;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ColumnNameAndConverter;
import org.hillview.table.api.ITable;
import org.junit.Test;
import static org.hillview.utils.TestTables.getIntTable;

/**
 * Test class for performance profiling of histogram and CDF
 */
public class CDFTest {
    private final BasicColStats colStat;
    private final LocalDataSet<ITable> dataSet;
    private final String colName;

    public CDFTest() {
        final int bigSize = 30000000;
        final int numCols = 1;
        final SmallTable bigTable = getIntTable(bigSize, numCols);
        this.colName = bigTable.getSchema().getColumnNames().iterator().next();
        this.dataSet = new LocalDataSet<ITable>(bigTable);
        this.colStat = this.dataSet.blockingSketch(
                       new BasicColStatSketch(new ColumnNameAndConverter(this.colName)));
    }

    private Histogram prepareCDF(int width, int height, boolean useSampling) {
        BucketsDescriptionEqSize bDec  =
                new BucketsDescriptionEqSize(this.colStat.getMin(), this.colStat.getMax(), width);
        double sampleSize  =  2 * height * height * width;
        double rate = sampleSize / (double)this.colStat.getPresentCount();
        if ((rate > 0.1) || (!useSampling))
            rate = 1.0; // no performance gains in sampling
        HistogramSketch sk = new HistogramSketch(bDec, new ColumnNameAndConverter(this.colName), rate);
        final Histogram tmpHist = this.dataSet.blockingSketch(sk);
        return tmpHist.createCDF();
    }

    private Histogram prepareHist(int width, int height, int barWidth, boolean useSampling) {
        int bucketNum = width / barWidth;
        BucketsDescriptionEqSize bDec  =
                new BucketsDescriptionEqSize(this.colStat.getMin(), this.colStat.getMax(), bucketNum);
        // approximately what is needed to have error smaller than a single pixel
        double sampleSize  =  2 * height * height * bucketNum;
        double rate = sampleSize / this.colStat.getPresentCount();
        if ((rate > 0.1) || (!useSampling))
            rate = 1.0; //no use in sampling
        return this.dataSet.blockingSketch(
                new HistogramSketch(bDec, new ColumnNameAndConverter(this.colName), rate));
    }

    @Test
    public void HistE2E() {
        CDFTest cdftest = new CDFTest();
        System.out.println("Running time of cdf: ");
        TestUtil.runPerfTest(k -> cdftest.prepareCDF(1000, 1000, false), 2);
        System.out.println("Running time of hist with sampling: ");
        TestUtil.runPerfTest(k -> prepareHist(1000, 100, 10, true), 2);
        System.out.println("Running time of hist without sampling: ");
        TestUtil.runPerfTest(k -> cdftest.prepareHist(1000, 100, 10, false), 2);
    }
}
