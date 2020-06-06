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
import org.hillview.sketches.*;
import org.hillview.sketches.results.BasicColStats;
import org.hillview.sketches.results.DoubleHistogramBuckets;
import org.hillview.sketches.results.IHistogramBuckets;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.test.TestUtil;
import org.hillview.utils.JsonList;
import org.junit.Assert;
import org.junit.Test;
import static org.hillview.utils.TestTables.getIntTable;

/**
 * Test class for performance profiling of histograms
 */
public class HistPerfTest extends BaseTest {
    private final BasicColStats colStat;
    private final LocalDataSet<ITable> dataSet;
    private final String colName;

    public HistPerfTest() {
        final int bigSize = 30000000;
        final int numCols = 1;
        final SmallTable bigTable = getIntTable(bigSize, numCols);
        this.colName = bigTable.getSchema().getColumnNames().get(0);
        this.dataSet = new LocalDataSet<ITable>(bigTable);
        JsonList<BasicColStats> r = this.dataSet.blockingSketch(
                new BasicColStatSketch(this.colName, 0));
        Assert.assertNotNull(r);
        this.colStat = r.get(0);
    }

    /*
    private void prepareHist(int width, int height, int barWidth, boolean useSampling) {
        int bucketNum = width / barWidth;
        IHistogramBuckets bDec  =
                new DoubleHistogramBuckets(this.colName, this.colStat.getMin(), this.colStat.getMax(), bucketNum);
        // approximately what is needed to have error smaller than a single pixel
        double sampleSize  =  2 * height * height * bucketNum;
        double rate = sampleSize / this.colStat.getPresentCount();
        if ((rate > 0.1) || (!useSampling))
            rate = 1.0; // no use in sampling
        this.dataSet.blockingSketch(
                new HistogramSketch(bDec, rate, 0, null));
    }
    */

    private void prepareHistNew(int width, int height, int barWidth, boolean useSampling) {
        int bucketNum = width / barWidth;
        IHistogramBuckets bDec  =
                new DoubleHistogramBuckets(this.colName, this.colStat.getMin(), this.colStat.getMax(), bucketNum);
        // approximately what is needed to have error smaller than a single pixel
        double sampleSize  =  2 * height * height * bucketNum;
        double rate = sampleSize / this.colStat.getPresentCount();
        if ((rate > 0.1) || (!useSampling))
            rate = 1.0; // no use in sampling
        this.dataSet.blockingSketch(
                new HistogramSketch(bDec).sampled(rate, 0));
    }

    @Test
    public void HistE2E() {
        HistPerfTest cdftest = new HistPerfTest();
        /*
        TestUtil.runPerfTest("Running time of hist with sampling: ",
                k -> prepareHist(1000, 100, 10, true), 2);
        TestUtil.runPerfTest("Running time of hist without sampling: ",
                k -> cdftest.prepareHist(1000, 100, 10, false), 2);
         */
        TestUtil.runPerfTest("Running time of hist1 with sampling: ",
                k -> prepareHistNew(1000, 100, 10, true), 2);
        TestUtil.runPerfTest("Running time of hist1 without sampling: ",
                k -> cdftest.prepareHistNew(1000, 100, 10, false), 2);
    }
}
