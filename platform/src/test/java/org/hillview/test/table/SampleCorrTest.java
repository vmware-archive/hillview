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

package org.hillview.test.table;

import org.hillview.dataset.ParallelDataSet;
import org.hillview.sketches.*;
import org.hillview.sketches.results.CorrMatrix;
import org.hillview.sketches.results.ICorrelation;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.test.TestUtil;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class SampleCorrTest extends BaseTest {
    @Test
    public void SampleCorrTest1() {
        int size = 40000;
        int range = 50000;
        int numCols = 5;
        SmallTable data = TestTables.getCorrelatedCols(size, numCols, range);
        //System.out.println(data.toLongString(20));
        String[] cn = new String[numCols];
        for (int i = 0; i < numCols; i++) {
            cn[i] = "Col" + i;
        }
        for (int i = 0; i <= 3; i++) {
            SampleCorrelationSketch ip = new SampleCorrelationSketch(cn, Math.pow(0.5, i), 0);
            CorrMatrix cm = ip.create(data);
            Assert.assertNotNull(cm);
            //System.out.printf("Sampling rate %f: ", Math.pow(0.5, i));
            for (int j = 0; j < cn.length; j++)
                for (int k = 0; k < cn.length; k++) {
                    Assert.assertEquals(cm.getCorrelationMatrix()[j][k],
                            cm.getCorrelationWith(cn[j])[k], 0.001);
                    Assert.assertEquals(cm.getCorrelationMatrix()[j][k],
                            cm.getCorrelation(cn[j], cn[k]), 0.001);
                }
            for (String s : cn)
                Assert.assertEquals(cm.getNorm(s) * cm.getNorm(s), cm.getInnerProduct(s, s), 0.1);
        }
    }

    @Test
    public void SampleCorrTest2() {
        int size = 20000;
        int range = 5000;
        int numCols = 4;
        int lowDim = 100;
        double p = 0.01;
        ITable data = TestTables.getCorrelatedCols(size, numCols, range);
        ParallelDataSet<ITable> all = TestTables.makeParallel(data, 100);
        String[] cn = new String[numCols];
        for (int i = 0; i < numCols; i++) {
            cn[i] = "Col" + i;
        }
        long start, end;
        start = System.currentTimeMillis();
        SampleCorrelationSketch exactIP = new SampleCorrelationSketch(cn, 1);
        ICorrelation exactCorr = all.blockingSketch(exactIP);
        Assert.assertNotNull(exactCorr);
        end = System.currentTimeMillis();
        TestUtil.comparePerf("Exact", end - start);
        start = System.currentTimeMillis();
        SampleCorrelationSketch ip = new SampleCorrelationSketch(cn, p, 0);
        ICorrelation cm = all.blockingSketch(ip);
        Assert.assertNotNull(cm);
        end = System.currentTimeMillis();
        TestUtil.comparePerf("SampleIP", end - start);
        start = System.currentTimeMillis();
        JLSketch jl = new JLSketch(cn, lowDim, 0);
        ICorrelation jlp = all.blockingSketch(jl);
        Assert.assertNotNull(jlp);
        end = System.currentTimeMillis();
        TestUtil.comparePerf("JL", end - start);
        for (int j = 0; j < cn.length; j++)
            for (int k = 0; k < cn.length; k++) {
                String s = cn[j];
                String t = cn[k];
                /*System.out.printf("(%d, %d): %.2f, %.2f, %.2f\n", j, k,
                        exactCorr.getCorrelation(s, t), cm.getCorrelation(s, t),
                        jlp.getCorrelation(s, t));*/
                Assert.assertEquals(cm.getCorrelationMatrix()[j][k],
                            cm.getCorrelationWith(cn[j])[k], 0.001);
                Assert.assertEquals(exactCorr.getCorrelationMatrix()[j][k],
                        exactCorr.getCorrelationWith(cn[j])[k], 0.001);
                Assert.assertEquals(jlp.getCorrelationMatrix()[j][k],
                        jlp.getCorrelationWith(cn[j])[k], 0.001);
            }
    }
}
