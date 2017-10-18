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

import org.hillview.sketches.CorrMatrix;
import org.hillview.sketches.JLProjection;
import org.hillview.sketches.JLSketch;
import org.hillview.sketches.SampleCorrelationSketch;
import org.hillview.table.SmallTable;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class JLSketchTest extends BaseTest {
    @Test
    public void JLtest1() {
        int size = 1000;
        int range = 10;
        int numCols = 6;
        SmallTable data = TestTables.getCorrelatedCols(size, numCols, range);
        //System.out.println(data.toLongString(20));
        String[] cn = new String[numCols];
        for (int i = 0; i < numCols; i++) {
            cn[i] = "Col" + String.valueOf(i);
        }
        for (int a = 1; a < 3; a++) {
            JLSketch jls = new JLSketch(cn, a * 100, 0);
            JLProjection jlp = jls.create(data);
            for (int j = 0; j < cn.length; j++)
                for (int k = 0; k < cn.length; k++) {
                    Assert.assertEquals(jlp.getCorrelationMatrix()[j][k],
                            jlp.getCorrelationWith(cn[j])[k], 0.001);
                    Assert.assertEquals(jlp.getCorrelationMatrix()[j][k],
                            jlp.getCorrelation(cn[j], cn[k]), 0.001);
                }
            for (String s : cn)
                Assert.assertEquals(jlp.getNorm(s) * jlp.getNorm(s), jlp.getInnerProduct(s, s), 0.001);
        }
    }

    @Test
    public void JLtest2() {
        int size = 10000;
        int range = 10;
        int numCols = 4;
        SmallTable leftTable = TestTables.getCorrelatedCols(size, numCols,  range);
        SmallTable rightTable = TestTables.getCorrelatedCols(size, numCols, range);
        String[] cn = new String[numCols];
        for (int i = 0; i < numCols; i++) {
            cn[i] = "Col" + String.valueOf(i);
        }
        JLSketch jls = new JLSketch(cn, 100, 0);
        JLProjection jlp = jls.add(jls.create(leftTable), jls.create(rightTable));
        //System.out.println(Arrays.toString(jlp.getCorrelationMatrix()[0]));
        SampleCorrelationSketch ip = new SampleCorrelationSketch(cn, 0);
        CorrMatrix cm = ip.add(ip.create(leftTable), ip.create(rightTable));
        //System.out.printf("IP Sketch: " + Arrays.toString(cm.getCorrelationMatrix()[0]) + "\n");
    }
}