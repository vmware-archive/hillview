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

import org.hillview.dataset.api.IDataSet;
import org.hillview.maps.LinearProjectionMap;
import org.hillview.sketches.BasicColStatSketch;
import org.hillview.sketches.results.BasicColStats;
import org.hillview.sketches.results.HLogLog;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.utils.*;
import org.jblas.DoubleMatrix;
import org.jblas.ranges.AllRange;
import org.jblas.util.Random;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings("ConstantConditions")
public class LinearProjectionTest extends BaseTest {
    @Test
    public void testConversions() {
        int cols = 20;
        int rows = 2000;
        Random.seed(42);
        DoubleMatrix matrix = DoubleMatrix.rand(rows, cols);
        ITable table = BlasConversions.toTable(matrix);
        DoubleMatrix matrix2 = BlasConversions.toDoubleMatrix(table, table.getSchema().getColumnNames());
        Assert.assertEquals(rows * cols, matrix.eq(matrix2).sum(), Math.ulp(rows * rows));
    }

    @Test
    public void testProjection() {
        int cols = 20;
        int rows = 2000;
        int numProjections = 8;
        Random.seed(42);
        DoubleMatrix matrix = DoubleMatrix.rand(rows, cols);
        DoubleMatrix projectionMatrix = DoubleMatrix.rand(numProjections, cols);
        ITable table = BlasConversions.toTable(matrix);
        LinearProjectionMap lpm = new LinearProjectionMap(
                Utilities.toArray(table.getSchema().getColumnNames()), projectionMatrix,
                "LP");
        ITable result = lpm.apply(table);

        List<String> newColNames = new ArrayList<String>(numProjections);
        for (int i = 0; i < numProjections; i++) {
            newColNames.add(String.format("LP%d", i));
        }

        DoubleMatrix projectedData = BlasConversions.toDoubleMatrix(result, newColNames);
        DoubleMatrix projectedDataCheck = matrix.mmul(projectionMatrix.transpose());
        Assert.assertEquals(rows * numProjections, projectedData.eq(projectedDataCheck).sum(), Math.ulp(rows * cols));
    }

    @Test
    public void testDatasetProjection() {
        int rows = 10000;
        int cols = 20;
        int numProjections = 2;
        long[] seeds = new long[cols];
        Arrays.fill(seeds, 0);
        DoubleMatrix dataMatrix = DoubleMatrix.rand(rows, cols);
        DoubleMatrix projectionMatrix = DoubleMatrix.rand(numProjections, cols);
        DoubleMatrix projectionCheck = dataMatrix.mmul(projectionMatrix.transpose());

        ITable bigTable = BlasConversions.toTable(dataMatrix);
        String[] colNames = Utilities.toArray(bigTable.getSchema().getColumnNames());
        // Convert it to an IDataset
        IDataSet<ITable> all = TestTables.makeParallel(bigTable, rows / 10);

        LinearProjectionMap lpm = new LinearProjectionMap(colNames, projectionMatrix, "LP");
        IDataSet<ITable> result = all.blockingMap(lpm);

        for (int i = 0; i < numProjections; i++) {
            BasicColStatSketch b = new BasicColStatSketch(
                    String.format("LP%d", i), 1, 0);
            JsonList<Pair<BasicColStats, HLogLog>> bcss = result.blockingSketch(b);
            Assert.assertNotNull(bcss);
            Assert.assertEquals(1, bcss.size());
            double expectedMean = projectionCheck.get(new AllRange(), i).mean();
            double actualMean = bcss.get(0).first.getMoment(1);
            double eps = actualMean * 1e-6;
            Assert.assertTrue("Mean is too far from actual mean", Math.abs(actualMean - expectedMean) < eps);

            double expectedMin = projectionCheck.get(new AllRange(), i).min();
            double actualMin = bcss.get(0).first.getMin();
            eps = actualMin * 1e-6;
            Assert.assertTrue("Min is too far from actual min", Math.abs(actualMin - expectedMin) < eps);

            double expectedMax = projectionCheck.get(new AllRange(), i).max();
            double actualMax = bcss.get(0).first.getMax();
            eps = actualMax * 1e-6;
            Assert.assertTrue("Max is too far from actual min", Math.abs(actualMax - expectedMax) < eps);
        }
    }
}
