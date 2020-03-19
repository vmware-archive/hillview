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
import org.hillview.maps.LAMPMap;
import org.hillview.sketches.RandomSamplingSketch;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.utils.*;
import org.jblas.DoubleMatrix;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LAMPMapTest extends BaseTest {
    private void testLAMPMap(ITable table, int numSamples, int fragmentSize) {
        long seed = 1;
        IDataSet<ITable> dataset = TestTables.makeParallel(table, fragmentSize);
        String[] colNames = Utilities.toArray(TestUtils.getNumericColumnNames(table));
        double samplingRate = ((double) numSamples) / table.getNumOfRows();
        RandomSamplingSketch sketch = new RandomSamplingSketch(
                samplingRate, seed, colNames, false);
        SmallTable sampling = dataset.blockingSketch(sketch);
        Assert.assertNotNull(sampling);
        sampling = sampling.compress(sampling.getMembershipSet().sample(numSamples, 0));

        DoubleMatrix ndControlPoints = BlasConversions.toDoubleMatrix(sampling,
                Arrays.asList(colNames));
        MetricMDS mds = new MetricMDS(ndControlPoints);
        DoubleMatrix proj = mds.computeEmbedding(3);

        System.out.println("\nMDS projection:");
        System.out.println("\tMin x: " + proj.getColumn(0).min());
        System.out.println("\tMax x: " + proj.getColumn(0).max());
        System.out.println("\tMin y: " + proj.getColumn(1).min());
        System.out.println("\tMax y: " + proj.getColumn(1).max());

        List<String> newColNames = new ArrayList<String>(2);
        newColNames.add("LAMP1");
        newColNames.add("LAMP2");
        LAMPMap map = new LAMPMap(ndControlPoints, proj, colNames,
                Utilities.toArray(newColNames));
        ITable result = map.apply(table);
        Assert.assertNotNull(result);
        IDataSet<ITable> datasetResult = dataset.blockingMap(map);
        Assert.assertNotNull(datasetResult);
        DoubleMatrix lampProjection = BlasConversions.toDoubleMatrix(result, newColNames);
        System.out.println("\nLAMP projection:");
        System.out.println("\tMin x: " + lampProjection.getColumn(0).min());
        System.out.println("\tMax x: " + lampProjection.getColumn(0).max());
        System.out.println("\tMin y: " + lampProjection.getColumn(1).min());
        System.out.println("\tMax y: " + lampProjection.getColumn(1).max());
        System.out.println("Number of NaNs: " + lampProjection.isNaN().sum());
    }

    @Test
    public void testBlobs() {
        ITable table = TestTables.getNdGaussianBlobs(10, 200, 15, 0.05);
        this.testLAMPMap(table, 20, 200);
    }

    @Test
    public void testMNIST() {
        try {
            ITable table = TestUtils.loadTableFromCSV("../data", "mnist.csv", "mnist.schema");
            this.testLAMPMap(table, 20, 5000);
        } catch (Exception e) {
            System.out.println("Skipping test because MNIST data is not present.");
        }
    }

    @Test
    public void testSegmentation() {
        try {
            ITable table = TestUtils.loadTableFromCSV("../data", "segmentation.csv", "segmentation.schema");
            this.testLAMPMap(table, 20, 200);
        } catch (Exception e) {
            System.out.println("Skipping test because MNIST data is not present.");
        }
    }
}
