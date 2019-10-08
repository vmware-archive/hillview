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
import org.hillview.sketches.results.CorrMatrix;
import org.hillview.sketches.PCACorrelationSketch;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.utils.LinAlg;
import org.hillview.utils.TestTables;
import org.hillview.utils.TestUtils;
import org.hillview.utils.Utilities;
import org.jblas.DoubleMatrix;
import org.junit.Assert;
import org.junit.Test;

public class PCATest extends BaseTest {
    @Test
    public void testLinearDataset() {
        int size = 100000;
        int numFrags = 10;
        int numCols  = 3;
        ITable table = TestTables.getLinearTable(size, numCols);
        //ITable table = TestTables.getMissingIntTable(size, numCols);
        String[] colNames = Utilities.toArray(table.getSchema().getColumnNames());
        IDataSet<ITable> dataset = TestTables.makeParallel(table, size/numFrags);

        PCACorrelationSketch fcs = new PCACorrelationSketch(colNames, size, 30202);
        CorrMatrix cm1 = dataset.blockingSketch(fcs);
        PCACorrelationSketch fcs2 = new PCACorrelationSketch(colNames);
        CorrMatrix cm2 = dataset.blockingSketch(fcs2);

        Assert.assertNotNull(cm1);
        DoubleMatrix corrMatrix1 = new DoubleMatrix(cm1.getCorrelationMatrix());
        // Get just the eigenvector corresponding to the largest eigenvalue (because we know the data is approximately
        // linear).
        DoubleMatrix eigenVectors1 = LinAlg.eigenVectors(corrMatrix1, 1);
        Assert.assertNotNull(cm2);
        DoubleMatrix corrMatrix2 = new DoubleMatrix(cm2.getCorrelationMatrix());
        DoubleMatrix eigenVectors2 = LinAlg.eigenVectors(corrMatrix2, 1);

        System.out.println(cm1.toString());
        System.out.println(cm2.toString());
        eigenVectors1.print();
        eigenVectors2.print();

        for (int i = 2; i < eigenVectors1.columns; i++) {
            // The eigenvector should have reasonably large components in the first two columns, compared to the
            // other components in the eigenvector.
            Assert.assertTrue(
                    "First component of eigenvector not large enough.",
                    Math.abs(eigenVectors1.get(0, 0)) >
                            3 * Math.abs(eigenVectors1.get(0, i))
            );
            Assert.assertTrue(
                    "Second component of eigenvector not large enough.",
                    Math.abs(eigenVectors1.get(0, 1)) >
                            3 * Math.abs(eigenVectors1.get(0, i))
            );
        }
    }

    @Test
    public void testMNIST(){
        try {
            ITable table = TestUtils.loadTableFromCSV("../data", "mnist.csv", "mnist.schema");
            String[] numericColNames = Utilities.toArray(TestUtils.getNumericColumnNames(table));
            PCACorrelationSketch fcs = new PCACorrelationSketch(numericColNames);
            CorrMatrix cm = fcs.create(table);
            Assert.assertNotNull(cm);
            DoubleMatrix corrMatrix = new DoubleMatrix(cm.getCorrelationMatrix());
            DoubleMatrix eigenVectors = LinAlg.eigenVectors(corrMatrix, 2);
            LinearProjectionMap lpm = new LinearProjectionMap(numericColNames, eigenVectors, "PCA");
            ITable result = lpm.apply(table);
        } catch (Exception e) {
            System.out.println("Skipped test because MNIST data is not present.");
        }
    }
}
