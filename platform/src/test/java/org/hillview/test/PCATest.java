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

import org.hillview.dataset.api.IDataSet;
import org.hillview.maps.LinearProjectionMap;
import org.hillview.sketches.CorrMatrix;
import org.hillview.sketches.FullCorrelationSketch;
import org.hillview.table.api.ITable;
import org.hillview.utils.LinAlg;
import org.hillview.utils.TestTables;
import org.hillview.utils.TestUtils;
import org.jblas.DoubleMatrix;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PCATest extends BaseTest {
    @Test
    public void testLinearDataset() {
        ITable table = TestTables.getLinearTable(10000, 30);
        List<String> colNames = new ArrayList<String>(table.getSchema().getColumnNames());

        IDataSet<ITable> dataset = TestTables.makeParallel(table, 1000);

        FullCorrelationSketch fcs = new FullCorrelationSketch(colNames);
        CorrMatrix cm = dataset.blockingSketch(fcs);

        DoubleMatrix corrMatrix = new DoubleMatrix(cm.getCorrelationMatrix());
        // Get just the eigenvector corresponding to the largest eigenvalue (because we know the data is approximately
        // linear).
        DoubleMatrix eigenVectors = LinAlg.eigenVectors(corrMatrix, 1);
        eigenVectors.print();

        for (int i = 2; i < eigenVectors.columns; i++) {
            // The eigenvector should have reasonably large components in the first two columns, compared to the
            // other components in the eigenvector.
            Assert.assertTrue(
                    "First component of eigenvector not large enough.",
                    Math.abs(eigenVectors.get(0, 0)) > 3 * Math.abs(eigenVectors.get(0, i))
            );
            Assert.assertTrue(
                    "Second component of eigenvector not large enough.",
                    Math.abs(eigenVectors.get(0, 1)) > 3 * Math.abs(eigenVectors.get(0, i))
            );
        }
    }

    @Test
    public void testMNIST(){
        try {
            ITable table = TestUtils.loadTableFromCSV("../data", "mnist.csv", "mnist.schema");
            List<String> numericColNames = TestUtils.getNumericColumnNames(table);

            FullCorrelationSketch fcs = new FullCorrelationSketch(numericColNames);
            CorrMatrix cm = fcs.create(table);
            DoubleMatrix corrMatrix = new DoubleMatrix(cm.getCorrelationMatrix());
            DoubleMatrix eigenVectors = LinAlg.eigenVectors(corrMatrix, 2);
            LinearProjectionMap lpm = new LinearProjectionMap(numericColNames, eigenVectors, "PCA");
            ITable result = lpm.apply(table);
        } catch (IOException e) {
            System.out.println("Skipped test because MNIST data is not present.");
        }
    }
}
