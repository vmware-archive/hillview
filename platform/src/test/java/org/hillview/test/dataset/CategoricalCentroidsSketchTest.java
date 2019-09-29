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
import org.hillview.sketches.CategoryCentroidsSketch;
import org.hillview.sketches.Centroids;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class CategoricalCentroidsSketchTest extends BaseTest {
    @Test
    public void testFruits() {
        ITable table = TestTables.getCentroidTestTable();
        CategoryCentroidsSketch sketch = new CategoryCentroidsSketch(
                "FruitType",
                new String[] { "x", "y" }
        );
        Centroids<String> centroids = sketch.create(table);
        Assert.assertNotNull(centroids);
        HashMap<String, double[]> centroidsMap = centroids.computeCentroids();
        Assert.assertEquals(2, centroidsMap.get("Banana")[0], Math.ulp(2));
        Assert.assertEquals(11, centroidsMap.get("Banana")[1], Math.ulp(11));
        Assert.assertEquals(5, centroidsMap.get("Orange")[0], Math.ulp(5));
        Assert.assertEquals(26, centroidsMap.get("Orange")[1], Math.ulp(26));

        IDataSet<ITable> dataset = TestTables.makeParallel(table, 2);
        centroids = dataset.blockingSketch(sketch);
        Assert.assertNotNull(centroids);
        centroidsMap = centroids.computeCentroids();
        Assert.assertEquals(2, centroidsMap.get("Banana")[0], Math.ulp(2));
        Assert.assertEquals(11, centroidsMap.get("Banana")[1], Math.ulp(11));
        Assert.assertEquals(5, centroidsMap.get("Orange")[0], Math.ulp(5));
        Assert.assertEquals(26, centroidsMap.get("Orange")[1], Math.ulp(26));
    }
}
