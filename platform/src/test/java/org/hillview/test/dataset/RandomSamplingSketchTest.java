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
import org.hillview.sketches.RandomSamplingSketch;
import org.hillview.table.SmallTable;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class RandomSamplingSketchTest extends BaseTest {
    @Test
    public void testRandomSampling() {
        long seed = 1;
        ITable table = TestTables.getNdGaussianBlobs(10, 1000, 15, 0.1);
        IDataSet<ITable> dataset = TestTables.makeParallel(table, 500);
        int numSamples = 20;
        double samplingRate = ((double) numSamples) / table.getNumOfRows();
        RandomSamplingSketch sketch = new RandomSamplingSketch(samplingRate, seed);
        SmallTable result = dataset.blockingSketch(sketch);
        Assert.assertNotNull(result);
        System.out.println(String.format("Result has %d rows.", result.getNumOfRows()));
        result = result.compress(result.getMembershipSet().sample(numSamples, 0));
        System.out.println(String.format("Resampled result has %d rows.", result.getNumOfRows()));
    }
}
