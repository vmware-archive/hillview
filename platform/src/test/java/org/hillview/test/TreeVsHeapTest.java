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

import org.hillview.sketches.HeapTopK;
import org.hillview.sketches.TreeTopK;
import org.hillview.utils.Randomness;
import org.junit.Test;

public class TreeVsHeapTest extends BaseTest {
    private final int inpSize = 1000;
    private final int[] randInp = new int[this.inpSize];
    private final Randomness rn = new Randomness();

    @Test
    public void TreeVsHeapOne() {
        final int runs = 10;
        long startTime, endTime;
        //noinspection ConstantConditions
        for (int i = 1; i < runs; i++) {
            for (int j = 1; j < this.inpSize; j++) {
                this.randInp[j] = this.rn.nextInt(this.inpSize);
            }
            final int maxSize = 100;
            final HeapTopK<Integer> myHeap = new HeapTopK<Integer>(maxSize, Integer::compare);
            startTime = System.nanoTime();
            for (final int j: this.randInp) {
                myHeap.push(j);
            }
            endTime = System.nanoTime();
            PerfRegressionTest.comparePerf(" Using Heap: ", endTime - startTime);
            final TreeTopK<Integer> myTree = new TreeTopK<Integer>(maxSize, Integer::compare);
            startTime = System.nanoTime();
            for (final int j: this.randInp) {
                myTree.push(j);
            }
            endTime = System.nanoTime();
            PerfRegressionTest.comparePerf(" Using Tree: ", endTime - startTime);
        }
    }
}
