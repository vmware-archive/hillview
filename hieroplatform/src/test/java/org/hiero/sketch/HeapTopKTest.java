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
 *
 */

package org.hiero.sketch;
import org.hiero.sketch.spreadsheet.HeapTopK;
import org.hiero.utils.Randomness;
import org.junit.Test;


/**
 * Tests for HashMap Implementation of TopK
 */
public class HeapTopKTest {
    private final int maxSize = 10;
    private final HeapTopK<Integer> myHeap = new HeapTopK<Integer>(this.maxSize, MyCompare.instance);

    @Test
    public void testHeapTopKZero() {
        final Randomness rn = Randomness.getInstance();
        for (int j =1; j <20; j++) {
            for (int i = 1; i < 1000; i++)
                this.myHeap.push(rn.nextInt(10000));
            //System.out.println(myHeap.getTopK().toString());
        }
    }

    @Test
    public void testHeapTopKTimed() {
        final Randomness rn = Randomness.getInstance();
        final int inpSize = 1000000;
        final long startTime = System.nanoTime();
        for (int j = 1; j < inpSize; j++)
            this.myHeap.push(rn.nextInt(inpSize));
        final long endTime = System.nanoTime();
        PerfRegressionTest.comparePerf(endTime - startTime);
        //System.out.format("Largest: %d%n", myHeap.getTopK().lastKey());
    }
}
