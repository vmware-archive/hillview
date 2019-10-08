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

package org.hillview.test.dataStructures;
import org.hillview.sketches.results.IntTreeTopK;
import org.hillview.test.BaseTest;
import org.hillview.test.TestUtil;
import org.hillview.utils.Randomness;
import org.junit.Test;

/**
 * Tests for TreeMap implementation f TopK.
 */
public class TreeTopKTest extends BaseTest {
    private final int maxSize = 10;

    @Test
    public void testHeapTopKZero() {
        final Randomness rn = this.getRandomness();
        IntTreeTopK myTree = new IntTreeTopK(this.maxSize, Integer::compare);
        for (int j =1; j <20; j++) {
            for (int i = 1; i < 1000; i++)
                myTree.push(rn.nextInt(10000));
            //System.out.println(myHeap.getTopK().toString());
        }
    }

    @Test
    public void testTreeTopKTimed() {
        final Randomness rn = this.getRandomness();
        final int inpSize = 1000000;
        final long startTime = System.nanoTime();
        IntTreeTopK myTree = new IntTreeTopK(this.maxSize, Integer::compare);
        for (int j = 1; j < inpSize; j++)
            myTree.push(rn.nextInt(inpSize));
        final long endTime = System.nanoTime();
        TestUtil.comparePerf(endTime - startTime);
        //System.out.format("Largest: %d%n", myTree.getTopK().lastKey());
    }
}
