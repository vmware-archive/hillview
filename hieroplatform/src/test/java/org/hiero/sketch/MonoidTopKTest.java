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

import org.hiero.sketches.MonoidTopK;
import org.hiero.sketches.TreeTopK;
import org.hiero.utils.Converters;
import org.hiero.utils.Randomness;
import org.junit.Test;

import java.util.SortedMap;
import static org.junit.Assert.assertTrue;


public class MonoidTopKTest {
    private final int maxSize =1000;
    private int lSize;
    private int rSize;
    private final int inpSize = 10000;
    private final MonoidTopK<Integer> myTopK = new MonoidTopK<Integer>(this.maxSize, Integer::compare);

    void checkSorted(SortedMap<Integer, Integer> t) {
        boolean first = true;
        int previous = 0;
        for (int k : t.keySet()) {
            if (!first)
                assertTrue(previous < k);
            previous = k;
            first = false;
        }
    }

    @Test
    public void MonoidTopKTest0() {
        this.lSize = 100;
        this.rSize = 100;
        TreeTopK<Integer> leftTree = new TreeTopK<Integer>(this.lSize, Integer::compare);
        TreeTopK<Integer> rightTree = new TreeTopK<Integer>(this.rSize, Integer::compare);
        final Randomness rn = new Randomness();
        for (int i = 0; i < this.inpSize; i++)
            leftTree.push(rn.nextInt(this.inpSize));
        for (int j = 0; j < this.inpSize; j++)
            rightTree.push(rn.nextInt(this.inpSize));
        this.checkSorted(leftTree.getTopK());
        this.checkSorted(rightTree.getTopK());
        SortedMap<Integer, Integer> s = this.myTopK.add(leftTree.getTopK(), rightTree.getTopK());
        this.checkSorted(Converters.checkNull(s));
    }

    @Test
    public void MonoidTopKTest1() {
        this.lSize = 50;
        this.rSize = 50;
        TreeTopK<Integer> leftTree = new TreeTopK<Integer>(this.lSize, Integer::compare);
        TreeTopK<Integer> rightTree = new TreeTopK<Integer>(this.rSize, Integer::compare);
        final Randomness rn = new Randomness();
        for (int i = 0; i < this.inpSize; i++)
            leftTree.push(rn.nextInt(this.inpSize));
        for (int j = 0; j < this.inpSize; j++)
            rightTree.push(rn.nextInt(this.inpSize));
        this.checkSorted(leftTree.getTopK());
        this.checkSorted(rightTree.getTopK());
        SortedMap<Integer, Integer> s = this.myTopK.add(leftTree.getTopK(), rightTree.getTopK());
        this.checkSorted(Converters.checkNull(s));
    }

    @Test
    public void MonoidTopKTestTimed() {
        this.lSize = 1000;
        this.rSize = 1000;
        TreeTopK<Integer> leftTree = new TreeTopK<Integer>(this.lSize, Integer::compare);
        TreeTopK<Integer> rightTree = new TreeTopK<Integer>(this.rSize, Integer::compare);
        final Randomness rn = new Randomness();
        for (int i = 0; i < this.inpSize; i++)
            leftTree.push(rn.nextInt(this.inpSize));
        for (int j = 0; j < this.inpSize; j++)
            rightTree.push(rn.nextInt(this.inpSize));
        final long startTime = System.nanoTime();
        this.myTopK.add(leftTree.getTopK(), rightTree.getTopK());
        final long endTime = System.nanoTime();
        PerfRegressionTest.comparePerf(endTime - startTime);
    }
}

