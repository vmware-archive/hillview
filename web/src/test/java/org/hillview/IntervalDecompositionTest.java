/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

package org.hillview;

import org.hillview.dataStructures.NumericIntervalDecomposition;
import org.hillview.dataset.api.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class IntervalDecompositionTest {
    @Test
    public void testDyadicDecomposition() {
        int leftLeafIdx = 0;
        int rightLeafIdx = 10;

        ArrayList<Pair<Integer, Integer>> ret =
                NumericIntervalDecomposition.dyadicDecomposition(leftLeafIdx, rightLeafIdx);
        Assert.assertNotNull(ret);
        Pair<Integer, Integer> e = ret.get(0);
        Assert.assertNotNull(e);
        Assert.assertNotNull(e.first);
        Assert.assertNotNull(e.second);
        assert(e.first == 0);
        assert(e.second == 8);

        e = ret.get(1);
        Assert.assertNotNull(e.first);
        Assert.assertNotNull(e.second);
        assert(e.first == 8);
        assert(e.second == 2);
    }

    @Test
    public void testThreeadicDecomposition() {
        int leftLeafIdx = 0;
        int rightLeafIdx = 10;

        ArrayList<Pair<Integer, Integer>> ret =
                NumericIntervalDecomposition.kadicDecomposition(leftLeafIdx, rightLeafIdx, 3);
        Assert.assertNotNull(ret);
        Pair<Integer, Integer> e = ret.get(0);
        Assert.assertNotNull(e);
        Assert.assertNotNull(e.first);
        Assert.assertNotNull(e.second);
        assert(e.first == 0);
        assert(e.second == 9);

        e = ret.get(1);
        Assert.assertNotNull(e.first);
        Assert.assertNotNull(e.second);
        assert(e.first == 9);
        assert(e.second == 1);
    }


    @Test
    public void testNadicDecomposition() {
        int leftLeafIdx = 0;
        int rightLeafIdx = 10;

        ArrayList<Pair<Integer, Integer>> ret =
                NumericIntervalDecomposition.kadicDecomposition(leftLeafIdx, rightLeafIdx, 10);
        Assert.assertNotNull(ret);
        assert(ret.size() == 10);
        Pair<Integer, Integer> e = ret.get(0);
        Assert.assertNotNull(e);
        Assert.assertNotNull(e.first);
        Assert.assertNotNull(e.second);
        assert(e.first == 0);
        assert(e.second == 1);
    }
}
