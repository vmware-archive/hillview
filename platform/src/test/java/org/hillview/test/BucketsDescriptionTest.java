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

import org.hillview.sketches.BucketsDescription;
import org.hillview.sketches.BucketsDescriptionEqSize;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class BucketsDescriptionTest {
    @Test
    public void testEqSize() throws Exception {
        BucketsDescriptionEqSize bdEqSize = new BucketsDescriptionEqSize(0.5, 100.5, 100);
        assertEquals(bdEqSize.getNumOfBuckets(), 100);
        assertEquals(bdEqSize.indexOf(0.5), 0);
        assertEquals(bdEqSize.indexOf(0.6), 0);
        assertEquals(bdEqSize.indexOf(100.5), 99);
        assertEquals(bdEqSize.indexOf(100.4), 99);
        assertEquals(bdEqSize.indexOf(70.5), 70);
        assertEquals(bdEqSize.indexOf(30.6), 30);
        assertEquals(bdEqSize.getLeftBoundary(23), 23.5, .1);
        assertEquals(bdEqSize.getRightBoundary(23), 24.5, .1);
        assertEquals(bdEqSize.getRightBoundary(99), 100.5, .1);
    }

    @Test
    public void testGeneric1D() throws Exception {
        double[] boundaries = new double[101];
        for (int i = 0; i < 101; i++)
            boundaries[i] = i + 0.5;
        BucketsDescription bdEq = new BucketsDescription(boundaries);
        assertEquals(bdEq.getNumOfBuckets(), 100);
        assertEquals(bdEq.indexOf(0.5), 0);
        assertEquals(bdEq.indexOf(0.6), 0);
        assertEquals(bdEq.indexOf(100.5), 99);
        assertEquals(bdEq.indexOf(100.4), 99);
        assertEquals(bdEq.indexOf(70.5), 70);
        assertEquals(bdEq.indexOf(30.6), 30);
        assertEquals(bdEq.getLeftBoundary(23), 23.5, .1);
        assertEquals(bdEq.getRightBoundary(23), 24.5, .1);
        assertEquals(bdEq.getRightBoundary(99), 100.5, .1);
        double[] boundaries1 = new double[101];
        for (int i = 0; i < 101; i++)
            boundaries1[i] = i + 0.4;
        BucketsDescription bdEq1 = new BucketsDescription(boundaries1);
        assertFalse(bdEq.equals(bdEq1));
    }
}
