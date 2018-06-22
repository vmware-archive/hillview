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

import org.hillview.sketches.StringBucketsDescription;
import org.hillview.sketches.BucketsDescriptionEqSize;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class BucketsDescriptionTest extends BaseTest {
    @Test
    public void testEqSize() {
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
    public void testGeneric1D() {
        String[] boundaries = { "Apple", "Bad", "China", "Rome", "Zetta" };
        StringBucketsDescription b = new StringBucketsDescription(boundaries);
        assertEquals(b.getNumOfBuckets(), 4);
        assertEquals(b.indexOf("Aardwark"), -1);
        assertEquals(b.indexOf("Apple"), 0);
        assertEquals(b.indexOf("Away"), 1);
        assertEquals(b.indexOf(""), -1);
        assertEquals(b.indexOf("Bad"), 1);
        assertEquals(b.indexOf("Zz"), -1);
        assertEquals(b.indexOf("Z"), 4);
        assertEquals(b.indexOf("Zetta"), 4);
    }
}
