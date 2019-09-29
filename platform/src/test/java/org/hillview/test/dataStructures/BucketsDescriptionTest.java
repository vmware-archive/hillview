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

import org.hillview.sketches.DoubleHistogramBuckets;
import org.hillview.sketches.StringHistogramBuckets;
import org.hillview.test.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


class BucketsDescriptionTest extends BaseTest {
    @Test
    public void testEqSize() {
        DoubleHistogramBuckets bdEqSize = new DoubleHistogramBuckets(0.5, 100.5, 100);
        assertEquals(bdEqSize.getNumOfBuckets(), 100);
        assertEquals(bdEqSize.indexOf(0.5), 0);
        assertEquals(bdEqSize.indexOf(0.6), 0);
        assertEquals(bdEqSize.indexOf(100.5), 99);
        assertEquals(bdEqSize.indexOf(100.4), 99);
        assertEquals(bdEqSize.indexOf(70.5), 70);
        assertEquals(bdEqSize.indexOf(30.6), 30);
    }

    @Test
    public void testGeneric1D() {
        String[] boundaries = { "Apple", "Bad", "China", "Rome", "Zetta" };
        StringHistogramBuckets b = new StringHistogramBuckets(boundaries);
        assertEquals(5, b.getNumOfBuckets());
        assertEquals(-1, b.indexOf("Aardwark"));
        assertEquals(0, b.indexOf("Apple"));
        assertEquals(0, b.indexOf("Away"));
        assertEquals(-1, b.indexOf(""));
        assertEquals(1, b.indexOf("Bad"));
        assertEquals(1, b.indexOf("Bubble"));
        assertEquals(2, b.indexOf("China"));
        assertEquals(2, b.indexOf("Cocoon"));
        assertEquals(3, b.indexOf("Rome"));
        assertEquals(3, b.indexOf("Sudden"));
        assertEquals(3, b.indexOf("Z"));
        assertEquals(4, b.indexOf("Zetta"));
        assertEquals(4, b.indexOf("Zz"));
    }
}
