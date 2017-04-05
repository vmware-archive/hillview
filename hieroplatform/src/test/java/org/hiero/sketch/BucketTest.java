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

import org.hiero.sketches.Bucket1D;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BucketTest {
    @Test
    public void testBucket() throws Exception {
        final Bucket1D myBucket = new Bucket1D();
        final Bucket1D myBucket1 = new Bucket1D();
        assertTrue(myBucket.isEmpty());
        for (int i = 0; i < 100; i++) {
            myBucket.add((double) i, Integer.toString(i));
            myBucket1.add( 99.5 - i , Double.toString(99.5 - i));
        }
        assertEquals(myBucket.getCount(), 100);
        assertEquals(myBucket1.getCount(), 100);
        assertEquals(myBucket1.getMinValue(), 0.5, .1);
        assertEquals(myBucket1.getMaxValue(), 99.5, .1);
        final Bucket1D myBucket2 = myBucket.union(myBucket1);
        assertEquals(myBucket2.getCount(), 200);
        assertEquals(myBucket2.getMinValue(), 0.0, .1);
        assertEquals(myBucket2.getMaxValue(), 99.5, .1);
        assertEquals(myBucket2.getMinObject(), "0");
        assertEquals(myBucket2.getMaxObject(), "99.5");
    }
}
