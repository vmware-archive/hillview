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

package org.hillview.test;

import org.hillview.utils.IntSet;
import org.junit.Test;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;


public class IntSetTest {

    @Test
    public void TestIntSet() {
        final int size = 1000000;
        final IntSet IS = new IntSet(50,0.75F);
        for (int i = 0; i < size; i++ )
           IS.add(i);
        for (int i = 0; i < size; i++ )
            IS.add(i);
        assertEquals(size, IS.size());
        assertTrue(IS.contains(7));
        assertFalse(IS.contains(-200));
        final IntSet IS1 = IS.copy();
        for (int i = 0; i < size; i++ )
            IS1.add(i);
        assertEquals(size, IS1.size());
        assertTrue(IS1.contains(7));
        assertFalse(IS1.contains(-200));
    }
}
