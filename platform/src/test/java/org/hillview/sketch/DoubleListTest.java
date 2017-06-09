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

package org.hillview.sketch;

import org.hillview.table.ColumnDescription;
import org.hillview.table.DoubleListColumn;
import org.hillview.table.api.ContentsKind;
import org.junit.Test;
import static java.lang.Math.sqrt;
import static org.junit.Assert.*;

public class DoubleListTest {
    private final ColumnDescription desc = new ColumnDescription("test", ContentsKind.Double, true);

    /* Test for constructor using length and no arrays*/
    @Test
    public void testIntListColumnZero() {
        final DoubleListColumn col = new DoubleListColumn(this.desc);
        final int size = 10000;
        for (int i = 0; i < size; i++) {
            if ((i % 5) != 0)
                col.append(sqrt(i+1));
            if ((i % 5) == 0)
                col.appendMissing();
        }
        assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            if ((i % 5) == 0)
                assertTrue(col.isMissing(i));
            else {
                assertFalse(col.isMissing(i));
                assertEquals(sqrt(i+1), col.getDouble(i), 1.0E-03);
            }
        }
    }
}
