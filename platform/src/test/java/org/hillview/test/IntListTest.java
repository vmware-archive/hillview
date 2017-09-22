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

import org.hillview.table.ColumnDescription;
import org.hillview.table.IntListColumn;
import org.hillview.table.api.ContentsKind;
import org.junit.Test;
import static org.junit.Assert.*;

public class IntListTest {
    private final ColumnDescription desc = new ColumnDescription("test", ContentsKind.Integer, true);

    /* Test for constructor using length and no arrays*/
    @Test
    public void testIntListColumnZero() {
        final IntListColumn col = new IntListColumn(this.desc);
        final int size = 10000;
        for (int i = 0; i < size; i++) {
            if ((i % 5) != 0)
                col.append(i);
            if ((i % 5) == 0)
                col.appendMissing();
        }
        assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            if ((i % 5) == 0)
                assertTrue(col.isMissing(i));
            else {
                assertFalse(col.isMissing(i));
                assertEquals(i, col.getInt(i));
            }
        }
    }
}
