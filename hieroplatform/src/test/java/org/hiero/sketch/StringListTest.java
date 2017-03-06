/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0 2
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

import org.hiero.sketch.table.ColumnDescription;
import org.hiero.sketch.table.StringListColumn;
import org.hiero.sketch.table.api.ContentsKind;
import org.junit.Test;

import static junit.framework.TestCase.*;

/*
 * Test for StringArrayColumn class.
*/
public class StringListTest {
    private final ColumnDescription desc = new ColumnDescription("test", ContentsKind.String, true);

    @Test
    public void testStringArrayZero() {
        final StringListColumn col = new StringListColumn(this.desc);
        final int size = 1000;
        for (int i = 0; i < size; i++) {
            if ((i % 5) == 0) {
                col.appendMissing();
            } else {
                col.append(String.valueOf(i));
            }
        }
        assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            if ((i % 5) == 0) {
                assertTrue(col.isMissing(i));
            } else {
                assertFalse(col.isMissing(i));
                assertEquals(String.valueOf(i), col.getString(i));
            }
        }
    }
}
