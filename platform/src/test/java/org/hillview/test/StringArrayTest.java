/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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

import org.hillview.table.ColumnDescription;
import org.hillview.table.columns.StringArrayColumn;
import org.hillview.table.api.ContentsKind;
import org.junit.Test;

import static org.junit.Assert.*;

/*
 * Test for StringArrayColumn class.
*/
public class StringArrayTest {
    private final int size = 100;
    private final ColumnDescription desc = new ColumnDescription("test", ContentsKind.String, true);

    private void checkContents(StringArrayColumn col) {
        for (int i = 0; i < col.sizeInRows(); i++) {
            if ((i % 5) == 0) {
                assertTrue(col.isMissing(i));
            } else {
                assertFalse(col.isMissing(i));
                assertEquals(String.valueOf(i), col.getString(i));
            }
        }
    }

    /* Test for constructor using length and no arrays*/
    @Test
    public void testStringArrayZero() {
        final StringArrayColumn col = new StringArrayColumn(this.desc, this.size);
        for (int i = 0; i < this.size; i++) {
            if ((i % 5) == 0)
                col.setMissing(i);
            else
                col.set(i, String.valueOf(i));
        }
        assertEquals(col.sizeInRows(), this.size);
        checkContents(col);
    }

    /* Test for constructor using data array */
    @Test
    public void testStringArrayOne() {
        final String[] data = new String[this.size];
        for (int i = 0; i < this.size; i++)
            data[i] = String.valueOf(i);
        final StringArrayColumn col = new StringArrayColumn(this.desc, data);
        for (int i = 0; i < this.size; i++)
            if ((i % 5) == 0)
                col.setMissing(i);
        assertEquals(col.sizeInRows(), this.size);
        checkContents(col);
    }
}
