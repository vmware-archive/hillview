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
import org.hillview.table.columns.StringListColumn;
import org.hillview.table.api.ContentsKind;
import org.junit.Assert;
import org.junit.Test;

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

        Assert.assertEquals(col.sizeInRows(), size);
        for (int i = 0; i < size; i++) {
            if ((i % 5) == 0) {
                Assert.assertTrue(col.isMissing(i));
            } else {
                Assert.assertFalse(col.isMissing(i));
                Assert.assertEquals(String.valueOf(i), col.getString(i));
            }
        }
    }

    @Test
    public void testStringColumnSparse() {
        // exercises a corner case in column growth
        final StringListColumn col = new StringListColumn(this.desc);
        int ss = col.SegmentSize;
        for (int i = 0; i < (2 * ss); i++)
            col.appendMissing();

        col.append("2.0");
        Assert.assertNotNull(col);
    }
}
