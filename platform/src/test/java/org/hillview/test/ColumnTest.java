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

import org.hillview.table.ColumnDescription;
import org.hillview.table.NoStringConverter;
import org.hillview.table.columns.CategoryListColumn;
import org.hillview.table.columns.DoubleListColumn;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.columns.IntArrayColumn;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ColumnTest extends BaseTest {
    @Test
    public void testIntColumn() {
        final IntArrayColumn col;
        final int size = 100;

        final ColumnDescription desc = new ColumnDescription("test", ContentsKind.Integer, false);
        col = new IntArrayColumn(desc, size);
        for (int i = 0; i < size; i++)
            col.set(i, i);

        assertEquals(col.sizeInRows(), size);
        assertEquals(col.getInt(0), 0);
        for (int i = 0; i < size; i++)
            assertEquals(i, col.getInt(i));
        assertEquals(col.asDouble(0, NoStringConverter.getConverterInstance()), 0.0, 1e-3);
    }

    @Test
    public void testDoubleListColumn() {
        final DoubleListColumn col;
        final int size = 10000000;

        final ColumnDescription desc = new ColumnDescription("test", ContentsKind.Double, false);
        col = new DoubleListColumn(desc);
        for (int i = 0; i < size; i++)
            col.append((double) i);

        assertEquals(col.sizeInRows(), size);
        assertEquals(col.getDouble(0), 0.0, 10e-3);
        for (int i = 0; i < size; i++)
            assertEquals((double) i, col.getDouble(i), 1e-3);
        assertEquals(col.asDouble(0, null), 0.0, 1e-3);
    }

    @Test
    public void testCategoryColumn() {
        ColumnDescription desc = new ColumnDescription("Cat", ContentsKind.Category, true);
        CategoryListColumn col = new CategoryListColumn(desc);
        col.append("First");
        col.append("First");
        col.append((String) null);
        col.appendMissing();
        col.append("Second");
        for (int i = 0; i < 100000; i++) {
            col.append(Integer.toString(i));
            col.append("First");
        }
        col.seal();
        Assert.assertTrue(col.isMissing(2));
        Assert.assertTrue(col.isMissing(3));
        Assert.assertFalse(col.isMissing(0));
        Assert.assertEquals(col.getString(0), "First");
        Assert.assertEquals(col.getString(1), "First");
        Assert.assertEquals(col.getString(2), null);
        Assert.assertEquals(col.getString(4), "Second");
        Assert.assertEquals(col.getString(264), "First");
        Assert.assertEquals(col.getString(265), "130");
        Assert.assertEquals(col.getString(266), "First");
        Assert.assertEquals(col.getString(100004), "First");
    }
}
