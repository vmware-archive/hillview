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

package org.hillview.test.table;

import org.hillview.table.ColumnDescription;
import org.hillview.table.api.IMembershipSet;
import org.hillview.table.api.Interval;
import org.hillview.table.columns.*;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

public class ColumnTest extends BaseTest {
    @Test
    public void testContentsKind() {
        Object o = ContentsKind.Integer.minimumValue();
        Assert.assertNotNull(o);
        Assert.assertEquals(Integer.MIN_VALUE, o);
        o = ContentsKind.Date.minimumValue();
        Instant now = Instant.now();
        Assert.assertNotNull(o);
        Assert.assertTrue(now.isAfter((Instant)o));
        o = ContentsKind.String.minimumValue();
        Assert.assertEquals("", o);
        o = ContentsKind.Duration.minimumValue();
        Instant later = Instant.now();
        Duration duration = Duration.between(now, later);
        Assert.assertNotNull(o);
        Assert.assertEquals(1, duration.compareTo((Duration)o));
        o = ContentsKind.Double.minimumValue();
        Assert.assertNotNull(o);
        Assert.assertEquals(-Double.MAX_VALUE, o);
    }

    @Test
    public void testIntColumn() {
        final IntArrayColumn col;
        final int size = 100;

        final ColumnDescription desc = new ColumnDescription("test", ContentsKind.Integer);
        col = new IntArrayColumn(desc, size);
        for (int i = 0; i < size; i++)
            col.set(i, i);

        Assert.assertEquals(size, col.sizeInRows());
        Assert.assertEquals(0, col.getInt(0));
        for (int i = 0; i < size; i++)
            Assert.assertEquals(i, col.getInt(i));
        Assert.assertEquals(0.0, col.asDouble(0), 1e-3);
    }

    @Test
    public void testIntervalListColumn() {
        final int size = 10000000;
        final ColumnDescription desc = new ColumnDescription("test", ContentsKind.Double);
        final DoubleListColumn col = new DoubleListColumn(desc);
        for (int i = 0; i < size; i++)
            col.append((double) i);

        final ColumnDescription interval = new ColumnDescription("interval", ContentsKind.Interval);
        final IntervalColumn ic = new IntervalColumn(interval, col, col);
        Assert.assertEquals(ic.sizeInRows(), size);
        Interval in = ic.getInterval(0);
        Assert.assertNotNull(in);
        Assert.assertEquals( 0.0, in.get(true), 10e-3);
        Assert.assertEquals( 0.0, in.get(false), 10e-3);
        for (int i = 0; i < size; i++) {
            Assert.assertFalse(ic.isMissing(i));
            Assert.assertEquals(i, ic.getEndpoint(i, true), 1e-3);
            Assert.assertEquals(i, ic.getEndpoint(i, false), 1e-3);
        }
    }

    @Test
    public void testDoubleListColumn() {
        final int size = 10000000;
        final ColumnDescription desc = new ColumnDescription("test0", ContentsKind.Double);
        final DoubleListColumn col = new DoubleListColumn(desc);
        for (int i = 0; i < size; i++)
            col.append((double) i);

        Assert.assertEquals(col.sizeInRows(), size);
        Assert.assertEquals(col.getDouble(0), 0.0, 10e-3);
        for (int i = 0; i < size; i++)
            Assert.assertEquals(i, col.getDouble(i), 1e-3);
        Assert.assertEquals(col.asDouble(0), 0.0, 1e-3);
    }

    @Test
    public void testTimeListColumn() {
        final int size = 10000000;
        final ColumnDescription desc = new ColumnDescription("test0", ContentsKind.Time);
        final DoubleListColumn col = new DoubleListColumn(desc);
        for (int i = 0; i < size; i++)
            col.append((double) i);

        Assert.assertEquals(col.sizeInRows(), size);
        Assert.assertEquals(col.getDouble(0), 0.0, 10e-3);
        for (int i = 0; i < size; i++)
            Assert.assertEquals(i, col.getDouble(i), 1e-3);
        Assert.assertEquals(col.asDouble(0), 0.0, 1e-3);
    }

    @Test
    public void testStringColumn() {
        ColumnDescription desc = new ColumnDescription("Cat", ContentsKind.String);
        StringListColumn col = new StringListColumn(desc);
        col.append("First");
        col.append("First");
        col.append(null);
        col.appendMissing();
        col.append("Second");
        for (int i = 0; i < 100000; i++) {
            col.append(Integer.toString(i));
            String str = col.getString(i * 2 + 5);
            Assert.assertEquals(Integer.toString(i), str);
            col.append("First");
            str = col.getString(i * 2 + 6);
            Assert.assertEquals("First", str);
        }
        col.seal();
        Assert.assertTrue(col.isMissing(2));
        Assert.assertTrue(col.isMissing(3));
        Assert.assertFalse(col.isMissing(0));
        Assert.assertEquals(col.getString(0), "First");
        Assert.assertEquals(col.getString(1), "First");
        Assert.assertNull(col.getString(2));
        Assert.assertEquals(col.getString(4), "Second");
        Assert.assertEquals(col.getString(264), "First");
        Assert.assertEquals(col.getString(265), "130");
        Assert.assertEquals(col.getString(266), "First");
        Assert.assertEquals(col.getString(100004), "First");

        Assert.assertEquals(200005, col.sizeInRows());
        int nulls = 0;
        int firstCount = 0;
        int otherCount = 0;
        for (int i=0; i < col.sizeInRows(); i++) {
            String str = col.getString(i);
            if (str == null)
                nulls++;
            else if (str.equals("First"))
                firstCount++;
            else
                otherCount++;
        }
        Assert.assertEquals(2, nulls);
        Assert.assertEquals(100002, firstCount);
        Assert.assertEquals(col.sizeInRows() - nulls - firstCount, otherCount);
    }

    @Test
    public void testSetComparisonColumn() {
        int rowCount = 6;
        IMembershipSet set0 = new FullMembershipSet(rowCount);
        IMembershipSet set1 = set0.filter(i -> i < 3);
        IMembershipSet set2 = set0.filter(i -> i >= 2);
        SetComparisonColumn col = new SetComparisonColumn(
                "C3", new IMembershipSet[] { set0, set1, set2 }, new String[] { "A", "B", "C" });
        Assert.assertEquals(rowCount, col.sizeInRows());
        for (int i = 0; i < 2; i++)
            Assert.assertEquals("A,B", col.getString(i));
        Assert.assertEquals("All", col.getString(2));
        for (int i = 3; i < rowCount; i++)
            Assert.assertEquals("A,C", col.getString(i));
    }
}
