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

package org.hillview.test;

import org.hillview.maps.FilterMap;
import org.hillview.table.Table;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.filters.ComparisonFilterDescription;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class ComparisonFilterTest extends BaseTest {
    @Test
    public void testFilterStringColumn() {
        Table table = TestTables.testRepTable();
        ComparisonFilterDescription filter = new ComparisonFilterDescription(
                "Name", "Ed", "==");
        FilterMap filterMap = new FilterMap(filter);
        ITable result = filterMap.apply(table);

        IColumn col = result.getLoadedColumn("Name");
        IRowIterator it = result.getMembershipSet().getIterator();
        int row = it.getNextRow();
        while (row != -1) {
            Assert.assertEquals("Ed", col.getString(row));
            row = it.getNextRow();
        }

        filter = new ComparisonFilterDescription(
                "Name", "Ed", "<=");
        filterMap = new FilterMap(filter);
        result = filterMap.apply(table);
        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            String str = col.getString(row);
            Assert.assertTrue(str == null || "Ed".compareTo(str) <= 0);
            row = it.getNextRow();
        }

        filter = new ComparisonFilterDescription(
                "Name", "Ed", ">=");
        filterMap = new FilterMap(filter);
        result = filterMap.apply(table);
        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            String str = col.getString(row);
            Assert.assertNotNull(str);
            Assert.assertTrue("Ed".compareTo(str) >= 0);
            row = it.getNextRow();
        }

        filter = new ComparisonFilterDescription(
                "Name", "Ed", "!=");
        filterMap = new FilterMap(filter);
        result = filterMap.apply(table);
        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            String str = col.getString(row);
            Assert.assertTrue(str == null || "Ed".compareTo(str) != 0);
            row = it.getNextRow();
        }

        filter = new ComparisonFilterDescription(
                "Name", "Ed", "<");
        filterMap = new FilterMap(filter);
        result = filterMap.apply(table);
        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            String str = col.getString(row);
            Assert.assertTrue(str == null || "Ed".compareTo(str) < 0);
            row = it.getNextRow();
        }

        filter = new ComparisonFilterDescription(
                "Name", "Ed", ">");
        filterMap = new FilterMap(filter);
        result = filterMap.apply(table);
        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            String str = col.getString(row);
            Assert.assertNotNull(str);
            Assert.assertTrue("Ed".compareTo(str) > 0);
            row = it.getNextRow();
        }
    }

    @Test
    public void testFilterIntColumn() {
        Table table = TestTables.testRepTable();
        ComparisonFilterDescription filter = new ComparisonFilterDescription(
                "Age", "10", "==");
        FilterMap filterMap = new FilterMap(filter);
        ITable result = filterMap.apply(table);

        IColumn col = result.getLoadedColumn("Age");
        IRowIterator it = result.getMembershipSet().getIterator();
        int row = it.getNextRow();
        while (row != -1) {
            Assert.assertEquals(10, col.getInt(row));
            row = it.getNextRow();
        }

        filter = new ComparisonFilterDescription(
                "Age", "10", "<=");
        filterMap = new FilterMap(filter);
        result = filterMap.apply(table);
        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            if (col.isMissing(row))
                continue;
            int i = col.getInt(row);
            Assert.assertTrue(10 <= i);
            row = it.getNextRow();
        }

        filter = new ComparisonFilterDescription(
                "Age", "10", ">=");
        filterMap = new FilterMap(filter);
        result = filterMap.apply(table);
        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            Assert.assertFalse(col.isMissing(row));
            int i = col.getInt(row);
            Assert.assertTrue(10 >= i);
            row = it.getNextRow();
        }

        filter = new ComparisonFilterDescription(
                "Age", "10", "!=");
        filterMap = new FilterMap(filter);
        result = filterMap.apply(table);
        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            if (col.isMissing(row))
                continue;
            int i = col.getInt(row);
            Assert.assertTrue(10 != i);
            row = it.getNextRow();
        }

        filter = new ComparisonFilterDescription(
                "Age", "10", "<");
        filterMap = new FilterMap(filter);
        result = filterMap.apply(table);
        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            if (col.isMissing(row))
                continue;
            int i = col.getInt(row);
            Assert.assertTrue(10 < i);
            row = it.getNextRow();
        }

        filter = new ComparisonFilterDescription(
                "Age", "10", ">");
        filterMap = new FilterMap(filter);
        result = filterMap.apply(table);
        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            Assert.assertFalse(col.isMissing(row));
            int i = col.getInt(row);
            Assert.assertTrue(10 > i);
            row = it.getNextRow();
        }
    }
}
