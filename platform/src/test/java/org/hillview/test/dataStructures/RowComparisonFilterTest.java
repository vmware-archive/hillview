/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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

package org.hillview.test.dataStructures;

import org.hillview.maps.FilterMap;
import org.hillview.sketches.results.ColumnSortOrientation;
import org.hillview.table.ColumnDescription;
import org.hillview.table.RecordOrder;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.filters.RowComparisonFilterDescription;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.test.BaseTest;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class RowComparisonFilterTest extends BaseTest {
    @Test
    public void testFilterStringColumn() {
        Table table = TestTables.testRepTable();
        Assert.assertEquals("Table[2x15]", table.toString());
        Schema schema = table.getSchema();
        ColumnDescription namecd = schema.getDescription("Name");
        ColumnDescription agecd = schema.getDescription("Age");

        RecordOrder order = new RecordOrder();
        order.append(new ColumnSortOrientation(namecd, true));
        order.append(new ColumnSortOrientation(agecd, true));
        Object[] data = new Object[2];
        data[0] = "Bob";
        data[1] = 10;
        RowSnapshot search = new RowSnapshot(schema, data);
        RowComparisonFilterDescription filter = new RowComparisonFilterDescription(
                search, order, "==");
        FilterMap filterMap = new FilterMap(filter);
        ITable result = filterMap.apply(table);

        Assert.assertNotNull(result);
        Assert.assertEquals("Table[2x1]", result.toString());
        IColumn col = result.getLoadedColumn("Name");
        IRowIterator it = result.getMembershipSet().getIterator();
        int row = it.getNextRow();
        while (row != -1) {
            Assert.assertEquals("Bob", col.getString(row));
            row = it.getNextRow();
        }

        filter = new RowComparisonFilterDescription(
                search, order, "<=");
        filterMap = new FilterMap(filter);
        result = filterMap.apply(table);
        Assert.assertNotNull(result);
        Assert.assertEquals("Table[2x13]", result.toString());
        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            String str = col.getString(row);
            Assert.assertTrue(str == null || "Bob".compareTo(str) <= 0);
            row = it.getNextRow();
        }

        filter = new RowComparisonFilterDescription(
                search, order, ">=");
        filterMap = new FilterMap(filter);
        result = filterMap.apply(table);
        Assert.assertNotNull(result);
        Assert.assertEquals("Table[2x3]", result.toString());
        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            String str = col.getString(row);
            Assert.assertNotNull(str);
            Assert.assertTrue("Bob".compareTo(str) >= 0);
            row = it.getNextRow();
        }

        filter = new RowComparisonFilterDescription(
                search, order, "!=");
        filterMap = new FilterMap(filter);
        result = filterMap.apply(table);
        Assert.assertNotNull(result);
        Assert.assertEquals("Table[2x14]", result.toString());
        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            String str = col.getString(row);
            Assert.assertTrue(str == null || "Bob".compareTo(str) != 0);
            row = it.getNextRow();
        }

        filter = new RowComparisonFilterDescription(
                search, order, "<");
        filterMap = new FilterMap(filter);
        result = filterMap.apply(table);
        Assert.assertNotNull(result);
        Assert.assertEquals("Table[2x12]", result.toString());
        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            String str = col.getString(row);
            Assert.assertTrue(str == null || "Bob".compareTo(str) < 0);
            row = it.getNextRow();
        }

        filter = new RowComparisonFilterDescription(
                search, order, ">");
        filterMap = new FilterMap(filter);
        result = filterMap.apply(table);
        Assert.assertNotNull(result);
        Assert.assertEquals("Table[2x2]", result.toString());
        it = result.getMembershipSet().getIterator();
        row = it.getNextRow();
        while (row != -1) {
            String str = col.getString(row);
            Assert.assertNotNull(str);
            Assert.assertTrue("Bob".compareTo(str) > 0);
            row = it.getNextRow();
        }
    }
}
