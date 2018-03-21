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

import org.hillview.sketches.ColumnSortOrientation;
import org.hillview.sketches.FindSketch;
import org.hillview.table.*;
import org.hillview.table.filters.StringFilterDescription;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

public class FindSketchTest extends BaseTest {
    @Test
    public void testFind1() {
        final Table table = TestTables.testTable();
        RecordOrder cso = new RecordOrder();
        for (String colName : table.getSchema().getColumnNames())
            cso.append(new ColumnSortOrientation(table.getSchema().getDescription(colName), true));

        StringFilterDescription sf = new StringFilterDescription("Mike", false, false, false);
        FindSketch fsk = new FindSketch(sf, null, cso);
        FindSketch.Result result = fsk.create(table);
        Assert.assertEquals(result.count, 1);
        Assert.assertNotNull(result.firstRow);
        Assert.assertEquals("Mike,20", result.firstRow.toString());
    }

    @Test
    public void testFind2() {
        final Table table = TestTables.testTable();
        RecordOrder cso = new RecordOrder();
        for (String colName : table.getSchema().getColumnNames())
            cso.append(new ColumnSortOrientation(table.getSchema().getDescription(colName), true));

        StringFilterDescription sf = new StringFilterDescription("Noone", false, false, false);
        FindSketch fsk = new FindSketch(sf, null, cso);
        FindSketch.Result result = fsk.create(table);
        Assert.assertEquals(result.count, 0);
        Assert.assertNull(result.firstRow);
    }

    @Test
    public void testFind3() {
        final Table table = TestTables.testTable();
        RecordOrder cso = new RecordOrder();
        for (String colName : table.getSchema().getColumnNames())
            cso.append(new ColumnSortOrientation(table.getSchema().getDescription(colName), true));

        StringFilterDescription sf = new StringFilterDescription("Bill", false, false, false);
        FindSketch fsk = new FindSketch(sf, null, cso);
        FindSketch.Result result = fsk.create(table);
        Assert.assertEquals(result.count, 2);
        Assert.assertNotNull(result.firstRow);
        Assert.assertEquals("Bill,1", result.firstRow.toString());
    }

    @Test
    public void testFind4() {
        final Table table = TestTables.testTable();
        RecordOrder cso = new RecordOrder();
        for (String colName : table.getSchema().getColumnNames())
            cso.append(new ColumnSortOrientation(table.getSchema().getDescription(colName), true));

        // start at row with index 2, which is larger than the first one in cso
        RowSnapshot top = new RowSnapshot(table, 2, table.getSchema());
        StringFilterDescription sf = new StringFilterDescription("Mike", false, false, false);
        FindSketch fsk = new FindSketch(sf, top, cso);
        FindSketch.Result result = fsk.create(table);
        Assert.assertEquals(result.count, 1);
        Assert.assertNull(result.firstRow);
    }

    @Test
    public void testFind5() {
        final Table table = TestTables.testTable();
        RecordOrder cso = new RecordOrder();
        String colName = table.getSchema().getColumnNames().get(0);
        cso.append(new ColumnSortOrientation(table.getSchema().getDescription(colName), true));

        // start at row with index 2, which is larger than the first one in cso
        RowSnapshot top = new RowSnapshot(table, 2, table.getSchema());
        StringFilterDescription sf = new StringFilterDescription("Mike", false, false, false);
        FindSketch fsk = new FindSketch(sf, top, cso);
        FindSketch.Result result = fsk.create(table);
        Assert.assertEquals(result.count, 1);
        Assert.assertNull(result.firstRow);
    }

    @Test
    public void testFind6() {
        final Table table = TestTables.testTable();
        RecordOrder cso = new RecordOrder();
        String colName = table.getSchema().getColumnNames().get(1);
        cso.append(new ColumnSortOrientation(table.getSchema().getDescription(colName), true));

        StringFilterDescription sf = new StringFilterDescription("Mike", false, false, false);
        FindSketch fsk = new FindSketch(sf, null, cso);
        FindSketch.Result result = fsk.create(table);
        // No matches on the second column
        Assert.assertEquals(0, result.count);
        Assert.assertNull(result.firstRow);
    }

    @Test
    public void testFind7() {
        final Table table = TestTables.testTable();
        RecordOrder cso = new RecordOrder();
        String colName = table.getSchema().getColumnNames().get(0);
        cso.append(new ColumnSortOrientation(table.getSchema().getDescription(colName), true));

        // Search substring "i".
        StringFilterDescription sf = new StringFilterDescription("i", false, false, true);
        FindSketch fsk = new FindSketch(sf, null, cso);
        FindSketch.Result result = fsk.create(table);
        // No matches on the second column
        Assert.assertEquals(5, result.count);
        Assert.assertNotNull(result.firstRow);
        Assert.assertEquals("Bill", result.firstRow.toString());
    }
}
