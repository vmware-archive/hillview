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

package org.hillview.test.dataset;

import org.hillview.sketches.results.ColumnSortOrientation;
import org.hillview.sketches.FindSketch;
import org.hillview.table.RecordOrder;
import org.hillview.table.Table;
import org.hillview.table.filters.StringFilterDescription;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.test.BaseTest;
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
        StringFilterDescription sf = new StringFilterDescription("Mike", false, false, false, false, false, true);
        FindSketch fsk = new FindSketch(sf, null, cso);
        FindSketch.Result result = fsk.create(table);
        Assert.assertNotNull(result);
        if (toPrint)
            printRes(table.getSchema().getColumnNames().get(0), result);
        this.assertResult(0, 1, 0,"Mike,20", result);
    }

    @Test
    public void testFind2() {
        final Table table = TestTables.testTable();
        RecordOrder cso = new RecordOrder();
        for (String colName : table.getSchema().getColumnNames())
            cso.append(new ColumnSortOrientation(table.getSchema().getDescription(colName), true));
        StringFilterDescription sf = new StringFilterDescription("Noone", false, false, false, false, false, true);
        FindSketch fsk = new FindSketch(sf, null, cso);
        FindSketch.Result result = fsk.create(table);
        Assert.assertNotNull(result);
        this.assertResult(0,0, 0,true, result);
    }

    @Test
    public void testFind3() {
        final Table table = TestTables.testTable();
        RecordOrder cso = new RecordOrder();
        for (String colName : table.getSchema().getColumnNames())
            cso.append(new ColumnSortOrientation(table.getSchema().getDescription(colName), true));
        StringFilterDescription sf = new StringFilterDescription("Bill", false, false, false, false, false, true);
        FindSketch fsk = new FindSketch(sf, null, cso);
        FindSketch.Result result = fsk.create(table);
        Assert.assertNotNull(result);
        if (toPrint)
            printRes(table.getSchema().getColumnNames().get(0), result);
        this.assertResult(0, 1, 1,"Bill,1", result);
    }

    @Test
    public void testFind4() {
        final Table table = TestTables.testTable();
        RecordOrder cso = new RecordOrder();
        for (String colName : table.getSchema().getColumnNames())
            cso.append(new ColumnSortOrientation(table.getSchema().getDescription(colName), true));

        // start at row with index 2, which is larger than the first one in cso
        RowSnapshot top2 = new RowSnapshot(table, 2, table.getSchema());
        StringFilterDescription sf = new StringFilterDescription("Mike", false, false, false, false, false, true);
        FindSketch fsk = new FindSketch(sf, top2, cso);
        FindSketch.Result result = fsk.create(table);
        Assert.assertNotNull(result);
        this.assertResult(1, 0, 0,true, result);
        RowSnapshot top0 = new RowSnapshot(table, 0, table.getSchema());
        sf.excludeTopRow = false;
        fsk = new FindSketch(sf, top0, cso);
        result = fsk.create(table);
        Assert.assertNotNull(result);
        this.assertResult(0, 1, 0, false, result);

        sf.excludeTopRow = true;
        fsk = new FindSketch(sf, top0, cso);
        result = fsk.create(table);
        Assert.assertNotNull(result);
        this.assertResult(1, 0, 0, true, result);
    }

    @Test
    public void testFind5() {
        final Table table = TestTables.testTable();
        RecordOrder cso = new RecordOrder();
        String colName = table.getSchema().getColumnNames().get(0);
        cso.append(new ColumnSortOrientation(table.getSchema().getDescription(colName), true));

        // start at row with index 3, which is larger than the first one in cso
        RowSnapshot top = new RowSnapshot(table, 3, table.getSchema());
        StringFilterDescription sf = new StringFilterDescription("Bi", true, false, true, false, false, true);
        FindSketch fsk = new FindSketch(sf, top, cso);
        FindSketch.Result result = fsk.create(table);
        Assert.assertNotNull(result);
        this.assertResult(0, 2, 0, "Bill", result);

        sf.next = true;
        sf.excludeTopRow = true;
        fsk = new FindSketch(sf, top, cso);
        result = fsk.create(table);
        Assert.assertNotNull(result);
        this.assertResult(2, 0, 0, true, result);
    }

    @Test
    public void testFind6() {
        final Table table = TestTables.testTable();
        RecordOrder cso = new RecordOrder();
        String colName = table.getSchema().getColumnNames().get(1);
        cso.append(new ColumnSortOrientation(table.getSchema().getDescription(colName), true));
        StringFilterDescription sf = new StringFilterDescription("Mike", false, false, false, false, false, true);
        FindSketch fsk = new FindSketch(sf, null, cso);
        FindSketch.Result result = fsk.create(table);
        Assert.assertNotNull(result);
        // No matches on the second column
        Assert.assertNotNull(result);
        this.assertResult(0, 0, 0,true, result);
    }

    @Test
    public void testFind7() {
        final Table table = TestTables.testTable();
        RecordOrder cso = new RecordOrder();
        String colName = table.getSchema().getColumnNames().get(0);
        cso.append(new ColumnSortOrientation(table.getSchema().getDescription(colName), true));

        // Search substring "i".
        StringFilterDescription sf = new StringFilterDescription("i", true, false, true, false, false, false);
        RowSnapshot top = new RowSnapshot(table, 0, table.getSchema());
        sf.excludeTopRow = false;
        sf.next = true;
        FindSketch fsk = new FindSketch(sf, top, cso);
        // Search for strings geq Mike
        FindSketch.Result result = fsk.create(table);
        Assert.assertNotNull(result);
        this.assertResult(2, 1, 2, "Mike", result);

        sf.next = true;
        sf.excludeTopRow = true;
        fsk = new FindSketch(sf, top, cso);
        // Searching strings strictly greater than Mike
        result = fsk.create(table);
        Assert.assertNotNull(result);
        this.assertResult(3, 1, 1, "Richard", result);

        // Previous search from Mike
        sf.excludeTopRow = true;
        sf.next = false;
        fsk = new FindSketch(sf, top, cso);
        result = fsk.create(table);
        Assert.assertNotNull(result);
        if (toPrint)
            printRes(top, colName, result);
        this.assertResult(0, 2, 3, "Bill", result);

        // Previous search from Smith
        top = new RowSnapshot(table, 5, table.getSchema());
        sf.excludeTopRow = true;
        sf.next = false;
        fsk = new FindSketch(sf, top, cso);
        result = fsk.create(table);
        Assert.assertNotNull(result);
        if (toPrint)
            printRes(top, colName, result);
        this.assertResult(3, 1, 1, "Richard", result);
    }

    private void printRes(RowSnapshot topRow,  String colName, FindSketch.Result result) {
        Assert.assertNotNull(result.firstMatchingRow);
        System.out.printf("TopRow: %s\n", topRow.asString(colName));
        Assert.assertNotNull(result.firstMatchingRow);
        System.out.printf("First Row: %s, before %d, at %d, after %d\n",
                result.firstMatchingRow.asString(colName), result.before, result.at,  result.after);
    }

    private void printRes(String colName, FindSketch.Result result) {
        Assert.assertNotNull(result.firstMatchingRow);
        System.out.printf("First Row: %s, before %d, at %d, after %d\n",
                result.firstMatchingRow.asString(colName), result.before, result.at,  result.after);
    }

    private void assertResult(long expBefore, long expAt, long expAfter, String expStr,
                              FindSketch.Result result) {
        Assert.assertEquals(expBefore, result.before);
        Assert.assertEquals(expAt, result.at);
        Assert.assertEquals(expAfter, result.after);
        Assert.assertNotNull(result.firstMatchingRow);
        Assert.assertEquals(expStr, result.firstMatchingRow.toString());
    }

    private void assertResult(long expBefore, long expAt, long expAfter, Boolean isNull,
                              FindSketch.Result result) {
        Assert.assertEquals(expBefore, result.before);
        Assert.assertEquals(expAt, result.at);
        Assert.assertEquals(expAfter, result.after);
        if (isNull)
            Assert.assertNull(result.firstMatchingRow);
        else
            Assert.assertNotNull(result.firstMatchingRow);
    }
}
