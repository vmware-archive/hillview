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

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyObject;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.maps.CreateColumnJSMap;
import org.hillview.maps.FilterMap;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.filters.JSFilterDescription;
import org.hillview.table.membership.SparseMembershipSet;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.Converters;
import org.hillview.utils.DateParsing;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;

/**
 * Test the Graal Javascript engine.
 */
public class JavascriptTest extends BaseTest {
    @Test
    public void helloWorldTest() {
        Context context = Context.create();
        context.eval("js", "print('Hello, World from Graal JavaScript!')");
    }

    @Test
    public void testJSDate() {
        Context context = Context.create();
        Value result = context.eval("js", "new Date(2010, 1, 2);");
        Assert.assertEquals("2010-02-02", result.asDate().toString());
    }

    @Test
    public void testFunctionAccess() {
        ITable table = TestTables.testRepTable();
        RowSnapshot row = new RowSnapshot(table, 0);
        Context context = Context.newBuilder().allowAllAccess(true).build();
        Value function = context.eval("js", "(row, col) => row[col]");
        String value = function.execute(ProxyObject.fromMap(row), "Name").asString();
        Assert.assertEquals("Mike", value);

        VirtualRowSnapshot vrs = new VirtualRowSnapshot(table, table.getSchema());
        ProxyObject vrsProxyObject = ProxyObject.fromMap(vrs);
        IRowIterator it = table.getMembershipSet().getIterator();
        int r = it.getNextRow();
        while (r >= 0) {
            vrs.setRow(r);
            int age = vrs.getInt("Age");
            int jsAge = function.execute(vrsProxyObject, "Age").asInt();
            Assert.assertEquals(age, jsAge);
            r = it.getNextRow();
        }
    }

    @Test
    public void testMap() {
        ITable table = TestTables.testRepTable();
        LocalDataSet<ITable> lds = new LocalDataSet<ITable>(table);
        String function = "function map(row) { return row['Age'] > 18 ? 'true' : 'false'; }";
        CreateColumnJSMap.Info info = new CreateColumnJSMap.Info(
                function, table.getSchema(), "IsAdult", ContentsKind.String, null);
        CreateColumnJSMap map = new CreateColumnJSMap(info);
        IDataSet<ITable> mapped = lds.blockingMap(map);
        ITable outTable = ((LocalDataSet<ITable>)mapped).data;
        Assert.assertNotNull(outTable);
        String data = outTable.toLongString(3);
        Assert.assertEquals("Table[3x15]\n" +
                "Mike,20,true\n" +
                "John,30,true\n" +
                "Tom,10,false\n", data);
    }

    @Test
    public void testIntervalMap() {
        ITable table = TestTables.testRepTable();
        LocalDataSet<ITable> lds = new LocalDataSet<ITable>(table);
        String function = "function map(row) { return row['Age'] > 18 ? [2,3] : [3,4]; }";
        CreateColumnJSMap.Info info = new CreateColumnJSMap.Info(
                function, table.getSchema(), "Interval", ContentsKind.Interval, null);
        CreateColumnJSMap map = new CreateColumnJSMap(info);
        IDataSet<ITable> mapped = lds.blockingMap(map);
        ITable outTable = ((LocalDataSet<ITable>)mapped).data;
        Assert.assertNotNull(outTable);
        String data = outTable.toLongString(3);
        Assert.assertEquals("Table[3x15]\n" +
                "Mike,20,[2.0 : 3.0]\n" +
                "John,30,[2.0 : 3.0]\n" +
                "Tom,10,[3.0 : 4.0]\n", data);

        function = "function map(row) { return row['Interval'][0] + row['Interval'][1]; }";
        info = new CreateColumnJSMap.Info(
                function, outTable.getSchema(), "Sum", ContentsKind.Double, null);
        map = new CreateColumnJSMap(info);
        mapped = mapped.blockingMap(map);
        outTable = ((LocalDataSet<ITable>)mapped).data;
        Assert.assertNotNull(outTable);
        data = outTable.toLongString(3);
        Assert.assertEquals("Table[4x15]\n" +
                "Mike,20,[2.0 : 3.0],5.0\n" +
                "John,30,[2.0 : 3.0],5.0\n" +
                "Tom,10,[3.0 : 4.0],7.0\n", data);
    }

    @Test
    public void testMap1() {
        ITable table = TestTables.testRepTable();
        LocalDataSet<ITable> lds = new LocalDataSet<ITable>(table);
        String function = "function map(row) { return Math.round(row['Age'] / 10) * 10; }";
        CreateColumnJSMap.Info info = new CreateColumnJSMap.Info(
                function, table.getSchema(), "Range", ContentsKind.Double, null);
        CreateColumnJSMap map = new CreateColumnJSMap(info);
        IDataSet<ITable> mapped = lds.blockingMap(map);
        ITable outTable = ((LocalDataSet<ITable>)mapped).data;
        Assert.assertNotNull(outTable);
        String data = outTable.toLongString(3);
        Assert.assertEquals("Table[3x15]\n" +
                "Mike,20,20.0\n" +
                "John,30,30.0\n" +
                "Tom,10,10.0\n", data);
    }

    @Test
    public void testMap2() {
        ITable table = TestTables.testRepTable();
        LocalDataSet<ITable> lds = new LocalDataSet<ITable>(table);
        String function = "function map(row) { return Math.round(row['Age'] / 10) * 10; }";
        CreateColumnJSMap.Info info = new CreateColumnJSMap.Info(
                function, table.getSchema(), "Range", ContentsKind.Integer, null);
        CreateColumnJSMap map = new CreateColumnJSMap(info);
        IDataSet<ITable> mapped = lds.blockingMap(map);
        ITable outTable = ((LocalDataSet<ITable>)mapped).data;
        Assert.assertNotNull(outTable);
        String data = outTable.toLongString(3);
        Assert.assertEquals("Table[3x15]\n" +
                "Mike,20,20\n" +
                "John,30,30\n" +
                "Tom,10,10\n", data);
    }

    @Test
    public void testFilter() {
        ITable table = TestTables.testRepTable();
        LocalDataSet<ITable> lds = new LocalDataSet<ITable>(table);
        String function = "function filter(row) { return row['Age'] > 18; }";
        JSFilterDescription.Info info = new JSFilterDescription.Info(table.getSchema(), function, null);
        JSFilterDescription desc = new JSFilterDescription(info);
        FilterMap map = new FilterMap(desc);
        IDataSet<ITable> mapped = lds.blockingMap(map);
        ITable outTable = ((LocalDataSet<ITable>)mapped).data;
        Assert.assertNotNull(outTable);
        String data = outTable.toLongString(3);
        Assert.assertEquals("Table[2x11]\n" +
                "Mike,20\n" +
                "John,30\n" +
                "Bill,20\n", data);
    }

    @Test
    public void testDate() {
        ITable table = TestTables.testRepTable();
        LocalDataSet<ITable> lds = new LocalDataSet<ITable>(table);
        String function = "function map(row) { return new Date(1970 + row['Age'], 0, 1); }";
        CreateColumnJSMap.Info info = new CreateColumnJSMap.Info(
                function, table.getSchema(), "Date", ContentsKind.LocalDate, null);
        CreateColumnJSMap map = new CreateColumnJSMap(info);
        IDataSet<ITable> mapped = lds.blockingMap(map);
        ITable outTable = ((LocalDataSet<ITable>)mapped).data;
        Assert.assertNotNull(outTable);
        IColumn dateColumn = outTable.getLoadedColumn("Date");
        LocalDateTime date = Converters.toLocalDate(dateColumn.getDouble(0));
        String expectedDate = "1990-01-01";
        DateParsing simple = new DateParsing(expectedDate);
        LocalDateTime expected = simple.parseLocalDate(expectedDate);
        Assert.assertEquals(expected, date);

        String data = outTable.toLongString(3);
        String someDate = "1990-01-01";
        DateParsing parsing = new DateParsing(someDate);
        Assert.assertEquals("Table[3x15]\n" +
                "Mike,20," + parsing.parseLocalDate("1990-01-01") + "\n" +
                "John,30," + parsing.parseLocalDate("2000-01-01") + "\n" +
                "Tom,10," + parsing.parseLocalDate("1980-01-01") + "\n", data);
    }

    @Test
    public void testInteger() {
        ITable table = TestTables.testRepTable();
        LocalDataSet<ITable> lds = new LocalDataSet<ITable>(table);
        String function = "function map(row) { return row['Age'] + 10; }";
        CreateColumnJSMap.Info info = new CreateColumnJSMap.Info(
                function, table.getSchema(), "Older", ContentsKind.Integer, null);
        CreateColumnJSMap map = new CreateColumnJSMap(info);
        IDataSet<ITable> mapped = lds.blockingMap(map);
        ITable outTable = ((LocalDataSet<ITable>)mapped).data;
        Assert.assertNotNull(outTable);
        String data = outTable.toLongString(3);
        Assert.assertEquals("Table[3x15]\n" +
                "Mike,20,30\n" +
                "John,30,40\n" +
                "Tom,10,20\n", data);
    }

    @Test
    public void testRename() {
        ITable table = TestTables.testRepTable();
        LocalDataSet<ITable> lds = new LocalDataSet<ITable>(table);
        String function = "function map(row) { return row['NewAge'] + 10; }";
        String[] renameMap = new String[] { "Age", "NewAge" };
        CreateColumnJSMap.Info info = new CreateColumnJSMap.Info(
                function, table.getSchema(), "Older", ContentsKind.Integer, renameMap);
        CreateColumnJSMap map = new CreateColumnJSMap(info);
        IDataSet<ITable> mapped = lds.blockingMap(map);
        ITable outTable = ((LocalDataSet<ITable>)mapped).data;
        Assert.assertNotNull(outTable);
        String data = outTable.toLongString(3);
        Assert.assertEquals("Table[3x15]\n" +
                "Mike,20,30\n" +
                "John,30,40\n" +
                "Tom,10,20\n", data);
    }

    @Test
    public void testCompute() {
        ITable table = TestTables.testRepTable();
        SparseMembershipSet set = new SparseMembershipSet(table.getMembershipSet().getMax(), 2);
        set.add(0);
        set.add(1);
        ITable tbl = table.selectRowsFromFullTable(set);
        LocalDataSet<ITable> lds = new LocalDataSet<ITable>(tbl);
        String function = "function map(row) { return row['Age'] + 10; }";
        CreateColumnJSMap.Info info = new CreateColumnJSMap.Info(
                function, table.getSchema(), "Older", ContentsKind.Integer, null);
        CreateColumnJSMap map = new CreateColumnJSMap(info);
        IDataSet<ITable> mapped = lds.blockingMap(map);
        ITable outTable = ((LocalDataSet<ITable>) mapped).data;
        Assert.assertNotNull(outTable);
        String data = outTable.toLongString(3);
        Assert.assertEquals("Table[3x2]\n" +
                "Mike,20,30\n" +
                "John,30,40\n", data);
    }

    @Test
    public void testDateOutput() {
        ITable table = TestTables.testRepTable();
        LocalDataSet<ITable> lds = new LocalDataSet<ITable>(table);
        // Add a date column
        ColumnDescription outCol = new ColumnDescription("Date", ContentsKind.LocalDate);
        String function = "function map(row) { return new Date(1970 + row['Age'], 0, 1); }";
        CreateColumnJSMap.Info info = new CreateColumnJSMap.Info(
                function, table.getSchema(), "Date", ContentsKind.LocalDate, null);
        CreateColumnJSMap map = new CreateColumnJSMap(info);
        IDataSet<ITable> mapped = lds.blockingMap(map);
        // Convert the date column
        function = "function map(row) { " +
                "var d = row['Date']; " +
                "d.setFullYear(d.getFullYear() + 10); " +
                "return d;" +
                " }";
        Schema outSchema = table.getSchema().clone();
        outSchema.append(outCol);
        CreateColumnJSMap.Info info1 = new CreateColumnJSMap.Info(
                function, outSchema, "Later", ContentsKind.LocalDate, null);
        map = new CreateColumnJSMap(info1);
        mapped = mapped.blockingMap(map);
        ITable outTable = ((LocalDataSet<ITable>)mapped).data;
        Assert.assertNotNull(outTable);
        String data = outTable.toLongString(3);

        String someDate = "1990-01-01";
        DateParsing p = new DateParsing(someDate);
        Assert.assertEquals("Table[4x15]\n" +
                "Mike,20," + p.parseLocalDate("1990-01-01") + "," + p.parseLocalDate("2000-01-01") + "\n" +
                "John,30," + p.parseLocalDate("2000-01-01") + "," + p.parseLocalDate("2010-01-01") + "\n" +
                "Tom,10," + p.parseLocalDate("1980-01-01") + ","+ p.parseLocalDate("1990-01-01") + "\n", data);
    }
}
