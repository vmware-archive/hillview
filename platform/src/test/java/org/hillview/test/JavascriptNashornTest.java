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

import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.maps.CreateColumnJSMap;
import org.hillview.maps.FilterMap;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.api.*;
import org.hillview.table.filters.JSFilterDescription;
import org.hillview.table.membership.SparseMembershipSet;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.hillview.utils.Converters;
import org.hillview.utils.DateParsing;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import javax.script.*;
import java.time.Instant;
import java.util.HashMap;

/**
 * Test the Javascript Nashorn engine.
 */
public class JavascriptNashornTest extends BaseTest {
    @Test
    public void helloWorldTest() throws ScriptException {
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("nashorn");
        engine.eval("print('Hello, World from Nashorn JavaScript!');");
    }

    @Test
    public void testJSDate() throws ScriptException {
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("nashorn");
        Object obj = engine.eval("new Date(2010, 1, 2);");
        ScriptObjectMirror jsDate = (ScriptObjectMirror)obj;
        double timestampLocal = (double)jsDate.callMember("getTime");
        Instant instant = Converters.toDate(timestampLocal);
        String someDate = "2010-02-02";
        DateParsing parsing = new DateParsing(someDate);
        Instant expected = parsing.parse(someDate);
        Assert.assertEquals(expected, instant);
    }

    @Test
    public void testFunctionAccess() throws ScriptException, NoSuchMethodException {
        ITable table = TestTables.testRepTable();
        RowSnapshot row = new RowSnapshot(table, 0);
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("nashorn");
        engine.eval("function getField(row, col) { return row[col]; }");
        Invocable invocable = (Invocable)engine;
        Object value = invocable.invokeFunction("getField", row, "Name");
        Assert.assertEquals(value, "Mike");

        VirtualRowSnapshot vrs = new VirtualRowSnapshot(table, table.getSchema());
        IRowIterator it = table.getMembershipSet().getIterator();
        int r = it.getNextRow();
        while (r >= 0) {
            vrs.setRow(r);
            int age = vrs.getInt("Age");
            Object jsAge = invocable.invokeFunction("getField", vrs, "Age");
            Assert.assertEquals(age, jsAge);
            r = it.getNextRow();
        }
    }

    @Test
    public void testMap() {
        ITable table = TestTables.testRepTable();
        LocalDataSet<ITable> lds = new LocalDataSet<ITable>(table);
        ColumnDescription outCol = new ColumnDescription("IsAdult", ContentsKind.String);
        String function = "function map(row) { return row['Age'] > 18 ? 'true' : 'false'; }";
        CreateColumnJSMap map = new CreateColumnJSMap(function, table.getSchema(), null, outCol);
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
    public void testFilter() {
        ITable table = TestTables.testRepTable();
        LocalDataSet<ITable> lds = new LocalDataSet<ITable>(table);
        String function = "function filter(row) { return row['Age'] > 18; }";
        JSFilterDescription desc = new JSFilterDescription(function, table.getSchema(), null);
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
        ColumnDescription outCol = new ColumnDescription("Date", ContentsKind.Date);
        String function = "function map(row) { return new Date(1970 + row['Age'], 0, 1); }";
        CreateColumnJSMap map = new CreateColumnJSMap(function, table.getSchema(), null, outCol);
        IDataSet<ITable> mapped = lds.blockingMap(map);
        ITable outTable = ((LocalDataSet<ITable>)mapped).data;
        Assert.assertNotNull(outTable);
        IColumn dateColumn = outTable.getLoadedColumn("Date");
        Instant instant = dateColumn.getDate(0);
        String expectedDate = "1990-01-01";
        DateParsing simple = new DateParsing(expectedDate);
        Instant expected = simple.parse(expectedDate);
        Assert.assertEquals(expected, instant);

        String data = outTable.toLongString(3);
        String someDate = "1990-01-01";
        DateParsing parsing = new DateParsing(someDate);
        Assert.assertEquals("Table[3x15]\n" +
                "Mike,20," + parsing.parse("1990-01-01") + "\n" +
                "John,30," + parsing.parse("2000-01-01") + "\n" +
                "Tom,10," + parsing.parse("1980-01-01") + "\n", data);
    }

    @Test
    public void testInteger() {
        ITable table = TestTables.testRepTable();
        LocalDataSet<ITable> lds = new LocalDataSet<ITable>(table);
        ColumnDescription outCol = new ColumnDescription("Older", ContentsKind.Integer);
        String function = "function map(row) { return row['Age'] + 10; }";
        CreateColumnJSMap map = new CreateColumnJSMap(function, table.getSchema(), null, outCol);
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
        ColumnDescription outCol = new ColumnDescription("Older", ContentsKind.Integer);
        String function = "function map(row) { return row['NewAge'] + 10; }";
        HashMap<String, String> renameMap = new HashMap<String, String>();
        renameMap.put("Age", "NewAge");
        CreateColumnJSMap map = new CreateColumnJSMap(
                function, table.getSchema(), renameMap, outCol);
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
        ColumnDescription outCol = new ColumnDescription("Older", ContentsKind.Integer);
        String function = "function map(row) { return row['Age'] + 10; }";
        CreateColumnJSMap map = new CreateColumnJSMap(function, table.getSchema(), null, outCol);
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
        ColumnDescription outCol = new ColumnDescription("Date", ContentsKind.Date);
        String function = "function map(row) { return new Date(1970 + row['Age'], 0, 1); }";
        CreateColumnJSMap map = new CreateColumnJSMap(function, table.getSchema(), null, outCol);
        IDataSet<ITable> mapped = lds.blockingMap(map);
        // Convert the date column
        ColumnDescription outCol1 = new ColumnDescription("Later", ContentsKind.Date);
        function = "function map(row) { " +
                "var d = row['Date']; " +
                "d.setFullYear(d.getFullYear() + 10); " +
                "return d;" +
                " }";
        Schema outSchema = table.getSchema().clone();
        outSchema.append(outCol);
        map = new CreateColumnJSMap(function, outSchema, null, outCol1);
        mapped = mapped.blockingMap(map);
        ITable outTable = ((LocalDataSet<ITable>)mapped).data;
        Assert.assertNotNull(outTable);
        String data = outTable.toLongString(3);

        String someDate = "1990-01-01";
        DateParsing p = new DateParsing(someDate);
        Assert.assertEquals("Table[4x15]\n" +
                "Mike,20," + p.parse("1990-01-01") + "," + p.parse("2000-01-01") + "\n" +
                "John,30," + p.parse("2000-01-01") + "," + p.parse("2010-01-01") + "\n" +
                "Tom,10," + p.parse("1980-01-01") + ","+ p.parse("1990-01-01") + "\n", data);
    }
}
