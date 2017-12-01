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

import org.hillview.dataset.LocalDataSet;
import org.hillview.dataset.api.IDataSet;
import org.hillview.maps.CreateColumnJSMap;
import org.hillview.table.ColumnDescription;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.table.rows.VirtualRowSnapshot;
import org.junit.Assert;
import org.junit.Test;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

/**
 * Test the Javascript Nashorn engine.
 */
public class JavascriptTest {
    @Test
    public void helloWorldTest() throws ScriptException {
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("nashorn");
        engine.eval("print('Hello, World from JavaScript!');");
    }

    @Test
    public void testFunctionAccess() throws ScriptException, NoSuchMethodException {
        ITable table = ToCatMapTest.tableWithStringColumn();
        RowSnapshot row = new RowSnapshot(table, 0);
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("nashorn");
        engine.eval("function getField(row, col) { return row[col]; }");
        Invocable invocable = (Invocable)engine;
        Object value = invocable.invokeFunction("getField", row, "Name");
        Assert.assertEquals(value, "Mike");

        VirtualRowSnapshot vrs = new VirtualRowSnapshot(table);
        IRowIterator it = table.getMembershipSet().getIterator();
        int r = it.getNextRow();
        while (r >= 0) {
            vrs.setRow(r);
            int age = vrs.getInt("Age");
            Object jsage = invocable.invokeFunction("getField", vrs, "Age");
            Assert.assertEquals(age, jsage);
            r = it.getNextRow();
        }
    }

    @Test
    public void testMap() throws ScriptException, NoSuchMethodException {
        ITable table = ToCatMapTest.tableWithStringColumn();
        LocalDataSet<ITable> lds = new LocalDataSet<ITable>(table);
        ColumnDescription outCol = new ColumnDescription("IsAdult", ContentsKind.Category, false);
        String function = "function map(row) { return row['Age'] > 18 ? 'true' : 'false'; }";
        CreateColumnJSMap map = new CreateColumnJSMap(function, table.getSchema(), outCol);
        IDataSet<ITable> mapped = lds.blockingMap(map);
        ITable outTable = ((LocalDataSet<ITable>)mapped).data;
        String data = outTable.toLongString(3);
        Assert.assertEquals("Table[3x15]\n" +
                "Mike,20,true\n" +
                "John,30,true\n" +
                "Tom,10,false\n", data);
    }
}
