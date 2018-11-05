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

// Test for the Rhino JavaScript engine.

import org.hillview.table.api.ITable;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.utils.Converters;
import org.hillview.utils.DateParsing;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;
import org.mozilla.javascript.*;

import java.time.Instant;

public class JavascriptRhinoTest extends BaseTest {
    @Test
    public void helloWorldRhinoTest() {
        Context mozillaJsContext = Context.enter();
        try {
            Scriptable scope = mozillaJsContext.initStandardObjects();
            String script = "java.lang.System.out.println('Hello, World from Rhino JavaScript!')";
            mozillaJsContext.evaluateString(scope, script, "helloWorldTest", 1, null);
        } finally {
            Context.exit();
        }
    }

    @Test
    public void testJSRhinoDate() {
        Context mozillaJsContext = Context.enter();
        try {
            Scriptable scope = mozillaJsContext.initStandardObjects();
            String source = "new Date(2010, 1, 2);";
            Script script = mozillaJsContext.compileString(source, "testScript", 1, null);
            Scriptable obj = (Scriptable)script.exec(mozillaJsContext, scope);
            Function method = (Function) ScriptableObject.getProperty(obj, "getTime");
            double timestampLocal = (double) method.call(mozillaJsContext, scope, obj, new Object[0]);
            Instant instant = Converters.toDate(timestampLocal);
            String someDate = "2010-02-02";
            DateParsing parsing = new DateParsing(someDate);
            Instant expected = parsing.parse(someDate);
            Assert.assertEquals(expected, instant);
        } finally {
            Context.exit();
        }
    }

    //@Test
    // This test fails: JS does not access Java objects implementing Map in this way
    public void testRhinoFunctionAccess() {
        ITable table = TestTables.testRepTable();
        RowSnapshot row = new RowSnapshot(table, 0);
        Context mozillaJsContext = Context.enter();
        try {
            Scriptable scope = mozillaJsContext.initStandardObjects();
            String source = "function getField(row, col) { return row[col]; }";
            Function function = mozillaJsContext.compileFunction(scope, source, "testScript", 1, null);
            Object[] args = new Object[2];
            args[0] = row;
            args[1] = "Name";
            Object value = function.call(mozillaJsContext, scope, null, args);
            Assert.assertEquals(value, "Mike");
        } finally {
            Context.exit();
        }
    }
}
