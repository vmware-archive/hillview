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

package org.hillview.test.dataStructures;

import org.hillview.test.BaseTest;
import org.hillview.utils.Converters;
import org.hillview.utils.DateParsing;
import org.junit.Assert;
import org.junit.Test;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.StringWriter;
import java.time.*;

public final class DateTests extends BaseTest {
    @Test
    public void parseDates() {
        DateParsing parsing = new DateParsing("2017-01-01");
        Instant instant = parsing.parse("2017-01-01");
        Instant expected = LocalDate.of(2017, 1, 1)
                .atStartOfDay(ZoneOffset.systemDefault())
                .toInstant();
        Assert.assertEquals(instant, expected);

        instant = parsing.parse("1999-12-10");
        expected = LocalDate.of(1999, 12, 10)
                .atStartOfDay(ZoneOffset.systemDefault())
                .toInstant();
        Assert.assertEquals(instant, expected);

        parsing = new DateParsing("2017-01-01 10:10:10");
        instant = parsing.parse("2017-01-01 10:10:10");
        expected = LocalDateTime.of(2017, 1, 1, 10, 10, 10)
                .atZone(ZoneOffset.systemDefault())
                .toInstant();
        Assert.assertEquals(instant, expected);

        parsing = new DateParsing("2017-01-01 10:10:10.555");
        instant = parsing.parse("2017-01-01 10:10:10.555");
        expected = LocalDateTime.of(2017, 1, 1, 10, 10, 10, 555000000)
                .atZone(ZoneOffset.systemDefault())
                .toInstant();
        Assert.assertEquals(instant, expected);

        instant = parsing.parse("2017-01-01 10:10:10.666");
        expected = LocalDateTime.of(2017, 1, 1, 10, 10, 10, 666000000)
                .atZone(ZoneOffset.systemDefault())
                .toInstant();
        Assert.assertEquals(instant, expected);

        parsing = new DateParsing("2017-01-01 10:10:10.5");
        instant = parsing.parse("2017-01-01 10:10:10.1");
        expected = LocalDateTime.of(2017, 1, 1, 10, 10, 10, 100000000)
                .atZone(ZoneOffset.systemDefault())
                .toInstant();
        Assert.assertEquals(instant, expected);

        parsing = new DateParsing("2010-12-27T10:50:44.000-08:00");
        instant = parsing.parse("2010-12-27T10:50:44.000-08:00");
        expected = LocalDateTime.of(2010, 12, 27, 10, 50, 44)
                .atZone(ZoneOffset.systemDefault())
                .toInstant();
        Assert.assertEquals(instant, expected);

        parsing = new DateParsing("2017-10-05T14:05:35.454000");
        instant = parsing.parse("2017-10-05T14:05:35.454000");
        expected = LocalDateTime.of(2017, 10, 5, 14, 5, 35, 454000000)
                .atZone(ZoneOffset.systemDefault())
                .toInstant();
        Assert.assertEquals(instant, expected);

        // This test has a little race which may fail around the new year.
        // I have no idea how to make it better.
        parsing = new DateParsing("Oct  7 06:47:01");
        instant = parsing.parse("Oct  7 06:47:01");
        LocalDateTime now = LocalDateTime.now();
        expected = LocalDateTime.of(now.getYear(), 10, 7, 6, 47, 1)
                .atZone(ZoneOffset.systemDefault())
                .toInstant();
        Assert.assertEquals(instant, expected);

        instant = parsing.parse("Oct 10 06:47:01");
        expected = LocalDateTime.of(now.getYear(), 10, 10, 6, 47, 1)
                .atZone(ZoneOffset.systemDefault())
                .toInstant();
        Assert.assertEquals(instant, expected);
    }

    @Test
    public void testFormat() {
        DateParsing parsing = new DateParsing("2017-10-05T14:05:35.454000");
        Instant instant = parsing.parse("2017-10-05T14:05:35.454000");

        String repr = Converters.toString(instant);
        Assert.assertEquals("2017/10/05 14:05:35.454", repr);

        instant = parsing.parse("2017-10-05T14:05:00.000");
        repr = Converters.toString(instant);
        Assert.assertEquals("2017/10/05 14:05", repr);
    }

    @Test
    public void compareJavascriptTest() throws ScriptException, NoSuchMethodException {
        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("nashorn");
        // This function should be the same as the one in util.ts.
        String jsConverter =
                "function zeroPad(num, length) {\n" +
                "    var n = Math.abs(num);\n" +
                "    var zeros = Math.max(0, length - Math.floor(n).toString().length );\n" +
                "    var zeroString = Math.pow(10, zeros).toString().substr(1);\n" +
                "    if (num < 0) {\n" +
                "        zeroString = \"-\" + zeroString;\n" +
                "    }\n" +
                "    return zeroString + n;\n" +
                "}" +
                "function formatDate(ds) {\n" +
                "    var d = new Date(ds);\n" +
                "    if (d == null)\n" +
                "        return \"missing\";\n" +
                "    var year = d.getFullYear();\n" +
                "    var month = d.getMonth() + 1;\n" +
                "    var day = d.getDate();\n" +
                "    var hour = d.getHours();\n" +
                "    var minutes = d.getMinutes();\n" +
                "    var seconds = d.getSeconds();\n" +
                "    var ms = d.getMilliseconds();\n" +
                "    var df = String(year) + \"/\" + zeroPad(month, 2) + \"/\" + zeroPad(day, 2);\n" +
                "    var suffix = \"\";\n" +
                "    if (ms !== 0)\n" +
                "        suffix = \".\" + zeroPad(ms, 3);\n" +
                "    if (seconds !== 0 || suffix !== \"\")\n" +
                "        suffix = \":\" + zeroPad(seconds, 2) + suffix;\n" +
                "    if (minutes !== 0 || suffix !== \"\")\n" +
                "        suffix = \":\" + zeroPad(minutes, 2) + suffix;\n" +
                "    if (hour !== 0 || suffix !== \"\") {\n" +
                "        if (suffix === \"\")\n" +
                "            suffix = \":00\";\n" +
                "        suffix = \" \" + zeroPad(hour, 2) + suffix;\n" +
                "    }\n" +
                "    return df + suffix;\n" +
                "}\n";
        engine.eval(jsConverter);
        Invocable invocable = (Invocable)engine;

        String[] dates = new String[] {
                "2017-01-01",
                "1999-12-10",
                "1999-12-10 10:10",
                "1999-12-10 10:10:00",
                "2017-01-01 10:10:10",
                "2017-01-01 10:10:10.555",
                "2017-01-01 10:10:10.666",
                "2017-01-01 10:10:10.5",
                "2017-01-01 10:10:10.1",
                "2010-12-27T10:50:44.000-08:00",
                "2017-10-05T14:05:35.454000",
                "Oct  7 06:47:01",
                "Oct 10 06:47:01",
                "2016/01/03 08:00:00"
        };

        for (String d: dates) {
            /*
            Captures print statements in the script for debugging.
            StringWriter writer = new StringWriter();
            engine.getContext().setWriter(writer);
            */
            DateParsing parsing = new DateParsing(d);
            Instant instant = parsing.parse(d);
            String s = Converters.toString(instant);
            double dbl = Converters.toDouble(instant);
            Object value = invocable.invokeFunction("formatDate", dbl);
            //System.out.println(s + "=>\n" + writer);
            Assert.assertEquals(s, value);
        }
    }
}
