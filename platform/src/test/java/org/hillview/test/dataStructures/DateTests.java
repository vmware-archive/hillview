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

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.hillview.test.BaseTest;
import org.hillview.utils.Converters;
import org.hillview.utils.DateParsing;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.time.*;

public final class DateTests extends BaseTest {
    @Test
    public void parseDates() {
        DateParsing parsing = new DateParsing("2017-01-01");
        LocalDateTime ldt = parsing.parseLocalDate("2017-01-01");
        LocalDateTime expectedLdt = LocalDate.of(2017, 1, 1).atStartOfDay();
        Assert.assertEquals(ldt, expectedLdt);
        ldt = parsing.parseLocalDate("1999-12-10");
        expectedLdt = LocalDate.of(1999, 12, 10).atStartOfDay();
        Assert.assertEquals(expectedLdt, ldt);

        parsing = new DateParsing("2018/01/27");
        ldt = parsing.parseLocalDate("2018/01/27");
        expectedLdt = LocalDate.of(2018, 1, 27).atStartOfDay();
        Assert.assertEquals(expectedLdt, ldt);

        parsing = new DateParsing("2017-01-01 10:10:10");
        ldt = parsing.parseLocalDate("2017-01-01 10:10:10");
        expectedLdt = LocalDateTime.of(2017, 1, 1, 10, 10, 10);
        Assert.assertEquals(expectedLdt, ldt);

        parsing = new DateParsing("2017-01-01 10:10:10.555");
        ldt = parsing.parseLocalDate("2017-01-01 10:10:10.555");
        expectedLdt = LocalDateTime.of(2017, 1, 1, 10, 10, 10, 555000000);
        Assert.assertEquals(expectedLdt, ldt);

        ldt = parsing.parseLocalDate("2017-01-01 10:10:10.666");
        expectedLdt = LocalDateTime.of(2017, 1, 1, 10, 10, 10, 666000000);
        Assert.assertEquals(expectedLdt, ldt);

        parsing = new DateParsing("2017-01-01 10:10:10.5");
        ldt = parsing.parseLocalDate("2017-01-01 10:10:10.1");
        expectedLdt = LocalDateTime.of(2017, 1, 1, 10, 10, 10, 100000000);
        Assert.assertEquals(expectedLdt, ldt);

        parsing = new DateParsing("2017-10-05T14:05:35.454000");
        ldt = parsing.parseLocalDate("2017-10-05T14:05:35.454000");
        expectedLdt = LocalDateTime.of(2017, 10, 5, 14, 5, 35, 454000000);
        Assert.assertEquals(expectedLdt, ldt);

        // This test has a little race which may fail around the new year.
        // I have no idea how to make it better.
        parsing = new DateParsing("Oct  7 06:47:01");
        ldt = parsing.parseLocalDate("Oct  7 06:47:01");
        LocalDateTime now = LocalDateTime.now();
        expectedLdt = LocalDateTime.of(now.getYear(), 10, 7, 6, 47, 1);
        Assert.assertEquals(expectedLdt, ldt);

        ldt = parsing.parseLocalDate("Oct 10 06:47:01");
        expectedLdt = LocalDateTime.of(now.getYear(), 10, 10, 6, 47, 1);
        Assert.assertEquals(expectedLdt, ldt);
    }

    @Test
    public void testTZ() {
        DateParsing parsing = new DateParsing("1979-01-01 00:00:00 +0000 UTC");
        Instant instant = parsing.parseDate("1979-01-01 00:00:00 +0000 UTC");
        Instant expected = LocalDate.of(1979, 1, 1)
                .atStartOfDay(ZoneOffset.UTC)
                .toInstant();
        Assert.assertEquals(expected, instant);
    }

    @Test
    public void testTZ1() {
        DateParsing parsing = new DateParsing("2010-12-27T10:50:44.000-08:00");
        Instant instant = parsing.parseDate("2010-12-27T10:50:44.000-08:00");
        Instant expected = LocalDateTime.of(2010, 12, 27, 10, 50, 44)
                .atZone(ZoneOffset.ofHours(-8))
                .toInstant();
        Assert.assertEquals(instant, expected);
    }

    @Test
    public void testFormat() {
        DateParsing parsing = new DateParsing("2017-10-05T14:05:35.454000");
        LocalDateTime ldt = parsing.parseLocalDate("2017-10-05T14:05:35.454000");

        String repr = Converters.toString(ldt);
        Assert.assertEquals("2017/10/05 14:05:35.454", repr);

        ldt = parsing.parseLocalDate("2017-10-05T14:05:00.000");
        repr = Converters.toString(ldt);
        Assert.assertEquals("2017/10/05 14:05", repr);
    }

    @Test
    public void compareJavascriptTest() {
        /* Captures print statements in the script for debugging. */
        OutputStream stdout = new ByteArrayOutputStream();
        Context context = Context.newBuilder("js").out(stdout).build();
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
                "function formatTime(d) {\n" +
                "    var hour = d.getHours();\n" +
                "    var minutes = d.getMinutes();\n" +
                "    var seconds = d.getSeconds();\n" +
                "    var ms = d.getMilliseconds();\n" +
                "    var time = \"\";\n" +
                "    if (ms !== 0)\n" +
                "        time = \".\" + zeroPad(ms, 3);\n" +
                "    if (seconds !== 0 || time !== \"\")\n" +
                "        time = \":\" + zeroPad(seconds, 2) + time;\n" +
                "    if (minutes !== 0 || time !== \"\")\n" +
                "        time = \":\" + zeroPad(minutes, 2) + time;\n" +
                "    if (hour !== 0 || time !== \"\") {\n" +
                "        if (time === \"\")\n" +
                "            time = \":00\";\n" +
                "        time = zeroPad(hour, 2) + time;\n" +
                "    }\n" +
                "    return time;\n" +
                "}\n" +
                "function formatDate(ds, local) {\n" +
                "    var d = new Date(ds);\n" +
                "    if (local) {\n" +
                // For some strange reason new Date().getTimezoneOffset() does not give the same result!
                "        var offset = d.getTimezoneOffset();\n" +
                "        d = new Date(ds + offset * 60 * 1000);\n" +
                "    }\n" +
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
                "    var time = formatTime(d);\n" +
                "    if (time != \"\")\n" +
                "        return df + \" \" + time;\n" +
                "    return df;\n" +
                "}\n";
        context.eval("js", jsConverter);
        Value function = context.eval("js", "(d, l) => formatDate(d, l)");
        assert function.canExecute();
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
                "2017-10-05T14:05:35.454000",
                "Oct  7 06:47:01",
                "Oct 10 06:47:01",
                "2016/01/03 08:00:00"
        };

        for (String d: dates) {
            DateParsing parsing = new DateParsing(d);
            LocalDateTime ldt = parsing.parseLocalDate(d);
            String s = Converters.toString(ldt);
            double dbl = Converters.toDouble(ldt);
            String value = function.execute(dbl, true).asString();
            /* Debugging output
            String debug = stdout.toString();
            System.out.println(s + "=>" + debug);
             */
            Assert.assertEquals(s, value);
        }

        dates = new String[] {
                "2010-12-27T10:50:44.000-08:00"
        };
        for (String d: dates) {
            DateParsing parsing = new DateParsing(d);
            Instant ldt = parsing.parseDate(d);
            String s = Converters.toString(ldt);
            double dbl = Converters.toDouble(ldt);
            String value = function.execute(dbl, false).asString();
            Assert.assertEquals(s, value);
        }
    }
}
