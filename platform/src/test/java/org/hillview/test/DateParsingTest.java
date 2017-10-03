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

import org.hillview.utils.DateParsing;
import org.junit.Assert;
import org.junit.Test;

import java.time.*;

public class DateParsingTest extends BaseTest {
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
    }
}
