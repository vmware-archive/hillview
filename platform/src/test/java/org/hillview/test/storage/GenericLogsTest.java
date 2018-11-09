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

package org.hillview.test.storage;

import io.krakens.grok.api.GrokCompiler;
import org.hillview.storage.GenericLogs;
import org.hillview.storage.TextFileLoader;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.utils.GrokExtra;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Various tests for reading Generic logs into ITable.
 */
public class GenericLogsTest extends BaseTest {
    @Test
    public void findTimestamp() {
        GrokCompiler grokCompiler = GrokCompiler.newInstance();
        grokCompiler.registerDefaultPatterns();
        grokCompiler.registerPatternFromClasspath("/patterns/log-patterns");
        String group = "Timestamp";

        String pattern = "%{SYSLOG}";
        String regex = GrokExtra.extractGroupPattern(
                grokCompiler.getPatternDefinitions(),
                pattern, group);
        Assert.assertEquals("SYSLOGTIMESTAMP", regex);

        pattern = "%{HADOOP}";
        regex = GrokExtra.extractGroupPattern(
                grokCompiler.getPatternDefinitions(),
                pattern, group);
        Assert.assertEquals("TIMESTAMP", regex);
    }

    @Test
    public void testSyslog() {
        String path = "../data/sample_logs/syslog";
        GenericLogs logs = new GenericLogs("%{SYSLOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        //System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[5x42]", table.toString());
    }

    @Test
    public void testSyslogTime() {
        String path = "../data/sample_logs/syslog";
        GenericLogs logs = new GenericLogs("%{SYSLOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path, null, Instant.now());
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        //System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[5x42]", table.toString());
    }

    @Test
    public void testSyslogTime1() {
        // This log has no years in the dates, so they will be parsed as the
        // current year...
        String path = "../data/sample_logs/syslog";
        GenericLogs logs = new GenericLogs("%{SYSLOG}");
        LocalDateTime now = LocalDateTime.now();
        Instant start = LocalDateTime.of(now.getYear(), 10, 7, 6, 0, 0)
                .atZone(ZoneOffset.systemDefault())
                .toInstant();
        Instant end = LocalDateTime.of(now.getYear(), 10, 7, 8, 0, 0)
                .atZone(ZoneOffset.systemDefault())
                .toInstant();

        TextFileLoader fileLoader = logs.getFileLoader(path, start, end);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        //System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[5x4]", table.toString());
    }

    @Test
    public void testEmptyLog() {
        String path = "../data/sample_logs/emptylog";
        GenericLogs logs = new GenericLogs("%{SYSLOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        // TODO: this test may need to be changed
        Assert.assertEquals("Table[2x0]", table.toString());
    }

    @Test
    public void testYarnLog() {
        String path = "../data/sample_logs/yarnlog";
        GenericLogs logs = new GenericLogs("%{YARNLOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        //System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[6x113]", table.toString());
    }

    @Test
    public void testHBaseLog() {
        String path = "../data/sample_logs/hbaselog";
        GenericLogs logs = new GenericLogs("%{HBASELOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        //System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[6x93]", table.toString());
    }

    @Test
    public void testDataNodeLog() {
        String path = "../data/sample_logs/datanodelog";
        GenericLogs logs = new GenericLogs("%{DATANODELOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        //System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[6x138]", table.toString());
    }
}
