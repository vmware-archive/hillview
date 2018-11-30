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

import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import org.hillview.storage.GenericLogs;
import org.hillview.storage.TextFileLoader;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.test.BaseTest;
import org.hillview.utils.DateParsing;
import org.hillview.utils.GrokExtra;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Various tests for reading Generic logs into ITable.
 */
public class GenericLogsTest extends BaseTest {
    private static final boolean print = false;

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
        Assert.assertEquals("LONGTIMESTAMP", regex);
    }

    @Test
    public void testColumns() {
        GrokCompiler grokCompiler = GrokCompiler.newInstance();
        grokCompiler.registerDefaultPatterns();
        grokCompiler.registerPatternFromClasspath("/patterns/log-patterns");
        String pattern = "%{SYSLOG}";
        Grok grok = grokCompiler.compile(pattern);
        List<String> cols = GrokExtra.getColumnsFromPattern(grok);
        Assert.assertEquals(cols.size(), 3);
        Assert.assertEquals("Timestamp", cols.get(0));
        Assert.assertEquals("Logsource", cols.get(1));
        Assert.assertEquals("Message", cols.get(2));

        pattern = "%{HADOOP}";
        grok = grokCompiler.compile(pattern);
        cols = GrokExtra.getColumnsFromPattern(grok);
        Assert.assertEquals(cols.size(), 3);
        Assert.assertEquals("Timestamp", cols.get(0));
        Assert.assertEquals("Level", cols.get(1));
        Assert.assertEquals("Message", cols.get(2));
    }

    @Test
    public void longTimestamp() {
        GrokCompiler grokCompiler = GrokCompiler.newInstance();
        grokCompiler.registerDefaultPatterns();
        grokCompiler.registerPatternFromClasspath("/patterns/log-patterns");
        Grok grok = grokCompiler.compile("%{LONGTIMESTAMP:Timestamp}", true);

        String ts = "2018-09-30 09:44:10,802";
        String rest = "INFO  resourcemanager.ResourceManager (LogAdapter.java:info(45)) - registered UNIX signal handlers for [TERM, HUP, INT]";
        Match m = grok.match(ts);
        Assert.assertNotNull(m);
        Assert.assertFalse(m.isNull());
        Assert.assertEquals(ts, m.capture().get("Timestamp"));

        m = grok.match(ts + " " + rest);
        Assert.assertNotNull(m);
        Assert.assertFalse(m.isNull());
        Assert.assertEquals(ts, m.capture().get("Timestamp"));

        DateParsing parsing = new DateParsing(ts);
        Instant i = parsing.parse(ts);
        LocalDateTime ldt = LocalDateTime.ofInstant(i, ZoneOffset.systemDefault());
        Assert.assertEquals(2018, ldt.getYear());
        Assert.assertEquals(9, ldt.getMonthValue());
    }

    @Test
    public void testSyslog() {
        String path = "../data/sample_logs/syslog";
        GenericLogs logs = new GenericLogs("%{SYSLOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        //System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x42]", table.toString());

        IColumn col = table.getLoadedColumn(GenericLogs.directoryColumn);
        String s = col.getString(0);
        Assert.assertEquals("../data/sample_logs/", s);

        col = table.getLoadedColumn(GenericLogs.filenameColumn);
        s = col.getString(0);
        Assert.assertEquals("syslog", s);

        col = table.getLoadedColumn(GenericLogs.lineNumberColumn);
        int i = col.getInt(0);
        Assert.assertEquals(1, i);
        i = col.getInt(1);
        Assert.assertEquals(2, i);

        col = table.getLoadedColumn(GenericLogs.hostColumn);
        String host = col.getString(0);
        Assert.assertNotNull(host);
    }

    @Test
    public void testWrongpattern() {
        String path = "../data/sample_logs/syslog";
        GenericLogs logs = new GenericLogs("%{HADOOP}");  // correct one is SYSLOG
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (print)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x42]", table.toString());
        IColumn col = table.getLoadedColumn(GenericLogs.parseErrorColumn);
        for (int i = 0; i < col.sizeInRows(); i++) {
            String s = col.getString(i);
            Assert.assertNotNull(s);
        }
    }
    
    @Test
    public void testStartuplog() {
        String path = "../data/sample_logs/startuplog";
        GenericLogs logs = new GenericLogs("%{HADOOP}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (print)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x2]", table.toString());
    }
 
    @Test
    public void testSyslogTime() {
        String path = "../data/sample_logs/syslog";
        GenericLogs logs = new GenericLogs("%{SYSLOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path, null, Instant.now());
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (print)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x42]", table.toString());
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
        Instant end = LocalDateTime.of(now.getYear(), 10, 7, 9, 0, 0)
                .atZone(ZoneOffset.systemDefault())
                .toInstant();

        TextFileLoader fileLoader = logs.getFileLoader(path, start, end);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (print)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x5]", table.toString());
    }

    @Test
    public void testEmptyLog() {
        String path = "../data/sample_logs/emptylog";
        GenericLogs logs = new GenericLogs("%{SYSLOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        Assert.assertEquals("Table[8x0]", table.toString());
    }

    @Test
    public void testYarnLog() {
        String path = "../data/sample_logs/yarnlog";
        GenericLogs logs = new GenericLogs("%{YARNLOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (print)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x113]", table.toString());
    }

    @Test
    public void testRFC5424Log() {
        String path = "../data/sample_logs/rfc5424log";
        GenericLogs logs = new GenericLogs("%{RFC5424}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (print)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[14x8]", table.toString());
        RowSnapshot row = new RowSnapshot(table, 0);
        Instant i = row.getDate("Timestamp");
        Assert.assertNotNull(i);
        String prio = row.getString("SyslogPriority");
        Assert.assertEquals("187", prio);
        String ver = row.getString("SyslogVersion");
        Assert.assertEquals("1", ver);
        String hostname = row.getString("Hostname");
        Assert.assertEquals("nsx-manager", hostname);
        String structured = row.getString("StructuredData");
        Assert.assertEquals("[nsx@6876 comp=\"nsx-manager\" errorCode=\"MP4039\" subcomp=\"manager\"]", structured);
        String message = row.getString("Message");
        Assert.assertEquals("Connection verification failed for broker '10.160.108.196'. Marking broker unhealthy.", message);
        Assert.assertNull(row.getString(GenericLogs.parseErrorColumn));
        String appname = row.getString("Appname");
        Assert.assertEquals("NSX", appname);
        String pid = row.getString("Pid");
        Assert.assertEquals("-", pid);
        String messageId = row.getString("MessageId");
        Assert.assertEquals("SYSTEM", messageId);
    }

    @Test
    public void testHBaseLog() {
        String path = "../data/sample_logs/hbaselog";
        GenericLogs logs = new GenericLogs("%{HBASELOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (print)
            System.out.println(table.toLongString(50));
        Assert.assertEquals("Table[8x93]", table.toString());
    }

    @Test
    public void testDataNodeLog() {
        String path = "../data/sample_logs/datanodelog";
        GenericLogs logs = new GenericLogs("%{DATANODELOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (print)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x138]", table.toString());
    }
    
    @Test
    public void testOozieLog() {
        String path = "../data/sample_logs/oozielog";
        GenericLogs logs = new GenericLogs("%{OOZIELOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (print)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x5]", table.toString());
    }
    
    @Test
    public void testZookeeperLog() {
        String path = "../data/sample_logs/zookeeperlog";
        GenericLogs logs = new GenericLogs("%{ZOOKEEPERLOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (print)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x12]", table.toString());
    }

    @Test
    public void testHDFSNameNodeLog() {
        String path = "../data/sample_logs/hdfsnamenodelog";
        GenericLogs logs = new GenericLogs("%{HDFSNAMENODELOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (print)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x9]", table.toString());
    }

    @Test
    public void testHDFSDataNodeLog() {
        String path = "../data/sample_logs/hdfsdatanodelog";
        GenericLogs logs = new GenericLogs("%{HDFSDATANODELOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (print)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x5]", table.toString());
    }

}
