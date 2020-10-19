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
import org.hillview.storage.GrokLogs;
import org.hillview.storage.LogFiles;
import org.hillview.storage.TextFileLoader;
import org.hillview.table.ColumnDescription;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.rows.RowSnapshot;
import org.hillview.test.BaseTest;
import org.hillview.utils.DateParsing;
import org.hillview.utils.GrokExtra;
import org.hillview.utils.Utilities;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Various tests for reading Generic logs into ITable.
 */
public class GenericLogsTest extends BaseTest {
    GrokCompiler getCompiler() {
        GrokCompiler grokCompiler = GrokCompiler.newInstance();
        grokCompiler.registerDefaultPatterns();
        grokCompiler.registerPatternFromClasspath("/patterns/log-patterns");
        return grokCompiler;
    }

    @Test
    public void findTimestamp() {
        GrokCompiler grokCompiler = this.getCompiler();
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
        GrokCompiler grokCompiler = this.getCompiler();
        String pattern = "%{SYSLOG}";
        Grok grok = grokCompiler.compile(pattern);
        List<ColumnDescription> cols = GrokExtra.getColumnsFromPattern(grok);
        Assert.assertEquals(cols.size(), 3);
        Assert.assertEquals(LogFiles.timestampColumnName, cols.get(0).name);
        Assert.assertEquals("Logsource", cols.get(1).name);
        Assert.assertEquals("Message", cols.get(2).name);

        pattern = "%{HADOOP}";
        grok = grokCompiler.compile(pattern);
        cols = GrokExtra.getColumnsFromPattern(grok);
        Assert.assertEquals(cols.size(), 3);
        Assert.assertEquals(LogFiles.timestampColumnName, cols.get(0).name);
        Assert.assertEquals("Level", cols.get(1).name);
        Assert.assertEquals("Message", cols.get(2).name);
    }

    @Test
    public void longTimestamp() {
        GrokCompiler grokCompiler = this.getCompiler();
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
        LocalDateTime ldt = parsing.parseLocalDate(ts);
        Assert.assertEquals(2018, ldt.getYear());
        Assert.assertEquals(9, ldt.getMonthValue());
    }

    @Test
    public void testSyslog() {
        String path = dataDir + "/sample_logs/syslog";
        GrokLogs logs = new GrokLogs("%{SYSLOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        //System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x42]", table.toString());

        IColumn col = table.getLoadedColumn(LogFiles.directoryColumn);
        String s = col.getString(0);
        Assert.assertEquals(dataDir + "/sample_logs/", s);

        col = table.getLoadedColumn(LogFiles.filenameColumn);
        s = col.getString(0);
        Assert.assertEquals("syslog", s);

        col = table.getLoadedColumn(LogFiles.lineNumberColumn);
        int i = col.getInt(0);
        Assert.assertEquals(1, i);
        i = col.getInt(1);
        Assert.assertEquals(2, i);

        col = table.getLoadedColumn(LogFiles.hostColumn);
        String host = col.getString(0);
        Assert.assertNotNull(host);
    }

    @Test
    public void testWrongpattern() {
        String path = dataDir + "/sample_logs/syslog";
        GrokLogs logs = new GrokLogs("%{HADOOP}");  // correct one is SYSLOG
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x42]", table.toString());
        IColumn col = table.getLoadedColumn(LogFiles.parseErrorColumn);
        for (int i = 0; i < col.sizeInRows(); i++) {
            String s = col.getString(i);
            Assert.assertNotNull(s);
        }
    }

    @Test
    public void testStartuplog() {
        String path = dataDir + "/sample_logs/startuplog";
        GrokLogs logs = new GrokLogs("%{HADOOP}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x2]", table.toString());
    }

    @Test
    public void testSyslogTime() {
        String path = dataDir + "/sample_logs/syslog";
        GrokLogs logs = new GrokLogs("%{SYSLOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path, null, null);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x42]", table.toString());
    }

    @Test
    public void testSyslogTime1() {
        // This log has no years in the dates, so they will be parsed as the
        // current year...
        String path = dataDir + "/sample_logs/syslog";
        GrokLogs logs = new GrokLogs("%{SYSLOG}");
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime start = LocalDateTime.of(now.getYear(), 10, 7, 6, 0, 0);
        LocalDateTime end = LocalDateTime.of(now.getYear(), 10, 7, 9, 0, 0);

        TextFileLoader fileLoader = logs.getFileLoader(path, start, end);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x5]", table.toString());
    }

    @Test
    public void testEmptyLog() {
        String path = dataDir + "/sample_logs/emptylog";
        GrokLogs logs = new GrokLogs("%{SYSLOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        Assert.assertEquals("Table[8x0]", table.toString());
    }

    @Test
    public void testYarnLog() {
        String path = dataDir + "/sample_logs/yarnlog";
        GrokLogs logs = new GrokLogs("%{YARNLOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x113]", table.toString());
    }

    @Test
    public void testRFC5424Log() {
        String path = dataDir + "/sample_logs/rfc5424log";
        GrokLogs logs = new GrokLogs("%{RFC5424}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[14x8]", table.toString());
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        RowSnapshot row = new RowSnapshot(table, 0);
        Instant i = (Instant)row.getObject(LogFiles.timestampColumnName);
        Assert.assertNotNull(i);
        int prio = row.getInt("SyslogPriority");
        Assert.assertEquals(187, prio);
        int ver = row.getInt("SyslogVersion");
        Assert.assertEquals(1, ver);
        String hostname = row.getString("Hostname");
        Assert.assertEquals("nsx-manager", hostname);
        String structured = row.getString("StructuredData");
        Assert.assertEquals("[nsx@6876 comp=\"nsx-manager\" errorCode=\"MP4039\" subcomp=\"manager\"]", structured);
        String message = row.getString("Message");
        Assert.assertEquals("Connection verification failed for broker '10.160.108.196'. Marking broker unhealthy.", message);
        Assert.assertNull(row.getString(LogFiles.parseErrorColumn));
        String appname = row.getString("Appname");
        Assert.assertEquals("NSX", appname);
        String pid = row.getString("Pid");
        Assert.assertEquals("-", pid);
        String messageId = row.getString("MessageId");
        Assert.assertEquals("SYSTEM", messageId);
    }

    @Test
    public void testHBaseLog() {
        String path = dataDir + "/sample_logs/hbaselog";
        GrokLogs logs = new GrokLogs("%{HBASELOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(50));
        Assert.assertEquals("Table[8x93]", table.toString());
    }

    @Test
    public void testDataNodeLog() {
        String path = dataDir + "/sample_logs/datanodelog";
        GrokLogs logs = new GrokLogs("%{DATANODELOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x138]", table.toString());
    }

    @Test
    public void testOozieLog() {
        String path = dataDir + "/sample_logs/oozielog";
        GrokLogs logs = new GrokLogs("%{OOZIELOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x5]", table.toString());
    }

    @Test
    public void testZookeeperLog() {
        String path = dataDir + "/sample_logs/zookeeperlog";
        GrokLogs logs = new GrokLogs("%{ZOOKEEPERLOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x12]", table.toString());
    }

    @Test
    public void testHDFSNameNodeLog() {
        String path = dataDir + "/sample_logs/hdfsnamenodelog";
        GrokLogs logs = new GrokLogs("%{HDFSNAMENODELOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x9]", table.toString());
    }

    @Test
    public void testNsxtProtonLog() {
        String path = dataDir + "/sample_logs/nsxtprotonlog";
        GrokLogs logs = new GrokLogs("%{NSXT_PROTON}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[13x8]", table.toString());
    }

    @Test
    public void testNsxtProxyLog() {
        String path = dataDir + "/sample_logs/nsxtproxylog";
        GrokLogs logs = new GrokLogs("%{NSXT_PROXY}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[14x5]", table.toString());
    }

    @Test
    public void testHDFSDataNodeLog() {
        String path = dataDir + "/sample_logs/hdfsdatanodelog";
        GrokLogs logs = new GrokLogs("%{HDFSDATANODELOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(10));
        Assert.assertEquals("Table[8x5]", table.toString());
    }

    @Test
    public void testVSANLog() {
        String path = dataDir + "/sample_logs/vsantracelog";
        GrokLogs logs = new GrokLogs("%{VSANTRACE}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(3));
        Assert.assertEquals("Table[15x287]", table.toString());
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        RowSnapshot row = new RowSnapshot(table, 0);
        String structured = row.getString(LogFiles.parseErrorColumn);
        Assert.assertNull(structured);
    }

    @Test
    public void testBlockTrace() {
        String path = dataDir + "/sample_logs/blockTracelog";
        GrokLogs logs = new GrokLogs("%{BLOCKTRACE}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        if (BaseTest.toPrint)
            System.out.println(table.toLongString(3));
        Assert.assertEquals("Table[17x200]", table.toString());
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        RowSnapshot row = new RowSnapshot(table, 0);
        String structured = row.getString(LogFiles.parseErrorColumn);
        // System.out.println(table.toLongString(1));
        Assert.assertNull(structured);
    }

    @Test
    public void testWildcard() {
        String re = Utilities.wildcardToRegex("/host/vsan/domTraces[34].txt-*");
        Assert.assertEquals("^/host/vsan/domTraces[34]\\.txt-.*$", re);
    }
}
