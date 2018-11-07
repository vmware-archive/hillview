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

import org.hillview.storage.GenericLogs;
import org.hillview.storage.TextFileLoader;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Various tests for reading Generic logs into ITable.
 */
public class GenericLogsTest extends BaseTest {
    @Test
    public void testSyslog() {
        String path = "../data/sample_logs/syslog";
        GenericLogs logs = new GenericLogs("%{SYSLOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        Assert.assertEquals("Table[5x42]", table.toString());
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
        Assert.assertEquals("Table[6x113]", table.toString());
    }

    @Test
    public void testHBaseLog() {
        String path = "../data/sample_logs/hbaselog";
        GenericLogs logs = new GenericLogs("%{HBASELOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        Assert.assertEquals("Table[6x93]", table.toString());
    }

    @Test
    public void testDataNodeLog() {
        String path = "../data/sample_logs/datanodelog";
        GenericLogs logs = new GenericLogs("%{DATANODELOG}");
        TextFileLoader fileLoader = logs.getFileLoader(path);
        ITable table = fileLoader.load();
        Assert.assertNotNull(table);
        Assert.assertEquals("Table[6x138]", table.toString());
    }
}
