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

import org.hillview.storage.HillviewLogs;
import org.hillview.storage.LogFiles;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.utils.Converters;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.*;

public class LogFileParserTest extends BaseTest {
    @Nullable
    private Object getValue(ITable table, String col, int row) {
        IColumn cc = table.getLoadedColumn(col);
        return cc.getObject(row);
    }

    @Test
    public void parseLogLines() throws IOException {
        String logFileContents = String.join("\n",
    "2021-09-24 15:02:19.474,none,INFO,linux,main,HillviewLogger.java," +
            "82,org.hillview.utils.HillviewLogger,<init>,Starting logger," +
            "Working directory: /home/mbudiu/git/hillview/.\n" +
            "2021-09-24 15:02:19.484,worker,INFO,linux,main,ExecutorUtils.java,83," +
            "org.hillview.utils.ExecutorUtils,getComputeExecutorService,Detect CPUs,Using 4 processors\n" +
            "2021-09-24 15:02:19.661,worker,INFO,linux,main,HillviewLogger.java," +
            "171,org.hillview.utils.HillviewLogger,info,Created HillviewServer"
        );

        File f = File.createTempFile("tmp", null, new File("."));
        f.deleteOnExit();
        PrintWriter out = new PrintWriter(f.getName());
        out.println(logFileContents);
        out.close();

        Path path = Paths.get(".", f.getName());
        ITable table = HillviewLogs.parseLogFile(path.toString());
        Converters.checkNull(table);
        Assert.assertEquals("Table[16x3]", table.toString());
        LocalDate date = LocalDate.of(2021, 9, 24);
        LocalTime time = LocalTime.of(15, 2, 19, 474000000);
        LocalDateTime dt = LocalDateTime.of(date, time);
        Assert.assertEquals(dt, getValue(table, LogFiles.timestampColumnName, 0));
        Assert.assertEquals("none", getValue(table, "Role", 0));
        Assert.assertEquals("INFO", getValue(table, "Level", 0));
        Assert.assertEquals("linux", getValue(table, "Machine", 0));
        Assert.assertEquals("main", getValue(table, "Thread", 0));
        Assert.assertEquals("org.hillview.utils.HillviewLogger", getValue(table, "Class", 0));
        Assert.assertEquals("<init>", getValue(table, "Method", 0));
        Assert.assertEquals("Starting logger", getValue(table, "Message", 0));
        Assert.assertEquals("Working directory: /home/mbudiu/git/hillview/.", getValue(table, "Arguments", 0));
        Assert.assertEquals(82, getValue(table, "SourceLine", 0));
        Assert.assertEquals("HillviewLogger.java", getValue(table, "SourceFile", 0));
        Assert.assertEquals(1, getValue(table, LogFiles.lineNumberColumn, 0));
        Assert.assertEquals("./", getValue(table, LogFiles.directoryColumn, 0));
        Assert.assertEquals(f.getName(), getValue(table, LogFiles.filenameColumn, 0));
        Assert.assertNull  (getValue(table, LogFiles.parseErrorColumn, 0));
    }

    @Test
    public void parseMalformedLog() throws IOException {
        String s = "2021-09-24 15:02:19.484,worker,INFO,linux,main,ExecutorUtils.java,83," +
                "org.hillview.utils.ExecutorUtils,getComputeExecutorService,Detect CPUs,Using 4 processors\n" +
        "2019-03-22 09:27:10.505,worker,WARNING,ip-172-31-12-140,Unable to load native-hadoop "+
                "library for your platform... using builtin-java classes where applicable\n";

        File f = File.createTempFile("tmp", null, new File("."));
        f.deleteOnExit();
        PrintWriter out = new PrintWriter(f.getName());
        out.println(s);
        out.close();

        Path path = Paths.get(".", f.getName());
        ITable table = HillviewLogs.parseLogFile(path.toString());
        Converters.checkNull(table);
        Assert.assertEquals(table.toString(), "Table[16x2]");
        Assert.assertNull(getValue(table, LogFiles.parseErrorColumn, 0));
        Assert.assertNotNull(getValue(table, LogFiles.parseErrorColumn, 1));
    }
}
