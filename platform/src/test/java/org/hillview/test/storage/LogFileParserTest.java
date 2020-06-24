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
    "2017-10-12 02:17:42.722,worker,INFO,ubuntu,main,org.hillview.dataset" +
            ".LocalDataSet,<clinit>,Detect CPUs,Using 3 processors",
    "2017-10-12 02:17:43.172,worker,INFO,ubuntu,main,org.hillview.dataset" +
            ".remoting.HillviewServer,put,Inserting dataset,0",
    "2017-10-12 02:17:43.173,worker,INFO,ubuntu,main,org.hillview.utils" +
            ".HillviewLogger,info,Created HillviewServer",
    "2017-10-12 02:18:29.084,worker,INFO,ubuntu,pool-1-thread-1,org.hillview" +
            ".maps.FindCsvFileMapper,apply,Find files in folder,/hillview/data"
        );

        File f = File.createTempFile("tmp", null, new File("."));
        f.deleteOnExit();
        PrintWriter out = new PrintWriter(f.getName());
        out.println(logFileContents);
        out.close();

        Path path = Paths.get(".", f.getName());
        ITable table = HillviewLogs.parseLogFile(path.toString());
        Converters.checkNull(table);
        Assert.assertEquals(table.toString(), "Table[14x4]");
        LocalDate date = LocalDate.of(2017, 10, 12);
        LocalTime time = LocalTime.of(2, 17, 42, 722000000);
        LocalDateTime dt = LocalDateTime.of(date, time);
        ZonedDateTime zdt = dt.atZone(ZoneId.systemDefault());
        Instant instant = zdt.toInstant();
        Assert.assertEquals(getValue(table, LogFiles.timestampColumnName, 0), instant);
        Assert.assertEquals(getValue(table, "Role", 0), "worker");
        Assert.assertEquals(getValue(table, "Level", 0), "INFO");
        Assert.assertEquals(getValue(table, "Machine", 0), "ubuntu");
        Assert.assertEquals(getValue(table, "Thread", 0), "main");
        Assert.assertEquals(getValue(table, "Class", 0), "org.hillview.dataset.LocalDataSet");
        Assert.assertEquals(getValue(table, "Method", 0), "<clinit>");
        Assert.assertEquals(getValue(table, "Message", 0), "Detect CPUs");
        Assert.assertEquals(getValue(table, "Arguments", 0), "Using 3 processors");
        Assert.assertEquals(getValue(table, LogFiles.lineNumberColumn, 0), 1);
        Assert.assertEquals(getValue(table, LogFiles.directoryColumn, 0), "./");
        Assert.assertEquals(getValue(table, LogFiles.filenameColumn, 0), f.getName());
        Assert.assertNull(getValue(table, LogFiles.parseErrorColumn, 0));
    }

    @Test
    public void parseMalformedLog() throws IOException {
        String s = "2019-03-22 09:27:10.292,worker,INFO,ip-172-31-12-140,computation-0,org.hillview.dataset.LocalDataSet," +
                "lambda$map$0,Starting map,org.hillview.dataset.LocalDataSet(2)@ip-172-31-12-140:org.hillview.storage." +
                "FileSetDescription$FileReference@3cfb3d27:org.hillview.maps.LoadFilesMapper\n" +
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
        Assert.assertEquals(table.toString(), "Table[14x2]");
        Assert.assertNull(getValue(table, LogFiles.parseErrorColumn, 0));
        Assert.assertNotNull(getValue(table, LogFiles.parseErrorColumn, 1));
    }
}
