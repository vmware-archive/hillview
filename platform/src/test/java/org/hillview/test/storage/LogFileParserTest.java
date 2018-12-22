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
import org.hillview.storage.HillviewLogs;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.test.BaseTest;
import org.hillview.utils.Converters;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.*;

public class LogFileParserTest extends BaseTest {
    private Object getValue(ITable table, String col) {
        IColumn cc = table.getLoadedColumn(col);
        return Converters.checkNull(cc.getObject(0));
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
        Assert.assertEquals(table.toString(), "Table[12x4]");
        LocalDate date = LocalDate.of(2017, 10, 12);
        LocalTime time = LocalTime.of(2, 17, 42, 722000000);
        LocalDateTime dt = LocalDateTime.of(date, time);
        ZonedDateTime zdt = dt.atZone(ZoneId.systemDefault());
        Instant instant = zdt.toInstant();
        Assert.assertEquals(getValue(table, GenericLogs.timestampColumnName), instant);
        Assert.assertEquals(getValue(table, "Role"), "worker");
        Assert.assertEquals(getValue(table, "Level"), "INFO");
        Assert.assertEquals(getValue(table, "Machine"), "ubuntu");
        Assert.assertEquals(getValue(table, "Thread"), "main");
        Assert.assertEquals(getValue(table, "Class"), "org.hillview.dataset.LocalDataSet");
        Assert.assertEquals(getValue(table, "Method"), "<clinit>");
        Assert.assertEquals(getValue(table, "Message"), "Detect CPUs");
        Assert.assertEquals(getValue(table, "Arguments"), "Using 3 processors");
        Assert.assertEquals(getValue(table, GenericLogs.lineNumberColumn), 1);
        Assert.assertEquals(getValue(table, GenericLogs.directoryColumn), "./");
        Assert.assertEquals(getValue(table, GenericLogs.filenameColumn), f.getName());
    }
}
