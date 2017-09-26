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
 *
 */

package org.hillview.test;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.hillview.storage.CsvFileReader;
import org.hillview.storage.CsvFileWriter;
import org.hillview.table.*;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.IntListColumn;
import org.hillview.table.columns.StringListColumn;
import org.hillview.utils.Converters;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CsvReaderTest {
    private static final String dataFolder = "../data";
    private static final String csvFile = "On_Time_Sample.csv";
    private static final String schemaFile = "On_Time.schema";

    @Nullable
    private ITable readTable(String folder, String file) throws IOException {
        Path path = Paths.get(folder, file);
        CsvFileReader.CsvConfiguration config = new CsvFileReader.CsvConfiguration();
        config.allowFewerColumns = false;
        config.hasHeaderRow = true;
        config.allowMissingData = false;
        CsvFileReader r = new CsvFileReader(path, config);
        return r.read();
    }

    @Test
    public void readCsvFileTest() throws IOException {
        ITable t = this.readTable(dataFolder, csvFile);
        Assert.assertNotNull(t);
    }

    @Test
    public void readCsvFileWithSchemaTest() throws IOException {
        Path path = Paths.get(dataFolder, schemaFile);
        Schema schema = Schema.readFromJsonFile(path);
        path = Paths.get(dataFolder, csvFile);
        CsvFileReader.CsvConfiguration config = new CsvFileReader.CsvConfiguration();
        config.allowFewerColumns = false;
        config.hasHeaderRow = true;
        config.allowMissingData = false;
        config.schema = schema;
        CsvFileReader r = new CsvFileReader(path, config);
        ITable t = r.read();
        Assert.assertNotNull(t);
        /*
        System.gc();
        long mem = Runtime.getRuntime().totalMemory();
        long freeMem = Runtime.getRuntime().freeMemory();
        System.out.printf("Total memory %d, Free memory %d.", mem, freeMem);*/
    }

    private void writeReadTable(ITable table) throws IOException {
        UUID uid = UUID.randomUUID();
        String tmpFileName = uid.toString();
        Path path = Paths.get(".", tmpFileName);

        try {
            CsvFileWriter writer = new CsvFileWriter(path);
            writer.setWriteHeaderRow(true);
            writer.writeTable(table);

            CsvFileReader.CsvConfiguration config = new CsvFileReader.CsvConfiguration();
            config.allowFewerColumns = false;
            config.hasHeaderRow = true;
            config.allowMissingData = false;
            config.schema = table.getSchema();
            CsvFileReader r = new CsvFileReader(path, config);
            ITable t = r.read();
            Assert.assertNotNull(t);

            List<String> list = Files.readAllLines(path);
            for (String l : list)
                System.out.println(l);

            String ft = table.toLongString(table.getNumOfRows());
            String st = t.toLongString(t.getNumOfRows());
            Assert.assertEquals(ft, st);
        } finally {
            if (Files.exists(path))
                Files.delete(path);
        }
    }

    @Test
    public void writeSmallFileTest() throws IOException {
        ColumnDescription nulls = new ColumnDescription("AllNulls", ContentsKind.String, true);
        StringListColumn first = new StringListColumn(nulls);
        first.append((String)null);
        first.append((String)null);
        ColumnDescription empty = new ColumnDescription("AllEmpty", ContentsKind.String, true);
        StringListColumn second = new StringListColumn(empty);
        second.append("");
        second.append("");
        ColumnDescription integers = new ColumnDescription("Integers", ContentsKind.Integer, true);
        IntListColumn third = new IntListColumn(integers);
        third.append(0);
        third.appendMissing();
        List<IColumn> cols = new ArrayList<IColumn>();
        cols.add(first);
        cols.add(second);
        cols.add(third);
        ITable table = new Table(cols);
        writeReadTable(table);
    }

    //@Test
    public void csvWriterTest() throws IOException {
        // The Csv writer we are using has a bug, reproduced with this test.
        String[] data = new String[]{ "", null };
        CsvWriterSettings settings = new CsvWriterSettings();
        CsvFormat format = new CsvFormat();
        settings.setFormat(format);
        settings.setEmptyValue("\"\"");
        settings.setNullValue("");
        Writer fw = new FileWriter("tmp");
        CsvWriter writer = new CsvWriter(fw, settings);
        writer.writeRow(data);
        writer.close();
        fw.close();
    }

    @Test
    public void writeCsvFileTest() throws IOException {
        ITable tbl = this.readTable(dataFolder, csvFile);
        writeReadTable(Converters.checkNull(tbl));
    }
}


