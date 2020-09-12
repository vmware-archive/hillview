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

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.hillview.table.LazySchema;
import org.hillview.storage.CsvFileLoader;
import org.hillview.storage.CsvFileWriter;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.StringListColumn;
import org.hillview.table.columns.IntListColumn;
import org.hillview.test.BaseTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Test read/write access to CSV files.
 */
public class CsvFileTest extends BaseTest {
    private static final String dataFolder = ".." + File.separator + "data";
    static final String ontimeFolder = dataFolder + File.separator + "ontime";
    private static final String criteoFolder = dataFolder + File.separator + "criteo";
    static final String csvFile = "On_Time_Sample.csv";
    private static final String schemaFile = "On_Time.schema";
    private static final String criteoFile = "criteoTab.csv";
    private static final String criteoSchema = "criteo.schema";
    private static final String criteoCompressed = "criteoTab.gz";
    private static final String weatherFile = "weather.csv";

    //@Test
    public void letterPairs() {
        for (char c = 'A'; c <= 'Z'; c++) {
            for (char d = 'a'; d <= 'z'; d++) {
                System.out.print("\"" + c + d + "\",");
            }
        }
    }

    private ITable readTable(String folder, String file, boolean header) {
        Path path = Paths.get(folder, file);
        CsvFileLoader.Config config = new CsvFileLoader.Config();
        config.allowFewerColumns = false;
        config.hasHeaderRow = header;
        CsvFileLoader r = new CsvFileLoader(path.toString(), config, new LazySchema());
        return r.load();
    }

    private void readFileWithSchema(String criteoFile) {
        Path schemaPath = Paths.get(criteoFolder, criteoSchema);
        Path path = Paths.get(criteoFolder, criteoFile);
        CsvFileLoader.Config config = new CsvFileLoader.Config();
        config.allowFewerColumns = false;
        config.hasHeaderRow = false;
        config.separator = '\t';
        CsvFileLoader r = new CsvFileLoader(path.toString(), config, new LazySchema(schemaPath.toString()));
        ITable t = r.load();
        Assert.assertNotNull(t);
    }

    @Test
    public void readCsvFileWithSchemaCriteoTest() {
        readFileWithSchema(criteoFile);
    }

    @Test
    public void readGzFileWithSchemaTest() {
        readFileWithSchema(criteoCompressed);
    }

    @Test
    public void readWeatherData() {
        ITable table = this.readTable(dataFolder, weatherFile, true);
        Assert.assertNotNull(table);
        Schema s = table.getSchema();
        Assert.assertEquals(25, s.getColumnCount());
    }

    @Test
    public void readCsvFileTest() {
        ITable t = this.readTable(ontimeFolder, csvFile, false);
        Assert.assertNotNull(t);
    }

    @Test
    public void readCsvFileWithSchemaTest() {
        Path schemaPath = Paths.get(ontimeFolder, schemaFile);
        Path path = Paths.get(ontimeFolder, csvFile);
        CsvFileLoader.Config config = new CsvFileLoader.Config();
        config.allowFewerColumns = false;
        config.hasHeaderRow = true;
        CsvFileLoader r = new CsvFileLoader(path.toString(), config, new LazySchema(schemaPath.toString()));
        ITable t = r.load();
        Assert.assertNotNull(t);
    }

    @Test
    public void readCsvFileGuessSchemaTest() {
        Path path = Paths.get(ontimeFolder, csvFile);
        CsvFileLoader.Config config = new CsvFileLoader.Config();
        config.allowFewerColumns = false;
        config.hasHeaderRow = true;
        CsvFileLoader r = new CsvFileLoader(path.toString(), config, new LazySchema());
        ITable t = r.load();
        Assert.assertNotNull(t);
    }

    @Test
    public void readUTF16FileTest() {
        Path path = Paths.get(dataDir + "/", "utf16-data.csv");
        CsvFileLoader.Config config = new CsvFileLoader.Config();
        config.allowFewerColumns = false;
        config.hasHeaderRow = true;
        CsvFileLoader r = new CsvFileLoader(path.toString(), config, new LazySchema());
        ITable t = r.load();
        Assert.assertNotNull(t);
        Assert.assertEquals("Table[3x5]", t.toString());
    }

    private void writeReadTable(ITable table) throws IOException {
        UUID uid = UUID.randomUUID();
        String tmpFileName = uid.toString();
        String path = "./" + tmpFileName;
        UUID uid1 = UUID.randomUUID();
        String tmpFileName1 = uid1.toString();
        Path schemaPath = Paths.get(".", tmpFileName1);

        try {
            CsvFileWriter writer = new CsvFileWriter(path);
            writer.setWriteHeaderRow(true);
            writer.writeTable(table);

            table.getSchema().writeToJsonFile(schemaPath);

            CsvFileLoader.Config config = new CsvFileLoader.Config();
            config.allowFewerColumns = false;
            config.hasHeaderRow = true;
            CsvFileLoader r = new CsvFileLoader(path, config, new LazySchema(schemaPath.toString()));
            ITable t = r.load();
            Assert.assertNotNull(t);

            String ft = table.toLongString(table.getNumOfRows());
            String st = t.toLongString(t.getNumOfRows());
            Assert.assertEquals(ft, st);
        } finally {
            if (Files.exists(Paths.get(path)))
                Files.delete(Paths.get(path));
            if (Files.exists(schemaPath))
                Files.delete(schemaPath);
        }
    }

    @Test
    public void writeSmallFileTest() throws IOException {
        ColumnDescription nulls = new ColumnDescription("AllNulls", ContentsKind.String);
        StringListColumn first = new StringListColumn(nulls);
        first.append(null);
        first.append(null);
        ColumnDescription empty = new ColumnDescription("AllEmpty", ContentsKind.String);
        StringListColumn second = new StringListColumn(empty);
        second.append("");
        second.append("");
        ColumnDescription integers = new ColumnDescription("Integers", ContentsKind.Integer);
        IntListColumn third = new IntListColumn(integers);
        third.append(0);
        third.appendMissing();
        List<IColumn> cols = new ArrayList<IColumn>();
        cols.add(first);
        cols.add(second);
        cols.add(third);
        ITable table = new Table(cols, null, null);
        writeReadTable(table);
    }

    @Test
    public void csvWriterTest() throws IOException {
        // The Csv writer (Univocity) we were using had a bug,
        // reproduced with this test.
        String[] data = new String[]{ "", null };
        CsvWriterSettings settings = new CsvWriterSettings();
        CsvFormat format = new CsvFormat();
        settings.setFormat(format);
        settings.setEmptyValue("\"\"");
        settings.setNullValue("");
        String fileName = "tmp.csv";
        Writer fw = new FileWriter(fileName);
        CsvWriter writer = new CsvWriter(fw, settings);
        writer.writeRow(data);
        writer.close();
        fw.close();
        File file = new File(fileName);
        if (file.exists()) {
            @SuppressWarnings("unused")
            boolean ignored = file.delete();
        }
    }

    @Test
    public void writeCsvFileTest() throws IOException {
        ITable tbl = this.readTable(ontimeFolder, csvFile, false);
        Assert.assertNotNull(tbl);
        writeReadTable(tbl);
    }
}


