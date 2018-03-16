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

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.hillview.storage.CsvFileLoader;
import org.hillview.storage.CsvFileWriter;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.IntListColumn;
import org.hillview.table.columns.StringListColumn;
import org.hillview.utils.Converters;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CsvReaderTest extends BaseTest {
    private static final String ontimeFolder = "../data/ontime";
    private static final String criteoFolder = "../data/criteo";
    private static final String csvFile = "On_Time_Sample.csv";
    private static final String schemaFile = "On_Time.schema";
    private static final String criteoFile = "criteoTab.csv";
    private static final String criteoSchema = "criteo.schema";
    private static final String criteoCompressed = "criteoTab.gz";

    private ITable readTable(String folder, String file) {
        Path path = Paths.get(folder, file);
        CsvFileLoader.CsvConfiguration config = new CsvFileLoader.CsvConfiguration();
        config.allowFewerColumns = false;
        config.hasHeaderRow = false;
        CsvFileLoader r = new CsvFileLoader(path.toString(), config, null);
        return r.load();
    }

    @Test
    public void readCsvFileWithSchemaCriteoTest() {
        Path schemaPath = Paths.get(criteoFolder, criteoSchema);
        Path path = Paths.get(criteoFolder, criteoFile);
        CsvFileLoader.CsvConfiguration config = new CsvFileLoader.CsvConfiguration();
        config.allowFewerColumns = false;
        config.hasHeaderRow = false;
         config.separator = '\t';
        CsvFileLoader r = new CsvFileLoader(path.toString(), config, schemaPath.toString());
        ITable t = r.load();
        Assert.assertNotNull(t);
    }

    @Test
    public void readGzFileWithSchemaTest() {
        Path schemaPath = Paths.get(criteoFolder, criteoSchema);
        Path path = Paths.get(criteoFolder, criteoCompressed);
        CsvFileLoader.CsvConfiguration config = new CsvFileLoader.CsvConfiguration();
        config.allowFewerColumns = false;
        config.hasHeaderRow = false;
        config.separator = '\t';
        CsvFileLoader r = new CsvFileLoader(path.toString(), config, schemaPath.toString());
        System.out.println(path.getFileName().toString());
        ITable t = r.load();
        Assert.assertNotNull(t);
    }

    @Test
    public void readCsvFileTest() {
        ITable t = this.readTable(ontimeFolder, csvFile);
        Assert.assertNotNull(t);
    }

    @Test
    public void readCsvFileWithSchemaTest() {
        Path schemaPath = Paths.get(ontimeFolder, schemaFile);
        Path path = Paths.get(ontimeFolder, csvFile);
        CsvFileLoader.CsvConfiguration config = new CsvFileLoader.CsvConfiguration();
        config.allowFewerColumns = false;
        config.hasHeaderRow = true;
        CsvFileLoader r = new CsvFileLoader(path.toString(), config, schemaPath.toString());
        ITable t = r.load();
        Assert.assertNotNull(t);
    }

    @Test
    public void readCsvFileGuessSchemaTest() {
        Path path = Paths.get(ontimeFolder, csvFile);
        CsvFileLoader.CsvConfiguration config = new CsvFileLoader.CsvConfiguration();
        config.allowFewerColumns = false;
        config.hasHeaderRow = true;
        CsvFileLoader r = new CsvFileLoader(path.toString(), config, null);
        ITable t = r.load();
        Assert.assertNotNull(t);
    }

    @Test
    public void readUTF16FileTest() {
        Path path = Paths.get("../data/", "utf16-data.csv");
        CsvFileLoader.CsvConfiguration config = new CsvFileLoader.CsvConfiguration();
        config.allowFewerColumns = false;
        config.hasHeaderRow = true;
        CsvFileLoader r = new CsvFileLoader(path.toString(), config, null);
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

            CsvFileLoader.CsvConfiguration config = new CsvFileLoader.CsvConfiguration();
            config.allowFewerColumns = false;
            config.hasHeaderRow = true;
            CsvFileLoader r = new CsvFileLoader(path, config, schemaPath.toString());
            ITable t = r.load();
            Assert.assertNotNull(t);

            List<String> list = Files.readAllLines(Paths.get(path));
            for (String l : list)
                System.out.println(l);

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
        first.append((String)null);
        first.append((String)null);
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
        ITable table = new Table(cols, null);
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
        ITable tbl = this.readTable(ontimeFolder, csvFile);
        writeReadTable(Converters.checkNull(tbl));
    }
}


