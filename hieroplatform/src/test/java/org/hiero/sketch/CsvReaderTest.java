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

package org.hiero.sketch;

import org.hiero.sketch.storage.CsvFileReader;
import org.hiero.sketch.storage.CsvFileWriter;
import org.hiero.sketch.table.GetTable;
import org.hiero.sketch.table.HashSubSchema;
import org.hiero.sketch.table.Schema;
import org.hiero.sketch.table.api.ITable;
import org.hiero.utils.Converters;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public class CsvReaderTest {
    static final String dataFolder = "../data";
    static final String csvFile = "On_Time_Sample.csv";
    static final String schemaFile = "On_Time.schema";

    @Nullable
    ITable readTable(String folder, String file) throws IOException {
        Path path = Paths.get(folder, file);
        CsvFileReader.CsvConfiguration config = new CsvFileReader.CsvConfiguration();
        config.allowFewerColumns = false;
        config.hasHeaderRow = true;
        config.allowMissingData = false;
        CsvFileReader r = new CsvFileReader(path, config);
        return r.read();
    }

    @Test
    public void ReadCsvFileTest() throws IOException {
        ITable t = this.readTable(dataFolder, csvFile);
        Assert.assertNotNull(t);
    }

    @Test
    public void ReadCsvFileWithSchemaTest() throws IOException {
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
    }

    @Test
    public void WriteCsvFileTest() throws IOException {
        ITable tbl = this.readTable(dataFolder, csvFile);
        Assert.assertNotNull(tbl);
        UUID uid = UUID.randomUUID();
        String tmpFileName = uid.toString();
        Path path = Paths.get(".", tmpFileName);

        try {
            CsvFileWriter writer = new CsvFileWriter(path);
            writer.setWriteHeaderRow(true);
            writer.writeTable(tbl);

            CsvFileReader.CsvConfiguration config = new CsvFileReader.CsvConfiguration();
            config.allowFewerColumns = false;
            config.hasHeaderRow = true;
            config.allowMissingData = false;
            config.schema = tbl.getSchema();
            CsvFileReader r = new CsvFileReader(path, config);
            ITable t = r.read();
            Assert.assertNotNull(t);

            String first = tbl.toLongString(tbl.getNumOfRows());
            String second = t.toLongString(tbl.getNumOfRows());
            Assert.assertEquals(first, second);
        } finally {
            if (Files.exists(path))
                Files.delete(path);
        }
    }

    //@Test // This is not really a test.
    public void SplitTable() throws IOException {
        String[] columns = {
                "DayOfWeek", "FlightDate", "UniqueCarrier",
                "Origin", "OriginCityName", "OriginState", "Dest", "DestState",
                "DepTime", "DepDelay", "ArrTime", "ArrDelay", "Cancelled",
                "ActualElapsedTime", "Distance"
        };

        Path path = Paths.get(dataFolder, schemaFile);
        Schema schema = Schema.readFromJsonFile(path);
        HashSubSchema subschema = new HashSubSchema(columns);
        Schema proj = schema.project(subschema);
        proj.writeToJsonFile(Paths.get(dataFolder, "short.schema"));

        // If non-zero, split each table into parts of this size.
        final int splitSize = 0;

        String prefix = "On_Time_On_Time_Performance";
        Path folder = Paths.get(dataFolder);
        Stream<Path> files = Files.walk(folder, 1);
        files.forEach(f -> {
            String filename = f.getFileName().toString();
            if (!filename.endsWith("csv")) return;
            if (!filename.startsWith(prefix)) return;
            String end = filename.substring(prefix.length() + 1);

            CsvFileReader.CsvConfiguration config = new CsvFileReader.CsvConfiguration();
            config.allowFewerColumns = false;
            config.hasHeaderRow = true;
            config.allowMissingData = false;
            config.schema = schema;
            CsvFileReader r = new CsvFileReader(f, config);

            ITable tbl = null;
            try {
                System.out.println("Reading " + f);
                tbl = r.read();
            } catch (IOException e) {
                e.printStackTrace();
            }
            Converters.checkNull(tbl);

            ITable p = tbl.project(proj);

            if (splitSize > 0) {
                List<ITable> pieces = GetTable.splitTable(p, splitSize);

                int index = 0;
                for (ITable t : pieces) {
                    String baseName = end.substring(0, end.lastIndexOf("."));
                    String name = baseName + "-" + Integer.toString(index) + ".csv";
                    Path outpath = Paths.get(dataFolder, name);
                    CsvFileWriter writer = new CsvFileWriter(outpath);
                    try {
                        System.out.println("Writing " + outpath);
                        writer.writeTable(t);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    index++;
                }
            } else {
                Path outpath = Paths.get(dataFolder, end);
                CsvFileWriter writer = new CsvFileWriter(outpath);
                try {
                    System.out.println("Writing " + outpath);
                    writer.writeTable(p);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
