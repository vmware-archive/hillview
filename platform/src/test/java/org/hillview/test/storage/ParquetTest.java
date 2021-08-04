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

import org.hillview.storage.ParquetFileLoader;
import org.hillview.storage.ParquetFileWriter;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.DoubleArrayColumn;
import org.hillview.table.columns.IntArrayColumn;
import org.hillview.table.columns.StringArrayColumn;
import org.hillview.test.BaseTest;
import org.hillview.test.TestUtil;
import org.hillview.utils.Converters;
import org.hillview.utils.Randomness;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ParquetTest extends BaseTest {
    // Not yet checked-in into the repository
    private static final Path parquetInputFile = Paths.get(dataDir, "ontime", "2016_1.parquet");

    private static final Path parquetOutputFile = Paths.get(dataDir, "parquet", "test.parquet");

    @Test
    public void readTest() {
        ITable table;
        try {
            ParquetFileLoader pr = new ParquetFileLoader(parquetInputFile.toString(), false);
            table = pr.load();
        } catch (Exception ex) {
            // If the file is not present do not fail the test.
            return;
        }

        Assert.assertNotNull(table);
        Assert.assertEquals("Table[15x445827]", table.toString());
        IColumn first = table.getLoadedColumn("OriginCityName");
        Assert.assertEquals("Dallas/Fort Worth, TX", first.getString(0));
        if (toPrint) {
            System.out.println(table.getSchema());
            System.out.println(table.toLongString(100));
        }
    }

    @Test
    public void lazyReadTest() {
        ITable table;
        try {
            ParquetFileLoader pr = new ParquetFileLoader(parquetInputFile.toString(), true);
            table = pr.load();
        } catch (Exception ex) {
            // If the file is not present do not fail the test.
            return;
        }

        Assert.assertNotNull(table);
        Assert.assertEquals("Table[15x445827]", table.toString());
        IColumn first = table.getLoadedColumn("OriginCityName");
        Assert.assertEquals(first.getString(0), "Dallas/Fort Worth, TX");
        Table tbl = (Table) table;
        Assert.assertFalse(tbl.getColumns().get(1).isLoaded());
    }

    @Test
    public void writeTest() throws IOException {
        Files.deleteIfExists(parquetOutputFile);
        Randomness rn = new Randomness(1234);
        List<IColumn> columnList = new ArrayList<>();

        ColumnDescription intColumnDescription = new ColumnDescription("Int", ContentsKind.Integer);
        IntArrayColumn intArrayColumn = new IntArrayColumn(intColumnDescription, 2);
        intArrayColumn.set(0, rn.nextInt());
        intArrayColumn.set(1, rn.nextInt());
        columnList.add(intArrayColumn);

        ColumnDescription stringColumnDescription = new ColumnDescription("String", ContentsKind.String);
        StringArrayColumn stringArrayColumn = new StringArrayColumn(stringColumnDescription, 2);
        stringArrayColumn.set(0, "something");
        stringArrayColumn.set(1, null);
        columnList.add(stringArrayColumn);

        columnList.add(TestUtil.getRandDateArray(2, "Date"));

        ColumnDescription timeColumnDescription = new ColumnDescription("Time", ContentsKind.Time);
        DoubleArrayColumn timeArrayColumn = new DoubleArrayColumn(timeColumnDescription, 2);
        timeArrayColumn.set(0, (double) rn.nextInt(Converters.SECONDS_TO_DAY * Converters.MILLIS_TO_SECONDS));
        timeArrayColumn.set(1, (double) rn.nextInt(Converters.SECONDS_TO_DAY * Converters.MILLIS_TO_SECONDS));
        columnList.add(timeArrayColumn);

        ColumnDescription jsonColumnDescription = new ColumnDescription("Json", ContentsKind.Json);
        StringArrayColumn jsonArrayColumn = new StringArrayColumn(jsonColumnDescription, 2);
        jsonArrayColumn.set(0, "{\n" +
                "  \"cloud\": \"if\",\n" +
                "  \"brown\": 216284933.78439975,\n" +
                "  \"neck\": true,\n" +
                "  \"recent\": true,\n" +
                "  \"manufacturing\": 405661684.64231133,\n" +
                "  \"nine\": -1257746979.641677\n" +
                "}" );
        jsonArrayColumn.set(1, "{\n" +
                "  \"chance\": 239690427,\n" +
                "  \"outline\": -555912547,\n" +
                "  \"alone\": [\n" +
                "    true,\n" +
                "    \"progress\",\n" +
                "    1291815253.6504672,\n" +
                "    \"hidden\",\n" +
                "    [\n" +
                "      \"according\",\n" +
                "      -258144348.32382202,\n" +
                "      -280375743.720135,\n" +
                "      \"garage\",\n" +
                "      \"sick\",\n" +
                "      1215321142\n" +
                "    ],\n" +
                "    -311046396.6782801\n" +
                "  ],\n" +
                "  \"wooden\": \"myself\",\n" +
                "  \"go\": \"colony\",\n" +
                "  \"welcome\": true\n" +
                "}");
        columnList.add(jsonArrayColumn);

        ITable originalTable = new Table(columnList, null, null);
        ParquetFileWriter writer = new ParquetFileWriter(parquetOutputFile.toString());
        writer.writeTable(originalTable);

        ParquetFileLoader loader = new ParquetFileLoader(parquetOutputFile.toString(), false);
        ITable processedTable = loader.load();

        Assert.assertNotNull(processedTable);
        Assert.assertEquals(originalTable.toLongString(originalTable.getNumOfRows()),
                processedTable.toLongString(processedTable.getNumOfRows()));
    }
}
