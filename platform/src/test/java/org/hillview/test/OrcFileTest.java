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

package org.hillview.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.hillview.storage.CsvFileLoader;
import org.hillview.storage.OrcFileLoader;
import org.hillview.storage.OrcFileWriter;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.StringArrayColumn;
import org.hillview.utils.TestTables;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class OrcFileTest extends BaseTest {
    private static final String orcFolder = "../data/orc/";
    private static final String orcOutFile = "test.orc";

    private void deleteOrcFile(String folder, String file) {
        File f = new File(Paths.get(folder, file).toString());
        if (f.exists()) {
            boolean success = f.delete();
            Assert.assertTrue(success);
        }
        f = new File(Paths.get(folder, "." + file + ".crc").toString());
        if (f.exists()) {
            boolean success = f.delete();
            Assert.assertTrue(success);
        }
    }

    @Test
    public void writeSmallFileTest() {
        String orcFile = orcFolder + "tmp.orc";

        File file = new File(orcFile);
        if (file.exists()) {
            boolean success = file.delete();
            Assert.assertTrue(success);
        }
        Table t = TestTables.testTable();
        OrcFileWriter ofw = new OrcFileWriter(orcFile);
        ofw.writeTable(t);

        OrcFileLoader loader = new OrcFileLoader(orcFile, null, false);
        ITable table = loader.load();
        Assert.assertEquals(t.toLongString(20), table.toLongString(20));
        deleteOrcFile(orcFolder, "tmp.orc");
    }

    @Test
    public void convertCsvFileTest() {
        String file = CsvFileTest.ontimeFolder + "/" + CsvFileTest.csvFile;
        CsvFileLoader.CsvConfiguration config = new CsvFileLoader.CsvConfiguration();
        CsvFileLoader loader = new CsvFileLoader(file, config, null);
        ITable table = loader.load();
        String orcFile = "tmpX.orc";
        OrcFileWriter writer = new OrcFileWriter(orcFile);
        writer.writeTable(table);
        deleteOrcFile(".", orcFile);
    }

    @Test
    public void writeNullTest() {
        String orcFile = orcFolder + "tmp.orc";

        File file = new File(orcFile);
        if (file.exists()) {
            boolean success = file.delete();
            Assert.assertTrue(success);
        }
        ColumnDescription str = new ColumnDescription("Str", ContentsKind.String);
        StringArrayColumn strCol = new StringArrayColumn(str, 2);
        strCol.set(0, "Something");
        strCol.set(1, (String)null);
        List<IColumn> cols = new ArrayList<IColumn>();
        cols.add(strCol);
        Table tbl = new Table(cols, null, null);

        OrcFileWriter ofw = new OrcFileWriter(orcFile);
        ofw.writeTable(tbl);

        OrcFileLoader loader = new OrcFileLoader(orcFile, null, false);
        ITable table = loader.load();
        Assert.assertEquals(tbl.toLongString(20), table.toLongString(20));
        deleteOrcFile(orcFolder, "tmp.orc");
    }

    @Test
    public void readOrcColumnTest() throws IOException {
        String orcFile = orcFolder + orcOutFile;
        final Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(new Path(orcFile),
                OrcFile.readerOptions(conf));
        boolean[] include = new boolean[3];
        include[0] = false;  // the struct type in the schema
        include[1] = false;  // first column
        include[2] = true;   // second column
        Reader.Options options = new Reader.Options().include(include);
        RecordReader rows = reader.rows(options);
        TypeDescription schema = reader.getSchema();
        VectorizedRowBatch batch = schema.createRowBatch();
        while (rows.nextBatch(batch)) {
            for (int i = 0; i < batch.cols.length; i++) {
                if (!include[i+1])
                    continue;
                StringBuilder builder = new StringBuilder();
                for (int j = 0; j < batch.size; j++) {
                    batch.cols[i].stringifyValue(builder, j);
                    builder.append(" ");
                }
                System.out.println(builder);
            }
        }
        rows.close();
    }

    @Test
    public void readOrcFileTest() {
        String orcFile = orcFolder + orcOutFile;
        OrcFileLoader loader = new OrcFileLoader(orcFile, null, false);
        ITable table = loader.load();
        Table ref = TestTables.testRepTable();
        Assert.assertEquals(ref.toLongString(20), table.toLongString(20));
    }

    @Test
    public void readOrcFileTestWithSchema() {
        String orcFile = orcFolder + orcOutFile;
        Table ref = TestTables.testRepTable();
        Schema schema = ref.getSchema();  // Uses Category for a column
        String tmpSchema = "tmpOrcSchema";
        schema.writeToJsonFile(Paths.get(tmpSchema));

        OrcFileLoader loader = new OrcFileLoader(orcFile, tmpSchema, false);
        ITable table = loader.load();
        Assert.assertEquals(ref.toLongString(20), table.toLongString(20));
        Assert.assertEquals(ref.getSchema().toString(), table.getSchema().toString());

        File file = new File(tmpSchema);
        if (file.exists()) {
            boolean success = file.delete();
            Assert.assertTrue(success);
        }
    }

    @Test
    public void readOrcFileLazyTest() {
        String orcFile = orcFolder + orcOutFile;
        OrcFileLoader loader = new OrcFileLoader(orcFile, null, true);
        ITable table = loader.load();
        Table ref = TestTables.testRepTable();
        Assert.assertEquals(ref.toLongString(20), table.toLongString(20));
    }
}
