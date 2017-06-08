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


import org.hiero.storage.CsvFileReader;
import org.hiero.table.Schema;
import org.hiero.table.api.ITable;
import org.junit.Assert;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;


public class WideTableDataTest {
    static final String dataFolder = "../data";
    static final String csvFile = "trim_rows.csv";
    static final String schemaFile = "trim.schema";

    @Nullable
    ITable readTable(String folder, String file) throws IOException {
        Path path = Paths.get(folder, file);
        CsvFileReader.CsvConfiguration config = new CsvFileReader.CsvConfiguration();
        config.allowFewerColumns = true;
        config.hasHeaderRow = true;
        config.allowMissingData = false;
        CsvFileReader r = new CsvFileReader(path, config);
        return r.read();
    }

    public void WideTableColCorrelations() throws IOException {
        Path path = Paths.get(dataFolder, schemaFile);
        Schema schema = Schema.readFromJsonFile(path);
        path = Paths.get(dataFolder, csvFile);
        CsvFileReader.CsvConfiguration config = new CsvFileReader.CsvConfiguration();
        config.allowFewerColumns = true;
        config.hasHeaderRow = true;
        config.allowMissingData = true;
        config.schema = schema;
        CsvFileReader r = new CsvFileReader(path, config);
        ITable t = r.read();
        Assert.assertNotNull(t);
        System.gc();
        long mem = Runtime.getRuntime().totalMemory();
        long freeMem = Runtime.getRuntime().freeMemory();
        System.out.printf("Total mememory %d, Free memory %d.", mem, freeMem);
    }
}

