/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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

package org.hillview.utils;

import org.hillview.storage.CsvFileReader;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;


public class CsvFileObject {
    private final Path dataPath;
    @Nullable
    private final Path schemaPath;

    public CsvFileObject(Path path, @Nullable Path schema) {
        this.dataPath = path;
        this.schemaPath = schema;
    }

    public ITable loadTable() throws IOException {
        Schema schema = null;
        if (this.schemaPath != null) {
            String s = new String(Files.readAllBytes(this.schemaPath));
            schema = Schema.fromJson(s);
        }

        CsvFileReader.CsvConfiguration config = new CsvFileReader.CsvConfiguration();
        config.allowFewerColumns = false;
        config.hasHeaderRow = true;
        config.allowMissingData = false;
        config.schema = schema;
        CsvFileReader r = new CsvFileReader(this.dataPath, config);

        ITable tbl = r.read();
        return Converters.checkNull(tbl);
    }

    @Override
    public String toString() {
        return "CsvFile " + this.dataPath.toString();
    }
}
