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

package org.hillview.storage;

import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;


public class CsvFileObject {
    private final Path dataPath;
    @Nullable
    private final CsvFileReader.CsvConfiguration config;

    public CsvFileObject(Path path, @Nullable CsvFileReader.CsvConfiguration config) {
        this.dataPath = path;
        this.config = config;
    }

    public ITable loadTable() throws IOException {
        CsvFileReader.CsvConfiguration config = this.config;
        if (config == null) {
            config = new CsvFileReader.CsvConfiguration();
            config.allowFewerColumns = false;
            config.hasHeaderRow = true;
            config.allowMissingData = false;
        }
        CsvFileReader r = new CsvFileReader(this.dataPath, config);

        ITable tbl = r.read();
        return Converters.checkNull(tbl);
    }

    @Override
    public String toString() {
        return "CsvFile " + this.dataPath.toString();
    }
}
