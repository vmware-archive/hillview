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

package org.hillview.utils;

import org.hillview.storage.CsvFileLoader;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class TestUtils {
    /**
     * Prints a table to stdout.
     * @param table The table that is printed.
     */
    public static void printTable(String message, ITable table) {
        System.out.println(message + " " + table.toLongString(0));
    }

    public static ITable loadTableFromCSV(String dataFolder, String csvFile, String schemaFile) {
        Path schemaPath = Paths.get(dataFolder, schemaFile);
        Path path = Paths.get(dataFolder, csvFile);
        CsvFileLoader.Config config = new CsvFileLoader.Config();
        config.allowFewerColumns = false;
        config.hasHeaderRow = true;
        CsvFileLoader r = new CsvFileLoader(path.toString(), config, schemaPath.toString());
        return Converters.checkNull(r.load());
    }

    public static List<String> getNumericColumnNames(ITable table) {
        List<String> numericColNames = new ArrayList<String>();
        for (String colName : table.getSchema().getColumnNames()) {
            ContentsKind kind = table.getSchema().getDescription(colName).kind;
            if (kind == ContentsKind.Double || kind == ContentsKind.Integer) {
                numericColNames.add(colName);
            }
        }
        return numericColNames;
    }
}
