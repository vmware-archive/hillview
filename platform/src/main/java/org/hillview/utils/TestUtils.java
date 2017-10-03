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

import org.hillview.storage.CsvFileReader;
import org.hillview.table.Schema;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IRowIterator;
import org.hillview.table.api.ITable;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TestUtils {
    /**
     * Prints a table to stdout.
     * @param table The table that is printed.
     */
    public static void printTable(ITable table) {
        StringBuilder header = new StringBuilder("| ");
        for (IColumn col : table.getColumns()) {
            header.append(col.getName()).append("\t| ");
        }
        header.append("\t|");
        System.out.println(header);
        IRowIterator rowIt = table.getRowIterator();
        int row = rowIt.getNextRow();
        while (row >= 0) {
            StringBuilder rowString = new StringBuilder("| ");
            for (IColumn col : table.getColumns()) {
                rowString.append(col.asString(row)).append("\t| ");
            }
            rowString.append("\t");
            System.out.println(rowString);
            row = rowIt.getNextRow();
        }
    }

    public static ITable loadTableFromCSV(String dataFolder, String csvFile, String schemaFile) throws IOException {
        Path path = Paths.get(dataFolder, schemaFile);
        Schema schema = Schema.readFromJsonFile(path);
        path = Paths.get(dataFolder, csvFile);
        CsvFileReader.CsvConfiguration config = new CsvFileReader.CsvConfiguration();
        config.allowFewerColumns = false;
        config.hasHeaderRow = true;
        config.allowMissingData = false;
        config.schema = schema;
        CsvFileReader r = new CsvFileReader(path, config);

        ITable table;
        table = r.read();
        table = Converters.checkNull(table);

        return table;
    }

    public static List<String> getNumericColumnNames(ITable table) {
        List<String> numericColNames = new ArrayList<String>();
        Set<String> colNames = table.getSchema().getColumnNames();
        for (String colName : colNames) {
            ContentsKind kind = table.getSchema().getDescription(colName).kind;
            if (kind == ContentsKind.Double || kind == ContentsKind.Integer) {
                numericColNames.add(colName);
            }
        }
        return numericColNames;
    }

}
