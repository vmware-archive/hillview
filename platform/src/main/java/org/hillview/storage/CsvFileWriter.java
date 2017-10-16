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

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.hillview.table.Schema;
import org.hillview.table.api.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;

public class CsvFileWriter {
    private char separator = ',';
    private boolean writeHeaderRow = true;
    private final Path file;

    public CsvFileWriter(Path file) { this.file = file; }

    public void setSeparator(char separator) { this.separator = separator; }

    public void setWriteHeaderRow(boolean write) { this.writeHeaderRow = write; }

    public void writeTable(ITable table) throws IOException {
        Schema schema = table.getSchema();
        ColumnAndConverterDescription[] ccds =
                ColumnAndConverterDescription.create(schema.getColumnNames());
        ColumnAndConverter[] cols = table.getLoadedColumns(ccds);

        CsvWriterSettings settings = new CsvWriterSettings();
        CsvFormat format = new CsvFormat();
        format.setDelimiter(this.separator);
        settings.setFormat(format);
        settings.setEmptyValue("\"\"");
        settings.setNullValue(null);
        try (Writer fw = new FileWriter(this.file.toString())) {
            CsvWriter writer = new CsvWriter(fw, settings);

            String[] data = new String[schema.getColumnCount()];
            int index = 0;
            for (String c : schema.getColumnNames()) {
                data[index] = c;
                index++;
            }
            if (this.writeHeaderRow)
                writer.writeHeaders(data);
            IRowIterator rowIter = table.getMembershipSet().getIterator();
            int nextRow = rowIter.getNextRow();
            while (nextRow >= 0) {
                for (index = 0; index < cols.length; index++) {
                    String d = cols[index].isMissing(nextRow) ? null : cols[index].asString(nextRow);
                    data[index] = d;
                }
                writer.writeRow(data);
                nextRow = rowIter.getNextRow();
            }
        }
    }
}