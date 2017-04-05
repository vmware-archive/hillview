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

package org.hiero.storage;

import org.hiero.table.Schema;
import org.hiero.table.api.IColumn;
import org.hiero.table.api.IRowIterator;
import org.hiero.table.api.ITable;
import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;

public class CsvFileWriter {
    protected char separator = ',';
    protected boolean writeHeaderRow = true;
    protected final Path file;

    public CsvFileWriter(Path file) { this.file = file; }

    public void setSeparator(char separator) { this.separator = separator; }

    public void setWriteHeaderRow(boolean write) { this.writeHeaderRow = write; }

    public void writeTable(ITable table) throws IOException {
        Schema schema = table.getSchema();
        try (Writer fw = new FileWriter(this.file.toString());
            CSVWriter writer = new CSVWriter(fw, this.separator)) {
            String[] data = new String[schema.getColumnCount()];
            IColumn[] columns = new IColumn[data.length];
            int index = 0;
            for (String c : schema.getColumnNames()) {
                data[index] = c;
                columns[index] = table.getColumn(c);
                index++;
            }
            if (this.writeHeaderRow)
                writer.writeNext(data);
            IRowIterator rowIter = table.getMembershipSet().getIterator();
            int nextRow = rowIter.getNextRow();
            while (nextRow >= 0) {
                for (index = 0; index < columns.length; index++) {
                    data[index] = columns[index].asString(nextRow);
                    if (data[index] == null)
                        data[index] = "";
                }
                writer.writeNext(data, false);
                nextRow = rowIter.getNextRow();
            }
        }
    }
}
