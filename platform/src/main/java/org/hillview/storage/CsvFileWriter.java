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

import java.io.*;
import java.util.List;
import java.util.zip.GZIPOutputStream;

public class CsvFileWriter implements ITableWriter {
    private char separator = ',';
    private boolean writeHeaderRow = true;
    private final String fileName;
    private boolean compress = false;

    public CsvFileWriter(String fileName) { this.fileName = fileName; }

    public CsvFileWriter setCompress(boolean compress) { this.compress = compress; return this; }

    public CsvFileWriter setSeparator(char separator) { this.separator = separator; return this; }

    public CsvFileWriter setWriteHeaderRow(boolean write) { this.writeHeaderRow = write; return this; }

    public void writeTable(ITable table) {
        try {
            Schema schema = table.getSchema();
            List<IColumn> cols = table.getLoadedColumns(schema.getColumnNames());

            CsvWriterSettings settings = new CsvWriterSettings();
            CsvFormat format = new CsvFormat();
            format.setDelimiter(this.separator);
            settings.setFormat(format);
            settings.setEmptyValue("\"\"");
            settings.setNullValue(null);

            OutputStream output;
            FileOutputStream fs = null;
            if (this.compress) {
                String fn = this.fileName;
                if (!this.fileName.endsWith(".gz"))
                    fn += ".gz";
                fs = new FileOutputStream(fn);
                output = new GZIPOutputStream(fs);
            } else {
                output = new FileOutputStream(this.fileName);
            }
            CsvWriter writer = new CsvWriter(output, settings);

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
                for (index = 0; index < cols.size(); index++) {
                    IColumn colI = cols.get(index);
                    String d = colI.isMissing(nextRow) ? null : colI.asString(nextRow);
                    data[index] = d;
                }
                writer.writeRow(data);
                nextRow = rowIter.getNextRow();
            }
            writer.close();
            output.close();
            if (fs != null)
                fs.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
