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
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.hillview.table.api.*;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.table.rows.GuessSchema;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.file.Paths;

/**
 * Knows how to read a CSV file (comma-separated file).
 */
public class CsvFileLoader extends TextFileLoader {
    public static class CsvConfiguration implements Serializable {
        /**
         * Field separator in CSV file.
         */
        public char separator = ',';
        /**
         * If true we allow a row to have fewer columns; the row is padded with "nulls".
         */
        public boolean allowFewerColumns;
        /**
         * If true the file is expected to have a header row.
         */
        public boolean hasHeaderRow;
    }

    private final CsvConfiguration configuration;
    @Nullable
    private Schema actualSchema;
    @Nullable
    private final String schemaPath;

    public CsvFileLoader(String path, CsvConfiguration configuration, @Nullable String schemaPath) {
        super(path);
        this.configuration = configuration;
        this.schemaPath = schemaPath;
        this.allowFewerColumns = configuration.allowFewerColumns;
    }

    public ITable load() {
        if (!Utilities.isNullOrEmpty(this.schemaPath))
            this.actualSchema = Schema.readFromJsonFile(Paths.get(this.schemaPath));

        Reader file = null;
        try {
            file = this.getFileReader();
            CsvParserSettings settings = new CsvParserSettings();
            CsvFormat format = new CsvFormat();
            format.setDelimiter(this.configuration.separator);
            settings.setFormat(format);
            settings.setIgnoreTrailingWhitespaces(true);
            settings.setEmptyValue("");
            settings.setNullValue(null);
            if (this.actualSchema != null)
                settings.setMaxColumns(this.actualSchema.getColumnCount());
            else
                settings.setMaxColumns(50000);
            CsvParser reader = new CsvParser(settings);
            reader.beginParsing(file);

            if (this.configuration.hasHeaderRow) {
                @Nullable
                String[] line = null;
                try {
                    line = reader.parseNext();
                } catch (Exception ex) {
                    this.error(ex.getMessage());
                }
                if (line == null)
                    throw new RuntimeException("Missing header row " + this.filename);
                if (this.actualSchema == null) {
                    HillviewLogger.instance.info("Creating schema");
                    this.actualSchema = new Schema();
                    int index = 0;
                    for (String col : line) {
                        if ((col == null) || col.isEmpty())
                            col = this.actualSchema.newColumnName("Column_" + Integer.toString(index));
                        col = this.actualSchema.newColumnName(col);
                        ColumnDescription cd = new ColumnDescription(col,
                                ContentsKind.String);
                        this.actualSchema.append(cd);
                        index++;
                    }
                } else {
                    this.currentRow++;
                }
            }

            String[] firstLine = null;
            if (this.actualSchema == null) {
                int columnCount;
                this.actualSchema = new Schema();
                firstLine = reader.parseNext();
                if (firstLine == null)
                    throw new RuntimeException("Cannot create schema from empty CSV file");
                columnCount = firstLine.length;

                for (int i = 0; i < columnCount; i++) {
                    ColumnDescription cd = new ColumnDescription("Column " + Integer.toString(i),
                            ContentsKind.String);
                    this.actualSchema.append(cd);
                }
            }

            Converters.checkNull(this.actualSchema);
            this.columns = this.actualSchema.createAppendableColumns();

            if (firstLine != null)
                this.append(firstLine);
            while (true) {
                @Nullable
                String[] line = null;
                try {
                    line = reader.parseNext();
                } catch (Exception ex) {
                    this.error(ex.getMessage());
                }
                if (line == null)
                    break;
                this.append(line);
            }

            IColumn[] sealed = new IColumn[this.columns.length];
            reader.stopParsing();
            IMembershipSet ms = null;
            for (int ci = 0; ci < this.columns.length; ci++) {
                IAppendableColumn c = this.columns[ci];
                IColumn s = c.seal();
                if (ms == null)
                    ms = new FullMembershipSet(s.sizeInRows());
                if (Utilities.isNullOrEmpty(this.schemaPath)) {
                    // TODO: this is not enough, we have to reconcile between machines.
                    GuessSchema gs = new GuessSchema();
                    GuessSchema.SchemaInfo info = gs.guess((IStringColumn)s);
                    if (info.kind != ContentsKind.String &&
                            info.kind != ContentsKind.None)  // all elements are null
                        sealed[ci] = s.convertKind(info.kind, c.getName(), ms);
                    else
                        sealed[ci] = s;
                } else {
                    sealed[ci] = s;
                }
                Converters.checkNull(sealed[ci]);
            }

            return new Table(sealed, this.filename, null);
        } finally {
            this.close(file);
        }
    }
}
