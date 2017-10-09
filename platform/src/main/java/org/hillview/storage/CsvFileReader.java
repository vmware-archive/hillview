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
import org.hillview.table.api.IAppendableColumn;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;

import javax.annotation.Nullable;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;

/**
 * Knows how to read a CSV file (comma-separated file).
 */
public class CsvFileReader extends TextFileReader {
    public static class CsvConfiguration {
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
        /**
         * If not zero it specifies the expected number of columns.
         */
        @SuppressWarnings("CanBeFinal")
        public int columnCount;
        /**
         * If true columns are allowed to contain "nulls".
         */
        public boolean allowMissingData;
        /**
         * If non-null it specifies the expected file schema.
         * In this case columnCount is ignored.  If schema is not specified
         * all columns are treated as strings.
         */
        @Nullable
        public Schema schema;
    }

    private final CsvConfiguration configuration;
    @Nullable
    private Schema actualSchema;

    public CsvFileReader(final Path path, CsvConfiguration configuration) {
        super(path);
        this.configuration = configuration;
        this.allowFewerColumns = configuration.allowFewerColumns;
    }

    // May return null when an error occurs.
    public ITable read() throws IOException {
        if (this.configuration.schema != null)
            this.actualSchema = this.configuration.schema;

        try (Reader file = new FileReader(this.filename.toString())) {
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
                settings.setMaxColumns(1024);
            CsvParser reader = new CsvParser(settings);
            reader.beginParsing(file);

            if (this.configuration.hasHeaderRow) {
                @Nullable
                String[] line = reader.parseNext();
                if (line == null)
                    throw new RuntimeException("Missing header row " + this.filename.toString());
                if (this.configuration.schema == null) {
                    HillviewLogger.instance.info("Creating schema");
                    this.actualSchema = new Schema();
                    int index = 0;
                    for (String col : line) {
                        if ((col == null) || col.isEmpty())
                            col = this.actualSchema.newColumnName("Column_" + Integer.toString(index));
                        ColumnDescription cd = new ColumnDescription(col,
                                ContentsKind.String,
                                this.configuration.allowMissingData);
                        this.actualSchema.append(cd);
                        index++;
                    }
                } else {
                    this.currentRow++;
                }
            }

            String[] firstLine = null;
            if (this.actualSchema == null) {
                this.actualSchema = new Schema();
                int columnCount = this.configuration.columnCount;
                if (columnCount == 0) {
                    firstLine = reader.parseNext();
                    if (firstLine == null)
                        throw new RuntimeException("Cannot create schema from empty CSV file");
                    columnCount = firstLine.length;
                }

                for (int i = 0; i < columnCount; i++) {
                    ColumnDescription cd = new ColumnDescription("Column " + Integer.toString(i),
                            ContentsKind.String, this.configuration.allowMissingData);
                    this.actualSchema.append(cd);
                }
            }

            Converters.checkNull(this.actualSchema);
            this.columns = this.actualSchema.createAppendableColumns();

            if (firstLine != null)
                this.append(firstLine);
            while (true) {
                String[] line = reader.parseNext();
                if (line == null)
                    break;
                this.append(line);
            }

            reader.stopParsing();
            for (IAppendableColumn c: this.columns)
                c.seal();
            return new Table(columns);
        }
    }
}
