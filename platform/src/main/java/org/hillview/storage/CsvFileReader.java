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

package org.hillview.storage;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.hillview.table.BaseListColumn;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogManager;

import javax.annotation.Nullable;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * Knows how to read a CSV file (comma-separated file).
 */
public class CsvFileReader {
    public static class CsvConfiguration {
        /**
         * Field separator in CSV file.
         */
        public final char separator = ',';
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

    private final Path filename;
    private final CsvConfiguration configuration;
    @Nullable
    private Schema actualSchema;
    private int currentRow;
    private int currentColumn;
    @Nullable
    private BaseListColumn[] columns;
    private long currentField;
    @Nullable
    private String currentToken;
    @SuppressWarnings("FieldCanBeLocal")
    private int actualColumnCount;

    public CsvFileReader(final Path path, CsvConfiguration configuration) {
        this.filename = path;
        this.configuration = configuration;
        this.currentRow = 0;
        this.currentColumn = 0;
        this.currentField = 0;
        this.currentToken = null;
    }

    // May return null when an error occurs.
    @Nullable
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
                    HillviewLogManager.instance.logger.log(Level.INFO, "Creating schema");
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
                if (this.configuration.columnCount == 0) {
                    firstLine = reader.parseNext();
                    if (firstLine == null)
                        throw new RuntimeException("Cannot create schema from empty CSV file");
                    this.actualColumnCount = firstLine.length;
                }

                for (int i = 0; i < this.configuration.columnCount; i++) {
                    ColumnDescription cd = new ColumnDescription("Column " + Integer.toString(i),
                            ContentsKind.String, this.configuration.allowMissingData);
                    this.actualSchema.append(cd);
                }
            }

            Converters.checkNull(this.actualSchema);
            this.actualColumnCount = this.actualSchema.getColumnCount();
            List<IColumn> columns = new ArrayList<IColumn>(this.actualColumnCount);
            this.columns = new BaseListColumn[this.actualColumnCount];
            int index = 0;
            for (String col : this.actualSchema.getColumnNames()) {
                ColumnDescription cd = Converters.checkNull(this.actualSchema.getDescription(col));
                BaseListColumn column = BaseListColumn.create(cd);
                columns.add(column);
                this.columns[index++] = column;
            }

            if (firstLine != null)
                this.append(firstLine);
            while (true) {
                String[] line = reader.parseNext();
                if (line == null)
                    break;
                this.append(line);
            }

            reader.stopParsing();
            return new Table(columns);
        }
    }

    private void append(String[] data) {
        try {
            Converters.checkNull(this.columns);
            int columnCount = this.columns.length;
            this.currentColumn = 0;
            if (data.length > columnCount)
                this.error("Too many columns " + data.length + " vs " + columnCount);
            for (this.currentColumn = 0; this.currentColumn < data.length; this.currentColumn++) {
                this.currentToken = data[this.currentColumn];
                this.columns[this.currentColumn].parseAndAppendString(this.currentToken);
                this.currentField++;
                if ((this.currentField % 100000) == 0) {
                    System.out.print(".");
                    System.out.flush();
                }
            }
            if (data.length < columnCount) {
                if (!this.configuration.allowFewerColumns)
                    this.error("Too few columns " + data.length + " vs " + columnCount);
                else {
                    this.currentToken = "";
                    for (int i = data.length; i < columnCount; i++)
                        this.columns[i].parseAndAppendString(this.currentToken);
                }
            }
            this.currentRow++;
        } catch (Exception ex) {
            this.error(ex);
        }
    }

    private String errorMessage() {
        String columnName = "";
        if (this.columns != null) {
            columnName = (this.currentColumn < this.columns.length) ?
                    (" (" + this.columns[this.currentColumn].getName() + ")") : "";
        }

        return "Error while parsing CSV file " + this.filename.toString() +
                " line " + this.currentRow + " column " + this.currentColumn +
                columnName + (this.currentToken != null ? " token " + this.currentToken : "");
    }

    private void error(String message) {
        throw new RuntimeException(this.errorMessage() + ": " + message);
    }

    private void error(Exception ex) {
        throw new RuntimeException(this.errorMessage(), ex);
    }
}
