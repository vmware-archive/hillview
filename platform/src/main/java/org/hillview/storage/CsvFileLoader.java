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
import org.hillview.LazySchema;
import org.hillview.table.api.*;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.membership.FullMembershipSet;
import org.hillview.table.rows.GuessSchema;
import org.hillview.utils.HillviewLogger;

import javax.annotation.Nullable;
import java.io.*;

/**
 * Knows how to read a CSV file (comma-separated file).
 */
public class CsvFileLoader extends TextFileLoader {
    public static class Config implements Serializable {
        static final long serialVersionUID = 1;
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

    private final Config configuration;
    @Nullable
    private Schema actualSchema;
    private final LazySchema schema;
    private boolean guessSchema;

    public CsvFileLoader(String path, Config configuration, LazySchema schema) {
        super(path);
        this.configuration = configuration;
        this.schema = schema;
        this.allowFewerColumns = configuration.allowFewerColumns;
        this.guessSchema = this.schema.isNull();
    }

    @Nullable
    Reader file;
    @Nullable
    CsvParser reader;
    @Nullable
    String[] firstLine;

    @Override
    public void prepareLoading() {
        this.actualSchema = this.schema.getSchema();
        this.file = this.getFileReader();
        CsvParserSettings settings = new CsvParserSettings();
        CsvFormat format = new CsvFormat();
        format.setDelimiter(this.configuration.separator);
        settings.setFormat(format);
        settings.setIgnoreTrailingWhitespaces(true);
        settings.setEmptyValue("");
        settings.setNullValue(null);
        settings.setReadInputOnSeparateThread(false);
        if (this.actualSchema != null)
            settings.setMaxColumns(this.actualSchema.getColumnCount());
        else
            settings.setMaxColumns(50000);
        settings.setMaxCharsPerColumn(100000);
        this.reader = new CsvParser(settings);
        this.reader.beginParsing(file);

        if (this.configuration.hasHeaderRow) {
            @Nullable
            String[] line = null;
            try {
                line = this.reader.parseNext();
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
                        col = this.actualSchema.newColumnName("Column_" + index);
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

        if (this.actualSchema == null) {
            int columnCount;
            this.actualSchema = new Schema();
            this.firstLine = reader.parseNext();
            if (this.firstLine == null)
                throw new RuntimeException("Cannot create schema from empty CSV file");
            columnCount = this.firstLine.length;

            for (int i = 0; i < columnCount; i++) {
                ColumnDescription cd = new ColumnDescription("Column " + i,
                        ContentsKind.String);
                this.actualSchema.append(cd);
            }
        }
    }

    public ITable loadFragment(int maxRows, boolean skip) {
        assert this.reader != null;
        assert this.actualSchema != null;
        this.columns = this.actualSchema.createAppendableColumns();

        if (this.firstLine != null) {
            this.append(this.firstLine);
            this.firstLine = null;
        }

        while (maxRows != 0) {
            @Nullable
            String[] line = null;
            try {
                line = this.reader.parseNext();
            } catch (Exception ex) {
                this.error(ex.getMessage());
            }
            if (line == null)
                break;
            if (!skip)
                this.append(line);
            if (maxRows > 0)
                maxRows--;
        }

        IColumn[] sealed = new IColumn[this.columns.length];
        IMembershipSet ms = null;
        for (int ci = 0; ci < this.columns.length; ci++) {
            IAppendableColumn c = this.columns[ci];
            IColumn s = c.seal();
            if (ms == null)
                ms = new FullMembershipSet(s.sizeInRows());
            if (this.guessSchema) {
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
            assert sealed[ci] != null;
        }

        ITable result = new Table(sealed, this.filename, null);
        this.guessSchema = false;
        this.actualSchema = result.getSchema();
        return result;
    }

    @Override
    public void endLoading() {
        if (this.reader != null)
            reader.stopParsing();
        if (this.file != null)
            this.close(file);
    }
}
