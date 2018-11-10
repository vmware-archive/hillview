/*
 * Copyright (c) 2018 MapleLabs. All Rights Reserved.
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

import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IAppendableColumn;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ConstantStringColumn;
import org.hillview.table.columns.StringListColumn;
import org.hillview.utils.DateParsing;
import org.hillview.utils.GrokExtra;
import org.hillview.utils.Utilities;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.io.BufferedReader;
import java.io.IOException;

import io.krakens.grok.api.*;

import javax.annotation.Nullable;

/**
 * Reads Generic logs into ITable objects.
 */
public class GenericLogs {
    private final String logFormat;
    /**
     * This column name must appear in all log formats.
     */
    private static final String timestampColumnName = "Timestamp";

    public GenericLogs(String logFormat) {
        this.logFormat = logFormat;
    }

    public class LogFileLoader extends TextFileLoader {
        private Grok grok;
        private GrokCompiler grokCompiler;

        @Nullable
        private final Instant start;
        @Nullable
        private final Instant end;
        @Nullable
        DateParsing dateTimeParser = null;
        private final Grok dateTime;
        @Nullable
        private List<String> columnNames = null;
        // Store here the lines that did not match the log pattern
        private StringListColumn parsingErrors;

        LogFileLoader(final String path, @Nullable Instant start, @Nullable Instant end) {
            super(path);
            this.grokCompiler = GrokCompiler.newInstance();
            this.grokCompiler.registerDefaultPatterns();
            this.grokCompiler.registerPatternFromClasspath("/patterns/log-patterns");
            this.grok = grokCompiler.compile(logFormat, true);
            this.start = start;
            this.end = end;
            this.parsingErrors = new StringListColumn(
                new ColumnDescription("ParsingErrors", ContentsKind.String));
                    String originalPattern = this.grok.getOriginalGrokPattern();
            String timestampPattern = GrokExtra.extractGroupPattern(
                    this.grokCompiler.getPatternDefinitions(),
                    originalPattern, GenericLogs.timestampColumnName);
            if (timestampPattern == null)
                this.error("Pattern " + logFormat + " does not contain column named 'Timestamp'");
            this.dateTime = this.grokCompiler.compile(
                        "%{" + timestampPattern + ":" + GenericLogs.timestampColumnName + "}" +
                        "%{GREEDYDATA}", true);
        }

        boolean parse(String line, String[] output) {
            assert this.columnNames != null;
            Match gm = this.grok.match(line);
            final Map<String, Object> capture = gm.capture();
            if (capture.size() > 0) {
                int index = 0;
                for (String col : this.columnNames) {
                    output[index] = capture.get(col).toString().replace("\\n", "\n").trim();
                    index += 1;
                }
                return true;
            }
            return false;
        }

        @Override
        public ITable load() {
            // Create the schema and allocate the columns based on the pattern.
            Schema schema = new Schema();
            this.columnNames = GrokExtra.getColumnsFromPattern(this.grok);
            for (String colName: this.columnNames) {
                ContentsKind kind = ContentsKind.String;
                if (colName.equals(GenericLogs.timestampColumnName))
                    kind = ContentsKind.Date;
                schema.append(new ColumnDescription(colName, kind));
            }
            this.columns = schema.createAppendableColumns();
            String[] fields = new String[this.columns.length];

            boolean first = true;
            try (BufferedReader reader = new BufferedReader(
                    this.getFileReader())) {
                // Used to build up a log line that spans multiple file lines
                StringBuilder logLine = new StringBuilder();
                String fileLine; // Current line in the file

                while (true) {
                    fileLine = reader.readLine();
                    if (fileLine != null) {
                        if (fileLine.trim().isEmpty())
                            continue;
                        Match gm = this.dateTime.match(fileLine);
                        boolean hasDate = !gm.isNull();

                        // If there is no date in a fileLine we consider heuristically that it
                        // is a continuation of the previous logLine.
                        if (!hasDate) {
                            if (first)
                                // This suggests that the pattern is wrong.
                                // This would cause all lines to be concatenated into
                                // a single giant line, and we want to avoid that.
                                this.error("No timestamp on the first line");
                            if (logLine.length() != 0)
                                logLine.append("\\n");
                            logLine.append(fileLine);
                            continue;
                        } else {
                            if (this.start != null || this.end != null) {
                                String date = gm.capture().get(GenericLogs.timestampColumnName).toString();
                                if (this.dateTimeParser == null)
                                    this.dateTimeParser = new DateParsing(date);
                                Instant parsed = this.dateTimeParser.parse(date);
                                if (this.start != null && this.start.isAfter(parsed))
                                    continue;
                                if (this.end != null && this.end.isBefore(parsed))
                                    // We assume timestamps are monotone, and thus
                                    // we won't see another one smaller.  So we end
                                    // parsing here.
                                    fileLine = null;
                            }
                        }
                    }

                    first = false;
                    // If we reach this point fileLine has a date or is null.
                    // We parse the logLine and save the fileLine for next time.
                    String logString = logLine.toString();
                    if (!logString.isEmpty()) {
                        logLine.setLength(0);

                        if (this.parse(logString, fields)) {
                            this.append(fields);
                            this.parsingErrors.appendMissing();
                        } else {
                            for (IAppendableColumn c: this.columns)
                                c.appendMissing();
                            this.parsingErrors.append(logString);
                        }
                    }
                    if (fileLine == null)
                        break;
                    logLine.append(fileLine);
                }
            } catch (IOException e) {
                this.error(e.getMessage());
            }
            this.close(null);
            int size;
            int columnCount;
            if (this.columns == null)
                columnCount = 0;
            else
                columnCount = this.columns.length;
            if (columnCount == 0)
                size = 0;
            else
                size = this.columns[0].sizeInRows();
            // Create a new column for the host
            IColumn host = new ConstantStringColumn(
                    new ColumnDescription("Host", ContentsKind.String), size, Utilities.getHostName());
            // Create a new column for the FileName
            IColumn fileName = new ConstantStringColumn(
                    new ColumnDescription("FileName", ContentsKind.String), size, this.filename);
            IColumn[] cols = new IColumn[columnCount + 3];
            cols[0] = host;
            cols[1] = fileName;
            if (columnCount > 0)
                System.arraycopy(this.columns, 0, cols, 2, columnCount);
            cols[cols.length - 1] = this.parsingErrors;
            return new Table(cols, this.filename, null);
        }
    }

    public TextFileLoader getFileLoader(String path, @Nullable Instant start, @Nullable Instant end) {
        return new LogFileLoader(path, start, end);
    }

    public TextFileLoader getFileLoader(String path) {
        return this.getFileLoader(path, null, null);
    }
}
