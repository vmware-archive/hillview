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

import org.apache.commons.io.FilenameUtils;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IAppendableColumn;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ConstantStringColumn;
import org.hillview.table.columns.IntListColumn;
import org.hillview.table.columns.StringListColumn;
import org.hillview.utils.DateParsing;
import org.hillview.utils.GrokExtra;
import org.hillview.utils.HillviewLogger;
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
    /**
     * Column name where the lines that are not parsed correctly are stored.
     */
    public static final String parseErrorColumn = "ParsingErrors";
    public static final String hostColumn = "Host";
    public static final String directoryColumn = "Directory";
    public static final String filenameColumn = "Filename";
    public static final String lineNumberColumn = "Line";

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
        /**
         * Pattern used for parsing timestamps.  Obtained from a column named 'Timestamp'.
         */
        @Nullable
        private final Grok dateTime;
        @Nullable
        private List<String> columnNames = null;
        private StringListColumn parsingErrors;
        private IntListColumn lineNumber;

        LogFileLoader(final String path, @Nullable Instant start, @Nullable Instant end) {
            super(path);
            this.grokCompiler = GrokCompiler.newInstance();
            this.grokCompiler.registerDefaultPatterns();
            this.grokCompiler.registerPatternFromClasspath("/patterns/log-patterns");
            this.grok = grokCompiler.compile(logFormat, true);
            this.start = start;
            this.end = end;
            this.parsingErrors = new StringListColumn(
                new ColumnDescription(parseErrorColumn, ContentsKind.String));
            this.lineNumber = new IntListColumn(
                    new ColumnDescription(lineNumberColumn, ContentsKind.Integer));
            String originalPattern = this.grok.getOriginalGrokPattern();
            String timestampPattern = GrokExtra.extractGroupPattern(
                    this.grokCompiler.getPatternDefinitions(),
                    originalPattern, GenericLogs.timestampColumnName);
            if (timestampPattern == null) {
                HillviewLogger.instance.warn("Pattern does not contain column named 'Timestamp'",
                        "{0}", originalPattern);
                this.dateTime = null;
            } else {
                this.dateTime = this.grokCompiler.compile(
                        "%{" + timestampPattern + ":" + GenericLogs.timestampColumnName + "}", true);
            }
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
            // True if the first non-empty line does not have a timestamp
            boolean firstTimestampIsMissing = this.dateTime == null;
            int currentLineNumber = 0;
            int previousLineNumber = 0;
            try (BufferedReader reader = new BufferedReader(
                    this.getFileReader())) {
                // Used to build up a log line that spans multiple file lines
                StringBuilder logLine = new StringBuilder();
                String fileLine; // Current line in the file

                while (true) {
                    currentLineNumber++;
                    fileLine = reader.readLine();
                    if (fileLine != null) {
                        if (fileLine.trim().isEmpty())
                            continue;
                        @Nullable
                        String currentTimestamp = null;
                        if (this.dateTime != null) {
                            Match gm = this.dateTime.match(fileLine);
                            if (!gm.isNull())
                                currentTimestamp = gm.capture()
                                        .get(GenericLogs.timestampColumnName)
                                        .toString();
                            else if (first)
                                // If the first line does not have a timestamp
                                // it may be that the pattern supplied by the user
                                // is actually wrong.   We do not want to end up
                                // concatenating all log lines into one big line.
                                firstTimestampIsMissing = true;
                        }

                        first = false;
                        // If there is no timestamp in a fileLine we consider heuristically that it
                        // is a continuation of the previous logLine.
                        if (currentTimestamp == null && !firstTimestampIsMissing) {
                            if (logLine.length() != 0)
                                logLine.append("\\n");
                            logLine.append(fileLine);
                            continue;
                        } else {
                            if (currentTimestamp != null &&
                                    (this.start != null || this.end != null)) {
                                if (this.dateTimeParser == null)
                                    this.dateTimeParser = new DateParsing(currentTimestamp);
                                Instant parsed = this.dateTimeParser.parse(currentTimestamp);
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

                    // If we reach this point fileLine has a date or is null.
                    // We parse the logLine and save the fileLine for next time.
                    String logString = logLine.toString();
                    if (!logString.isEmpty()) {
                        logLine.setLength(0);

                        this.lineNumber.append(previousLineNumber);
                        if (this.parse(logString, fields)) {
                            this.append(fields);
                            this.parsingErrors.appendMissing();
                        } else {
                            for (IAppendableColumn c: this.columns)
                                c.appendMissing();
                            this.parsingErrors.append(logString);
                        }
                    }
                    previousLineNumber = currentLineNumber;
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
                    new ColumnDescription(hostColumn, ContentsKind.String), size, Utilities.getHostName());
            IColumn fileName = new ConstantStringColumn(
                    new ColumnDescription(filenameColumn, ContentsKind.String),
                    size, FilenameUtils.getName(this.filename));
            IColumn directory = new ConstantStringColumn(
                    new ColumnDescription(directoryColumn, ContentsKind.String),
                    size, FilenameUtils.getPath(this.filename));
            IColumn[] cols = new IColumn[columnCount + 5];
            cols[0] = host;
            cols[1] = directory;
            cols[2] = fileName;
            cols[3] = this.lineNumber;
            if (columnCount > 0)
                System.arraycopy(this.columns, 0, cols, 4, columnCount);
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
