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
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ConstantStringColumn;
import org.hillview.utils.Utilities;

import java.util.Map;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import io.krakens.grok.api.*;

/**
 * Reads Generic logs into ITable objects.
 */
public class GenericLogs {
    private Grok grok;
    private static final Pattern datePattern = Pattern.compile("(?:Jan(?:uary)?|Feb(?:ruary)?|" +
            "Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ob" +
            "er)?|Nov(?:ember)?|Dec(?:ember)?)\\s* (?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9]" +
            ")|(?:0?[1-9]|1[0-2])[/-](?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])[/-](?>\\d\\d" +
            "){1,2}|(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])[./-](?:0?[1-9]|1[0-2])[./-](?" +
            ">\\d\\d){1,2}(?!<[0-9])(?:2[0123]|[01]?[0-9]):(?:[0-5][0-9])(?::(?:(?:[0-5]?[0-9]" +
            "|60)(?:[:.,][0-9]+)?))(?![0-9])");

    public GenericLogs(String logFormat) {
        GrokCompiler grokCompiler = GrokCompiler.newInstance();
        grokCompiler.registerDefaultPatterns();
        grokCompiler.registerPatternFromClasspath("/patterns/log-patterns");
        this.grok = grokCompiler.compile(logFormat, true);
    }

    public class LogFileLoader extends TextFileLoader {
        LogFileLoader(final String path) {
            super(path);
        }

        void parse(String line, String[] output) {
            Match gm = GenericLogs.this.grok.match(line);
            final Map<String, Object> capture = gm.capture();
            if (capture.size() > 0) {
                int index = 0;
                for (Map.Entry<String,Object> entry : capture.entrySet()) {
                    output[index] = entry.getValue().toString().replace("\\n", "\n");
                    index += 1;
                }
            }
        }

        @Override
        public ITable load() {
            Schema schema;
            try (BufferedReader reader = new BufferedReader(
                    this.getFileReader())) {
                // Used to build up a log line that spans multiple file lines
                StringBuilder logLine = new StringBuilder();
                String fileLine; // Current line in the file
                boolean first = true;
                String[] fields = null;

                while (true) {
                    fileLine = reader.readLine();
                    if (fileLine != null) {
                        if (fileLine.trim().isEmpty())
                            continue;

                        // If there is no date in a fileLine we consider heuristically that it
                        // is a continuation of the previous logLine.
                        Matcher matchDate = datePattern.matcher(fileLine);
                        if (!matchDate.find()) {
                            if (logLine.length() != 0)
                                logLine.append("\\n");
                            logLine.append(fileLine);
                            continue;
                        }
                    }

                    // If we reach this point fileLine has a date or is null.
                    // We parse the logLine and save the fileLine for next time.
                    String logString = logLine.toString();
                    if (!logString.isEmpty()) {
                        logLine.setLength(0);
                        if (first) {
                            // This is the first logLine we are processing.
                            schema = new Schema();
                            Match gmatch = GenericLogs.this.grok.match(logString);
                            final Map<String, Object> capture = gmatch.capture();
                            for (Map.Entry<String, Object> entry : capture.entrySet())
                                schema.append(new ColumnDescription(entry.getKey(), ContentsKind.String));
                            this.columns = schema.createAppendableColumns();
                            fields = new String[this.columns.length];
                            first = false;
                        }
                        this.parse(logString, fields);
                        this.append(fields);
                    }
                    if (fileLine == null)
                        break;
                    logLine.append(fileLine);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
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
            IColumn[] cols = new IColumn[columnCount + 1];
            cols[0] = host;
            if (columnCount > 0)
                System.arraycopy(this.columns, 0, cols, 1, columnCount);
            return new Table(cols, this.filename, null);
        }
    }

    public TextFileLoader getFileLoader(String path) {
        return new LogFileLoader(path);
    }
}
