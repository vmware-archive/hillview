/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use HillviewLogs file except in compliance with the License.
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

import org.apache.commons.text.StringEscapeUtils;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IAppendableColumn;
import org.hillview.table.api.ITable;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Reads Hillview logs into ITable objects.
 */
public class HillviewLogs extends LogFiles {
    private static final Schema schema = new Schema();
    private static final Pattern pattern = Pattern.compile("([^,]*),([^,]*),([^,]*),([^,]*),([^,]*)," +
            "([^,]*),([0-9]+),([^,]*),([^,]*),([^,]*),?(.*)");

    static {
        HillviewLogs.schema.append(new ColumnDescription(LogFiles.timestampColumnName, ContentsKind.LocalDate));
        HillviewLogs.schema.append(new ColumnDescription("Role", ContentsKind.String));
        HillviewLogs.schema.append(new ColumnDescription("Level", ContentsKind.String));
        HillviewLogs.schema.append(new ColumnDescription("Machine", ContentsKind.String));
        HillviewLogs.schema.append(new ColumnDescription("Thread", ContentsKind.String));
        HillviewLogs.schema.append(new ColumnDescription("SourceFile", ContentsKind.String));
        HillviewLogs.schema.append(new ColumnDescription("SourceLine", ContentsKind.Integer));
        HillviewLogs.schema.append(new ColumnDescription("Class", ContentsKind.String));
        HillviewLogs.schema.append(new ColumnDescription("Method", ContentsKind.String));
        HillviewLogs.schema.append(new ColumnDescription("Message", ContentsKind.String));
        HillviewLogs.schema.append(new ColumnDescription("Arguments", ContentsKind.String));
    }

    public static class LogFileLoader extends BaseLogLoader {
        LogFileLoader(final String path) {
            super(path);
        }

        /**
         * Return true on success.
         */
        boolean parse(String line, String[] output) {
            Matcher m = pattern.matcher(line);
            if (!m.find()) {
                Arrays.fill(output, null);
                return false;
            }
            output[0] = m.group(1); // Time
            output[1] = m.group(2); // Role
            output[2] = m.group(3); // Level
            output[3] = m.group(4); // Machine
            output[4] = m.group(5); // Thread
            output[5] = m.group(6); // SourceFile
            output[6] = m.group(7); // SourceLine
            output[7] = m.group(8); // Class
            output[8] = m.group(9); // Method
            output[9] = m.group(10); // Message
            String arguments = StringEscapeUtils.unescapeCsv(m.group(11));
            output[10] = arguments.replace("\\n", "\n");  // Arguments
            return true;
        }

        @Override
        public ITable load() {
            this.startFragment();
            assert this.lineNumber != null;
            assert this.parsingErrors != null;
            this.columns = schema.createAppendableColumns();
            try (BufferedReader reader = new BufferedReader(
                    new FileReader(this.filename))) {
                String[] fields = new String[this.columns.length];
                while (true) {
                    String line = reader.readLine();
                    if (line == null)
                        break;
                    if (line.trim().isEmpty()) {
                        this.currentRow++;
                        continue;
                    }
                    // rows are numbered from 0, lines from 1
                    this.lineNumber.append(this.currentRow + 1);
                    boolean success = this.parse(line, fields);
                    if (!success) {
                        for (IAppendableColumn column : this.columns) column.appendMissing();
                        this.parsingErrors.append(line);
                        this.currentRow++;
                        continue;
                    }
                    try {
                        this.append(fields);
                        // this increments currentRow on success
                    } catch (Exception ex) {
                        this.parsingErrors.append(line);
                        this.currentRow++;
                        continue;
                    }
                    this.parsingErrors.appendMissing();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.close(null);
            return this.createTable();
        }
    }

    @Nullable
    public static ITable parseLogFile(String file) {
        LogFileLoader reader = new LogFileLoader(file);
        return reader.load();
    }
}
