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

import org.apache.commons.lang.StringEscapeUtils;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Reads Hillview logs into ITable objects.
 */
public class HillviewLogs {
    static final Schema schema = new Schema();
    static final Pattern pattern = Pattern.compile("([^,]*),([^,]*),([^,]*),([^,]*),([^,]*)," +
            "([^,]*),([^,]*),([^,]*),?(.*)");

    static {
        HillviewLogs.schema.append(new ColumnDescription("Time", ContentsKind.Date));
        HillviewLogs.schema.append(new ColumnDescription("Role", ContentsKind.Category));
        HillviewLogs.schema.append(new ColumnDescription("Level", ContentsKind.Category));
        HillviewLogs.schema.append(new ColumnDescription("Machine", ContentsKind.Category));
        HillviewLogs.schema.append(new ColumnDescription("Thread", ContentsKind.Category));
        HillviewLogs.schema.append(new ColumnDescription("Class", ContentsKind.Category));
        HillviewLogs.schema.append(new ColumnDescription("Method", ContentsKind.Category));
        HillviewLogs.schema.append(new ColumnDescription("Message", ContentsKind.Category));
        HillviewLogs.schema.append(new ColumnDescription("Arguments", ContentsKind.String));
    }

    public static class LogFileLoader extends TextFileLoader {
        public LogFileLoader(final String path) {
            super(path);
        }

        public void parse(String line, String[] output) {
            Matcher m = pattern.matcher(line);
            if (!m.find())
                this.error("Could not parse line");
            output[0] = m.group(1); // Time
            output[1] = m.group(2); // Role
            output[2] = m.group(3); // Level
            output[3] = m.group(4); // Machine
            output[4] = m.group(5); // Thread
            output[5] = m.group(6); // Class
            output[6] = m.group(7); // Method
            output[7] = m.group(8); // Message
            String arguments = StringEscapeUtils.unescapeCsv(m.group(9));
            output[8] = arguments.replace("\\n", "\n");  // Arguments
        }

        @Override
        public ITable load() {
            this.columns = schema.createAppendableColumns();
            try (BufferedReader reader = new BufferedReader(
                    new FileReader(this.filename))) {
                String[] fields = new String[this.columns.length];
                while (true) {
                    String line = reader.readLine();
                    if (line == null)
                        break;
                    this.parse(line, fields);
                    this.append(fields);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.close(null);
            return new Table(this.columns);
        }
    }

    public static ITable parseLogFile(String file) {
        LogFileLoader reader = new LogFileLoader(file);
        return reader.load();
    }
}
