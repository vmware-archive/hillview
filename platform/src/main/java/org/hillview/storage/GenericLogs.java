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

import org.apache.commons.lang.StringEscapeUtils;
import org.hillview.table.ColumnDescription;
import org.hillview.table.Schema;
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.ITable;
import org.hillview.storage.FileSetDescription;
import org.hillview.utils.Utilities;

import java.util.Map;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;

import io.krakens.grok.api.*;

/**
 * Reads Generic logs into ITable objects.
 */
public class GenericLogs {
    private static String hostName;
    private Schema schema;
    private Grok grok;
    private String logFormat;

    GenericLogs(String logFormat) {
        this.logFormat = logFormat;
        GrokCompiler grokCompiler = GrokCompiler.newInstance();
        grokCompiler.registerDefaultPatterns();
        grokCompiler.registerPatternFromClasspath("/patterns/log-patterns");
        this.grok = grokCompiler.compile(this.logFormat, true);
        schema = new Schema();
    }

    static {
        GenericLogs.hostName = Utilities.getHostName();
    }

    public static class LogFileLoader extends TextFileLoader {
        private GenericLogs genLog;
        LogFileLoader(final String path, GenericLogs genLog) {
            super(path);
            this.genLog = genLog;
        }

        void parse(String line, String[] output) {
            Match gm = this.genLog.grok.match(line);
            final Map<String, Object> capture = gm.capture();
            if (capture.size() > 0) {
                output[0] = GenericLogs.hostName;
                int index = 1;
                for (Map.Entry<String,Object> entry : capture.entrySet()) {
                    output[index] = entry.getValue().toString();
                    index += 1;
		}
            }
        }

        @Override
        public ITable load() {
            try (BufferedReader reader = new BufferedReader(
                    this.getFileReader())) {
                if (new File(this.filename).length() == 0)
                    this.error("File " + this.filename + " is empty!");
                if (this.genLog.schema.getColumnCount() == 0) {
                    this.genLog.schema.append(new ColumnDescription("Host", ContentsKind.String));
                    String line = reader.readLine();
                    Match gmatch = this.genLog.grok.match(line);
                    final Map<String, Object> capture = gmatch.capture();
                    for (Map.Entry<String,Object> entry : capture.entrySet()) {
                        this.genLog.schema.append(new ColumnDescription(entry.getKey(), ContentsKind.String));
                    }
                    this.columns = this.genLog.schema.createAppendableColumns();
                }
                String[] fields = new String[this.columns.length];
                while (true) {
                    String line = reader.readLine();
                    if (line == null)
                        break;
                    if (line.trim().isEmpty())
                        continue;
                    this.parse(line, fields);
                    this.append(fields);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.close(null);
            return new Table(this.columns, this.filename, null);
        }
    }

    public static ITable parseLogFile(String file, GenericLogs genLog) {
        LogFileLoader reader = new LogFileLoader(file, genLog);
        return reader.load();
    }
}
