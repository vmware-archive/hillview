/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
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

import org.hillview.table.LazySchema;
import org.hillview.dataset.api.IJson;
import org.hillview.table.Schema;
import org.hillview.table.api.ITable;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.io.File;
import java.nio.file.Paths;
import java.time.LocalDateTime;

/**
 * Describes a set of files to load.  Not all fields are always used.
 * This class has no constructor because it is created directly by
 * (de)serialization from JSON.
 */
public class FileSetDescription implements IJson {
    static final long serialVersionUID = 1;
    /**
     * This could probably be an enum.  Supported values are:
     * csv, orc, parquet, json, hillviewlog.
     */
    public String fileKind = "";
    /**
     * A simple shell pattern matching file names.
     * Note: this is not a regular expression.
     */
    public String fileNamePattern = "*";
    /**
     * Name of schema file in folder.  If null or empty no schema file is assumed.
     */
    @Nullable
    public String schemaFile;
    /**
     * Schema given explicitly.
     */
    @Nullable
    public Schema schema;
    /**
     * If true the files are expected to have a header row.
     */
    public boolean headerRow = true;
    /**
     * Used to circumvent caching.
     */
    @Nullable
    public String cookie = null;
    /**
     * Used for testing: allows reading the same data multiple times.
     */
    public int repeat = 1;
    /**
     * This is a format in the "grok" pattern language.
     */
    @Nullable
    public String logFormat = null;
    @Nullable
    public Double startTime;
    @Nullable
    public Double endTime;
    /**
     * If true the file is deleted after loading the data.  This is
     * useful for temporary files.
     */
    public boolean deleteAfterLoading;

    @SuppressWarnings("unused")
    public String getBasename() {
        return Utilities.getFolder(this.fileNamePattern);
    }

   public LazySchema getSchema() {
        if (this.schema != null)
            return new LazySchema(this.schema);
        if (Utilities.isNullOrEmpty(this.schemaFile))
            return new LazySchema((String)null);
        return new LazySchema(Paths.get(Utilities.getFolder(this.fileNamePattern), this.schemaFile).toString());
    }

    @Nullable
    public String getLogFormat() {
        return this.logFormat;
    }

    public IFileReference createFileReference(String pathname) {
        return new FileReference(pathname);
    }

    class FileReference implements IFileReference {
        private final String pathname;

        FileReference(final String pathname) {
            this.pathname = pathname;
        }

        @Override
        public ITable load() {
            TextFileLoader loader;
            switch (FileSetDescription.this.fileKind) {
                case "csv":
                    CsvFileLoader.Config config = new CsvFileLoader.Config();
                    config.allowFewerColumns = true;
                    config.hasHeaderRow = FileSetDescription.this.headerRow;
                    loader = new CsvFileLoader(
                            this.pathname, config, FileSetDescription.this.getSchema());
                    break;
                case "orc":
                    loader = new OrcFileLoader(
                            this.pathname, FileSetDescription.this.getSchema(), true);
                    break;
                case "parquet":
                    loader = new ParquetFileLoader(
                            this.pathname, true);
                    break;
                case "json":
                    loader = new JsonFileLoader(
                            this.pathname, FileSetDescription.this.getSchema());
                    break;
                case "hillviewlog":
                    loader = new HillviewLogs.LogFileLoader(this.pathname);
                    break;
                case "genericlog":
                    String format = FileSetDescription.this.getLogFormat();
                    assert format != null;
                    GrokLogs genLog = new GrokLogs(format);
                    //noinspection ConstantConditions
                    LocalDateTime start = null;
                    LocalDateTime end = null;
                    if (FileSetDescription.this.startTime != null)
                        start = Converters.toLocalDate(FileSetDescription.this.startTime);
                    if (FileSetDescription.this.endTime != null)
                        end = Converters.toLocalDate(FileSetDescription.this.endTime);
                    loader = genLog.getFileLoader(this.pathname, start, end);
                    break;
                default:
                    throw new RuntimeException(
                            "Unexpected file kind " + FileSetDescription.this.fileKind);
            }
            ITable result = Converters.checkNull(loader.load());
            if (FileSetDescription.this.deleteAfterLoading) {
                File file = new File(this.pathname);
                boolean success = file.delete();
                if (!success)
                    HillviewLogger.instance.error("Error deleting file", "{0}", this.pathname);
                else
                    HillviewLogger.instance.info("Deleted file", "{0}", this.pathname);
            }
            return result;
        }

        public long getSizeInBytes() {
            File file = new File(this.pathname);
            if (file.exists())
                return file.length();
            return 0;
        }
    }
}
