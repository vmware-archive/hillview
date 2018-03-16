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

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * This class knows how to create a file loader given a path to a file name.
 * This is a base class with several implementations in this file.
 */
public abstract class FileLoaderDescription implements Serializable {
    public abstract IFileLoader createLoader(String path);

    public static class LogFile extends FileLoaderDescription {
        public IFileLoader createLoader(String path) {
            return new HillviewLogs.LogFileLoader(path);
        }
    }

    /**
     * A loader description that knows how to create a class
     * that can load a CSV (comma-separated value) file.
     */
    public static class CsvFile extends FileLoaderDescription {
        @Nullable
        private final String schemaPath;
        private final CsvFileLoader.CsvConfiguration config;

        public CsvFile(@Nullable String schemaPath,
                       CsvFileLoader.CsvConfiguration config) {
            this.schemaPath = schemaPath;
            this.config = config;
        }

        public IFileLoader createLoader(String path) {
            return new CsvFileLoader(path, this.config, this.schemaPath);
        }
    }

    /**
     * A loader description that knows how to create a loader that
     * can read a JSON file.
     */
    public static class JsonFile extends FileLoaderDescription {
        @Nullable
        private final String schemaPath;

        public JsonFile(@Nullable String schemaPath) {
            this.schemaPath = schemaPath;
        }

        public IFileLoader createLoader(String path) {
            return new JsonFileLoader(path, this.schemaPath);
        }
    }

    /**
     * A loader description that knows how to create a loader that can
     * read a Parquet file.
     */
    public static class ParquetFile extends FileLoaderDescription {
        private final boolean lazy;

        public ParquetFile(boolean lazy) {
            this.lazy = lazy;
        }

        public IFileLoader createLoader(String path) {
            return new ParquetFileLoader(path, this.lazy);
        }
    }
}
