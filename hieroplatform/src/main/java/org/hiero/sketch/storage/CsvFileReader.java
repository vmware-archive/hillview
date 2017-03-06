/*
 * Copyright (c) 2017 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0 2
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

package org.hiero.sketch.storage;

import org.hiero.sketch.table.Schema;
import org.hiero.sketch.table.Table;

import java.nio.file.Path;

/**
 * Knows how to read a CSV file (comma-separated file).
 */
class CsvFileReader {
    private final Path filename;
    private final Schema schema;
    private final int columnCount;
    char separator = ',';
    private final boolean allowFewerColumns;

    /**
     * Prepare for reading a CSV file with an unknown number of
     * columns and where all columns are strings.
     * @param path File to read
     */
    public CsvFileReader(final Path path) {
        this.filename = path;
        this.schema = null;
        this.columnCount = 0;
        this.allowFewerColumns = true;
    }

    /**
     * Prepare for reading a CSV file with a known number of columns
     * and where all columns are strings.
     * - If a line has fewer columns than specified
     * and allowFewer is set the data is considered missing.
     * - If a line has fewer columns than specified and
     * allowFewer is not set a runtime exception is thrown.
     * If a line has more columns than specified
     * a runtime exception is thrown.
     * @param columnCount Number of columns to read.
     * @param path File to read.
     * @param allowFewer If true allow rows with fewer columns.
     */
    public CsvFileReader(final Path path,
                         final int columnCount,
                         final boolean allowFewer) {
        this.filename = path;
        this.columnCount = columnCount;
        this.schema = new Schema();
        this.allowFewerColumns = allowFewer;
    }

    /**
     * Prepare for reading a CSV file with a known schema.
     * @param path    File to read.
     * @param schema  Schema of the file.  Data will be converted as described by this schema.
     * @param allowFewer If true we allow rows with fewer columns.
     */
    public CsvFileReader(final Path path, final Schema schema,
                         final boolean allowFewer) {
        this.filename = path;
        this.columnCount = schema.getColumnCount();
        this.schema = schema;
        this.allowFewerColumns = allowFewer;
    }

    Table read() {
        // TODO
        return null;
    }
}
