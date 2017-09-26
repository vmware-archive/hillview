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

import org.hillview.table.ColumnDescription;
import org.hillview.table.columns.StringListColumn;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Reads a newline-separated text file into a string column.
 */
class TextFileReader {
    private final Path file;
    TextFileReader(final Path filename) {
        this.file = filename;
    }

    IColumn readFile(final String columnName) throws IOException {
        final ColumnDescription desc = new ColumnDescription(columnName, ContentsKind.String, false);
        final StringListColumn result = new StringListColumn(desc);
        Files.lines(this.file).forEach(result::append);
        return result;
    }
}
