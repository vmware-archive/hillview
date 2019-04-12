/*
 * Copyright (c) 2019 VMware Inc. All Rights Reserved.
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
import org.hillview.table.Table;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.ITable;
import org.hillview.table.columns.ConstantStringColumn;
import org.hillview.table.columns.IntListColumn;
import org.hillview.table.columns.StringListColumn;
import org.hillview.utils.Utilities;

/**
 * Base class used for loading various log files.
 */
public class LogFiles {
    /**
     * This column name must appear in all log formats.
     */
    public static final String timestampColumnName = "Timestamp";
    /**
     * Column name where the lines that are not parsed correctly are stored.
     */
    public static final String parseErrorColumn = "ParsingErrors";
    public static final String hostColumn = "Host";
    public static final String directoryColumn = "Directory";
    public static final String filenameColumn = "Filename";
    public static final String lineNumberColumn = "Line";

    static abstract class BaseLogLoader extends TextFileLoader {
        /**
         * Column storing the line number for each log line.
         */
        final IntListColumn lineNumber;
        /**
         * Column storing lines that failed parsing.
         */
        final StringListColumn parsingErrors;

        BaseLogLoader(String path) {
            super(path);
            this.parsingErrors = new StringListColumn(
                    new ColumnDescription(parseErrorColumn, ContentsKind.String));
            this.lineNumber = new IntListColumn(
                    new ColumnDescription(lineNumberColumn, ContentsKind.Integer));
        }

        /**
         * Creates a table from the list of columns by appending some special columns.
         */
        ITable createTable() {
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
}
