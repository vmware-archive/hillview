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

import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.hillview.table.api.IAppendableColumn;
import org.hillview.utils.Converters;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;

import javax.annotation.Nullable;
import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Abstract class for a reader that reads data from a text file and keeps
 * track of the current position within the file.
 */
public abstract class TextFileLoader implements IFileLoader {
    protected final String filename;
    protected int currentRow;
    protected int currentColumn;
    @Nullable
    protected IAppendableColumn[] columns;
    private long currentField;
    @Nullable
    private String currentToken;
    protected boolean allowFewerColumns;

    public TextFileLoader(String path) {
        this.filename = path;
        this.currentRow = 0;
        this.currentColumn = 0;
        this.currentField = 0;
        this.currentToken = null;
    }

    Reader getFileReader() {
        try {
            HillviewLogger.instance.info("Reading file", "{0}", this.filename);
            InputStream fis = new FileInputStream(this.filename);
            if (this.filename.toLowerCase().endsWith(".gz"))
                fis = new GZIPInputStream(fis);
            BOMInputStream bos = new BOMInputStream(fis,
                    ByteOrderMark.UTF_8,
                    ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE,
                    ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE);
            ByteOrderMark bom = bos.getBOM();
            String charsetName = bom == null ? "UTF-8" : bom.getCharsetName();
            return new InputStreamReader(bos, charsetName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void append(String[] data) {
        try {
            Converters.checkNull(this.columns);
            int columnCount = this.columns.length;
            this.currentColumn = 0;
            if (data.length > columnCount)
                this.error("Too many columns " + data.length + " vs " + columnCount);
            for (this.currentColumn = 0; this.currentColumn < data.length; this.currentColumn++) {
                this.currentToken = data[this.currentColumn];
                this.columns[this.currentColumn].parseAndAppendString(this.currentToken);
                this.currentField++;
                if ((this.currentField % 100000) == 0) {
                    System.out.print(".");
                    System.out.flush();
                }
            }
            if (data.length < columnCount) {
                if (!this.allowFewerColumns)
                    this.error("Too few columns " + data.length + " vs " + columnCount);
                else {
                    this.currentToken = "";
                    for (int i = data.length; i < columnCount; i++)
                        this.columns[i].parseAndAppendString(this.currentToken);
                }
            }
            this.currentRow++;
        } catch (Exception ex) {
            this.error(ex);
        }
    }

    protected String errorMessage() {
        String columnName = "";
        if (this.columns != null) {
            columnName = (this.currentColumn < this.columns.length) ?
                    (" (" + this.columns[this.currentColumn].getName() + ")") : "";
        }

        return "Error while parsing file " + this.filename + "@" + Utilities.getHostName() +
                " line " + this.currentRow + (this.currentColumn > 0 ?
                " column " + this.currentColumn + columnName : "")
                + (this.currentToken != null ? " token " + this.currentToken : "");
    }

    protected void error(String message) {
        if (message.length() > 2048) {
            int lastIndex = message.length() - 48;
            message = message.substring(0, 2000) + "..." + message.substring(lastIndex);
        }
        throw new RuntimeException(this.errorMessage() + ": " + message);
    }

    protected void error(Exception ex) {
        throw new RuntimeException(this.errorMessage(), ex);
    }
}
