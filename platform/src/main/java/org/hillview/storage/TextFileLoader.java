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

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.hillview.table.api.IAppendableColumn;
import org.hillview.table.api.ITable;
import org.hillview.utils.HillviewLogger;
import org.hillview.utils.Utilities;


import javax.annotation.Nullable;
import java.io.*;

/**
 * Abstract class for a reader that reads data from a text file and keeps
 * track of the current position within the file.  These loaders are
 * only allocated where the data is, they are not serializable.
 */
public abstract class TextFileLoader {
    final String filename;
    int currentRow;
    int currentColumn;
    @Nullable
    IAppendableColumn[] columns;
    private long currentField;
    @Nullable
    private String currentToken;
    boolean allowFewerColumns;
    boolean traceProgress = false;  // for debugging

    // Some of these may be null
    @Nullable
    private InputStream inputStream = null;
    @Nullable
    private InputStream bufferedInputStream = null;
    @Nullable
    private InputStream compressedStream = null;
    @Nullable
    private BOMInputStream bomStream = null;

    TextFileLoader(String path) {
        this.filename = path;
        this.currentRow = 0;
        this.currentColumn = 0;
        this.currentField = 0;
        this.currentToken = null;
    }

    Reader getFileReader() {
        try {
            HillviewLogger.instance.info("Reading file", "{0}", this.filename);
            this.inputStream = new FileInputStream(this.filename);
            this.bufferedInputStream = new BufferedInputStream(inputStream);
            // The buffered input stream is needed by the CompressorStream
            // to detect the compression method at runtime.
            InputStream fis = this.bufferedInputStream;

            String suffix = Utilities.isCompressed(this.filename);
            if (suffix != null) {
                if (suffix.equals("zip")) {
                    // TODO: For zip files we expect a single file in archive
                    ArchiveInputStream is = new ArchiveStreamFactory().
                            createArchiveInputStream(ArchiveStreamFactory.ZIP, fis);
                    ArchiveEntry entry = is.getNextEntry();
                    if (entry == null)
                        throw new RuntimeException("No files in zip archive");
                    ZipArchiveEntry ze = (ZipArchiveEntry) entry;
                    if (ze.isDirectory())
                        throw new RuntimeException("zip archive contains a directory");
                    fis = is;
                } else {
                    this.compressedStream = new CompressorStreamFactory()
                            .createCompressorInputStream(fis);
                    fis = this.compressedStream;
                }
            }
            this.bomStream = new BOMInputStream(fis,
                    ByteOrderMark.UTF_8,
                    ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE,
                    ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE);
            ByteOrderMark bom = this.bomStream.getBOM();
            String charsetName = bom == null ? "UTF-8" : bom.getCharsetName();
            return new InputStreamReader(this.bomStream, charsetName);
        } catch (IOException | CompressorException | ArchiveException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Relinquishes all resources used.
     *
     * @param reader Reader that was created by getFileReader, or null.
     */
    void close(@Nullable Reader reader) {
        try {
            if (reader != null)
                reader.close();
            if (this.bomStream != null)
                this.bomStream.close();
            if (this.compressedStream != null)
                this.compressedStream.close();
            if (this.bufferedInputStream != null)
                this.bufferedInputStream.close();
            if (this.inputStream != null)
                this.inputStream.close();
        } catch (IOException e) {
            HillviewLogger.instance.error("Error closing input streams", e);
        }
    }

    void append(String[] data) {
        try {
            assert this.columns != null;
            int columnCount = this.columns.length;
            this.currentColumn = 0;
            if (data.length > columnCount)
                this.error("Too many columns " + data.length + " vs " + columnCount);
            for (this.currentColumn = 0; this.currentColumn < data.length; this.currentColumn++) {
                this.currentToken = data[this.currentColumn];
                this.columns[this.currentColumn].parseAndAppendString(this.currentToken);
                this.currentField++;
                if (this.traceProgress && (this.currentField % 100000) == 0) {
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
            this.currentToken = null;
        } catch (Exception ex) {
            this.error(ex);
        }
    }

    private String errorMessage() {
        String columnName = "";
        if (this.columns != null) {
            columnName = (this.currentColumn < this.columns.length) ?
                    (" (" + this.columns[this.currentColumn].toString() + ")") : "";
        }

        // rows are numbered from 0, lines from 1
        return "Error while parsing file " + this.filename + "@" + Utilities.getHostName() +
                " line " + (this.currentRow + 1) + (this.currentColumn >= 0 ?
                " column " + this.currentColumn + columnName : "")
                + (this.currentToken != null ? " token " + this.currentToken : "");
    }

    void error(String message) {
        if (message.length() > 2048) {
            int lastIndex = message.length() - 48;
            message = message.substring(0, 2000) + "..." + message.substring(lastIndex);
        }
        throw new RuntimeException(this.errorMessage() + ": " + message);
    }

    private void error(Exception ex) {
        throw new RuntimeException(this.errorMessage(), ex);
    }

    @Nullable
    public ITable load() {
        this.prepareLoading();
        ITable result = this.loadFragment(-1);
        this.endLoading();
        return result;
    }

    /**
     * Prepare to load the data.
     */
    public void prepareLoading() {
        throw new UnsupportedOperationException();
    }

    /**
     * Load this many rows from the file.
     *
     * @param maxRows Maximum number of rows to read.  If -1 then there is no limit.
     * @return The loaded rows as a table.  Returns an empty table when there is no more data.
     */
    public ITable loadFragment(int maxRows) {
        throw new UnsupportedOperationException();
    }

    /**
     * We are finished loading.
     */
    public void endLoading() {
        throw new UnsupportedOperationException();
    }
}