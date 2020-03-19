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

package org.hillview.table.columns;

import org.hillview.table.ColumnDescription;
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IMutableColumn;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for all columns.
 */
public abstract class BaseColumn implements IColumn {
    static final long serialVersionUID = 1;

    final ColumnDescription description;
    int parsingExceptionCount;
    private static final AtomicInteger uniqueId = new AtomicInteger(0);
    private final int id;

    void checkKind(ContentsKind kind) {
        if (this.description.kind != kind)
            throw new RuntimeException("Expected " + kind + " but have " + this.getDescription().kind);
    }

    BaseColumn(final ColumnDescription description) {
        this.description = description;
        this.parsingExceptionCount = 0;
        this.id = uniqueId.getAndIncrement();
    }

    @Override
    public ColumnDescription getDescription() {
        return this.description;
    }

    @Override
    public double getDouble(final int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Instant getDate(final int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(final int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Duration getDuration(final int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(final int rowIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isMissing(final int rowIndex) { throw new UnsupportedOperationException(); }

    public int getParsingExceptionCount() { return this.parsingExceptionCount; }

    @Override
    public String toString() {
        return this.description.toString() + "[id=" + id + "]";
    }

    /**
     * Create an empty column with the specified description.
     * @param maxSize     Column size.
     * @param description Column description.
     * @param usedSize    Number of rows used in column.
     */
    public static IMutableColumn create(
            ColumnDescription description, int maxSize, int usedSize) {
        if (usedSize > maxSize)
            throw new IllegalArgumentException("maxSize " + maxSize + " < usedsize " + usedSize);
        if (usedSize == 0 || (maxSize / usedSize > 4))
            return new SparseColumn(description, maxSize);
        else
            return BaseArrayColumn.create(description, maxSize);
    }
}
