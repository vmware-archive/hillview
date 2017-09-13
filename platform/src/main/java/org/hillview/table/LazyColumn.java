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
 *
 */

package org.hillview.table;

import net.openhft.hashing.LongHashFunction;
import org.hillview.table.api.*;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * A LazyColumn is loaded lazily on demand.  The data is kept in a separate column.
 */
public class LazyColumn implements IColumn {
    @Nullable
    private IColumn actualColumn;
    private final IColumnLoader loader;

    public LazyColumn(IColumnLoader loader) {
        this.loader = loader;
    }

    private IColumn loadIfNecessary() {
        if (this.actualColumn == null)
            this.actualColumn = this.loader.load();
        return Converters.checkNull(this.actualColumn);
    }

    @Override
    public ColumnDescription getDescription() {
        return this.loadIfNecessary().getDescription();
    }

    @Nullable
    @Override
    public String getString(int rowIndex) {
        return this.loadIfNecessary().getString(rowIndex);
    }

    @Override
    public double getDouble(int rowIndex) {
        return this.loadIfNecessary().getDouble(rowIndex);
    }

    @Nullable
    @Override
    public LocalDateTime getDate(int rowIndex) {
        return this.loadIfNecessary().getDate(rowIndex);
    }

    @Override
    public int getInt(int rowIndex) {
        return this.loadIfNecessary().getInt(rowIndex);
    }

    @Nullable
    @Override
    public Duration getDuration(int rowIndex) {
        return this.loadIfNecessary().getDuration(rowIndex);
    }

    @Override
    public boolean isMissing(int rowIndex) {
        return this.loadIfNecessary().isMissing(rowIndex);
    }

    @Override
    public int sizeInRows() {
        return this.loadIfNecessary().sizeInRows();
    }

    @Override
    public double asDouble(int rowIndex, @Nullable IStringConverter converter) {
        return this.loadIfNecessary().asDouble(rowIndex, converter);
    }

    @Nullable
    @Override
    public String asString(int rowIndex) {
        return this.loadIfNecessary().asString(rowIndex);
    }

    @Override
    public IndexComparator getComparator() {
        return this.loadIfNecessary().getComparator();
    }

    @Override
    public IColumn convertKind(ContentsKind kind, String newColName, IMembershipSet set) {
        return this.loadIfNecessary().convertKind(kind, newColName, set);
    }

    @Override
    public long hashCode64(int rowIndex, LongHashFunction hash) {
        return this.loadIfNecessary().hashCode64(rowIndex, hash);
    }
}
