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

package org.hillview.table.columns;

import net.openhft.hashing.LongHashFunction;
import org.hillview.table.ColumnDescription;
import org.hillview.table.api.*;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The contents of a lazy column is loaded lazily.
 */
public class LazyColumn extends BaseColumn {
    @Nullable
    private IColumn data;
    private final IColumnLoader loader;
    private final int size;

    public LazyColumn(final ColumnDescription description, int size, IColumnLoader loader) {
        super(description);
        this.data = null;
        this.loader = loader;
        this.size = size;
    }

    @Override
    public boolean isLoaded() {
        return this.data != null;
    }

    @Override
    public int sizeInRows() {
        return this.size;
    }

    public double getDouble(final int rowIndex) {
        return this.ensureLoaded().getDouble(rowIndex);
    }

    @Override
    public Instant getDate(final int rowIndex) {
        return this.ensureLoaded().getDate(rowIndex);
    }

    @Override
    public int getInt(final int rowIndex) {
        return this.ensureLoaded().getInt(rowIndex);
    }

    @Override
    public Duration getDuration(final int rowIndex) {
        return this.ensureLoaded().getDuration(rowIndex);
    }

    @Override
    public String getString(final int rowIndex) {
        return this.ensureLoaded().getString(rowIndex);
    }

    @Override
    public boolean isMissing(final int rowIndex) { return this.ensureLoaded().isMissing(rowIndex); }


    @Override
    public double asDouble(int rowIndex, IStringConverter converter) {
        return this.ensureLoaded().asDouble(rowIndex, converter);
    }

    @Nullable
    @Override
    public String asString(int rowIndex) {
        return this.ensureLoaded().asString(rowIndex);
    }

    @Override
    public IndexComparator getComparator() {
        return this.ensureLoaded().getComparator();
    }

    @Override
    public IColumn convertKind(ContentsKind kind, String newColName, IMembershipSet set) {
        return this.ensureLoaded().convertKind(kind, newColName, set);
    }

    @Override
    public long hashCode64(int rowIndex, LongHashFunction hash) {
        return this.ensureLoaded().hashCode64(rowIndex, hash);
    }

    synchronized private IColumn ensureLoaded() {
        if (this.data != null)
            return this.data;
        List<String> toLoad = new ArrayList<String>();
        toLoad.add(this.getName());
        IColumn[] loaded = this.loader.loadColumns(toLoad);
        Converters.checkNull(loaded);
        if (loaded.length != 1)
            throw new RuntimeException("Expected 1 column to be loaded, not " + loaded.length);
        this.data = loaded[0];
        return Converters.checkNull(this.data);
    }
}
