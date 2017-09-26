/*
 * Copyright (c) 2017 VMWare Inc. All Rights Reserved.
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

package org.hillview.table.api;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Convenience class packing a column and its associated string converter.
 */
public class ColumnAndConverter {
    public final IColumn column;
    @Nullable
    public final IStringConverter converter;

    public ColumnAndConverter(IColumn column, @Nullable IStringConverter converter) {
        this.column = column;
        this.converter = converter;
    }

    public ColumnAndConverter(IColumn column) {
        this(column, null);
    }

    public @Nullable Object getObject(int rowIndex) {
        return this.column.getObject(rowIndex);
    }

    public @Nullable String getString(int rowIndex) {
        return this.column.getString(rowIndex);
    }

    public double getDouble(int rowIndex) {
        return this.column.getDouble(rowIndex);
    }

    public int getInt(int rowIndex) {
        return this.column.getInt(rowIndex);
    }

    public @Nullable LocalDateTime getDate(int rowIndex) {
        return this.column.getDate(rowIndex);
    }

    public @Nullable Duration getDuration(int rowIndex) {
        return this.column.getDuration(rowIndex);
    }

    public boolean isMissing(int rowIndex) {
        return this.column.isMissing(rowIndex);
    }

    public double asDouble(int rowIndex) {
        return this.column.asDouble(rowIndex, this.converter);
    }
}
