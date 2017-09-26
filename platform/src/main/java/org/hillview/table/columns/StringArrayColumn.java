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
import org.hillview.table.api.*;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;

/**
 * Column of Strings, implemented as an array of strings and a bit vector of missing values.
 * Allows ContentsKind String or Json
 */
public final class StringArrayColumn
        extends BaseArrayColumn implements IStringColumn, IMutableColumn {
    private final String[] data;

    private void validate() {
        if ((this.description.kind != ContentsKind.String) &&
                (this.description.kind != ContentsKind.Json) &&
                (this.description.kind != ContentsKind.Category))
            throw new InvalidParameterException("Kind should be String or Json "
                    + this.description.kind);
    }

    public StringArrayColumn(final ColumnDescription description, final int size) {
        super(description, size);
        this.validate();
        this.data = new String[size];
    }

    public StringArrayColumn(final ColumnDescription description,
                             final String[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    @Override
    public int sizeInRows() {
        return this.data.length;
    }

    @Nullable
    @Override
    public String getString(final int rowIndex) {
        return this.data[rowIndex];
    }

    @Override
    public IColumn seal() {
        if (this.missing != null && this.missing.size() != this.data.length)
            throw new RuntimeException("Missing size does not match column data: " +
                    this.missing.size() + " vs. " + this.data.length);
        return this;
    }

    @Override
    public void set(int rowIndex, @Nullable Object value) {
        this.set(rowIndex, (String)value);
    }

    public void set(final int rowIndex, @Nullable final String value) {
        this.data[rowIndex] = value;
    }

    @Override
    public boolean isMissing(final int rowIndex){ return this.getString(rowIndex) == null;}

    @Override
    public void setMissing(final int rowIndex) { this.set(rowIndex, (String)null);}

    @Override
    public IColumn convertKind(ContentsKind kind, String newColName, IMembershipSet set) {
        IColumn newColumn;
        switch(kind) {
            case Category:
                ColumnDescription cd = new ColumnDescription(newColName, ContentsKind.Category, this.description.allowMissing);
                newColumn = new CategoryArrayColumn(cd, this.data);
                break;
            case Json:
            case String:
            case Integer:
            case Double:
            case Date:
            case Duration:
                throw new UnsupportedOperationException("Conversion from " + this.description.kind.toString() + " to " +
                        "" + kind.toString() + " is not supported.");
            default:
                throw new RuntimeException("Unexpected column kind " + description.toString());
        }
        return newColumn;
    }
}
