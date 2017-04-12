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

package org.hiero.table;

import org.hiero.table.api.ContentsKind;
import org.hiero.table.api.IColumn;
import org.hiero.table.api.IStringColumn;
import org.hiero.table.api.IStringConverter;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;

/**
 * Column of Strings, implemented as an array of strings and a bit vector of missing values.
 * Allows ContentsKind String or Json
 */
public final class StringArrayColumn
        extends BaseArrayColumn implements IStringColumn {
    private final String[] data;

    private void validate() {
        if ((this.description.kind != ContentsKind.String) &&
                (this.description.kind != ContentsKind.Json) &&
                (this.description.kind != ContentsKind.Category))
            throw new InvalidParameterException("Kind should be String or Json "
                    + this.description.kind);
    }

    protected StringArrayColumn(final StringArrayColumn other, @Nullable IStringConverter converter) {
        super(other, converter);
        this.data = other.data;
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
    public IColumn setDefaultConverter(@Nullable IStringConverter converter) {
        return new StringArrayColumn(this, converter);
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

    public void set(final int rowIndex, @Nullable final String value) {
        this.data[rowIndex] = value;
    }

    @Override
    public boolean isMissing(final int rowIndex){ return this.getString(rowIndex) == null;}

    @Override
    public void setMissing(final int rowIndex) { this.set(rowIndex, null);}
}
