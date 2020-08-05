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

public class StringArrayColumn extends BaseArrayColumn
        implements IStringColumn, IMutableColumn {
    static final long serialVersionUID = 1;

    private final int[] data;
    private final CategoryEncoding encoding;

    public StringArrayColumn(ColumnDescription description, final int size) {
        super(description, size);
        if (!description.kind.isString())
            throw new RuntimeException("Unexpected kind for string column: " + description.kind);
        this.encoding = new CategoryEncoding();
        this.data = new int[size];
    }

    public StringArrayColumn(ColumnDescription description, String[] values) {
        super(description, values.length);
        if (!description.kind.isString())
            throw new RuntimeException("Unexpected kind for string column: " + description.kind);
        this.encoding = new CategoryEncoding();
        this.data = new int[values.length];

        int i = 0;
        for (String value : values) {
            this.set(i, value);
            i++;
        }
    }

    private StringArrayColumn(ColumnDescription description,
                              int[] data, CategoryEncoding encoding) {
        super(description, data.length);
        this.data = data;
        this.encoding = encoding;
    }

    @Override
    public IColumn seal() { return this; }

    @Override
    public boolean isMissing(final int rowIndex) {
        return this.getString(rowIndex) == null;
    }

    public void setMissing(int rowIndex) {
        this.set(rowIndex, null);
    }

    @Override
    public void set(int rowIndex, @Nullable String value) {
        this.data[rowIndex] = this.encoding.encode(value);
    }

    @Nullable
    @Override
    public Object getObject(int rowIndex) {
        return this.getString(rowIndex);
    }

    @Override
    public String getString(int rowIndex) {
        return this.encoding.decode(this.data[rowIndex]);
    }

    @Override
    public int sizeInRows() {
        return this.data.length;
    }

    @Override
    public IColumn rename(String newName) {
        return new StringArrayColumn(
                this.description.rename(newName),
                this.data, this.encoding);
    }
}
