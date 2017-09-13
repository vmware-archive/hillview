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

import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IIntColumn;
import org.hillview.table.api.IMutableColumn;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Column of integers, implemented as an array of integers and a BitSet of missing values.
 */
public final class IntArrayColumn
        extends BaseArrayColumn
        implements IIntColumn, Serializable, IMutableColumn {
    private final int[] data;

    public IntArrayColumn() {
        super(new ColumnDescription(), 0);
        this.data = new int[0];
    }

    public IntArrayColumn(final ColumnDescription description, final int size) {
        super(description, size);
        this.checkKind(ContentsKind.Integer);
        this.data = new int[size];
    }

    public IntArrayColumn(final ColumnDescription description, final int[] data) {
        super(description, data.length);
        this.checkKind(ContentsKind.Integer);
        this.data = data;
    }

    @Override
    public int sizeInRows() {
        return this.data.length;
    }

    @Override
    public int getInt(final int rowIndex) {
        return this.data[rowIndex];
    }

    @Override
    public void set(int rowIndex, @Nullable Object value) {
        if (value == null)
            this.setMissing(rowIndex);
        else
            this.set(rowIndex, (int)value);
    }

    public void set(final int rowIndex, final int value) {
        this.data[rowIndex] = value;
    }
}
