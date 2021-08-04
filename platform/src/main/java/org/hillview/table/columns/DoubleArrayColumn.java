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
import org.hillview.table.api.IDoubleColumn;
import org.hillview.table.api.IMutableColumn;

/**
 * Column of doubles, implemented as an array of doubles and a BitSet of missing values.
 */
public class DoubleArrayColumn
        extends BaseArrayColumn
        implements IDoubleColumn, IMutableColumn {
    static final long serialVersionUID = 1;

    private final double[] data;

    public DoubleArrayColumn(final ColumnDescription description, final int size) {
        super(description, size);
        this.data = new double[size];
    }

    public DoubleArrayColumn(final ColumnDescription description,
                             final double[] data) {
        super(description, data.length);
        this.data = data;
    }

    @Override
    public IColumn seal() {
        return this;
    }

    @Override
    public int sizeInRows() { return this.data.length;}

    @Override
    public IColumn rename(String newName) {
        return new DoubleArrayColumn(this.description.rename(newName), this.data);
    }

    @Override
    public double getDouble(final int rowIndex) { return this.data[rowIndex];}

    @Override
    public void set(final int rowIndex, final double value)
    { this.data[rowIndex] = value; }
}
