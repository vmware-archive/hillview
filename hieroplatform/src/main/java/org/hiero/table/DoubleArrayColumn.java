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
import org.hiero.table.api.IDoubleColumn;

import java.security.InvalidParameterException;

/**
 * Column of doubles, implemented as an array of doubles and a BitSet of missing values.
 */
public final class DoubleArrayColumn
        extends BaseArrayColumn
        implements IDoubleColumn {
    private final double[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Double)
            throw new InvalidParameterException("Kind should be Double " + this.description.kind);
    }

    public DoubleArrayColumn(final ColumnDescription description, final int size) {
        super(description, size);
        this.validate();
        this.data = new double[size];
    }

    public DoubleArrayColumn(final ColumnDescription description,
                             final double[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    @Override
    public int sizeInRows() { return this.data.length;}

    @Override
    public double getDouble(final int rowIndex) { return this.data[rowIndex];}

    public void set(final int rowIndex, final double value) {this.data[rowIndex] = value;}
}
