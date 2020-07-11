/*
 * Copyright (c) 2020 VMware Inc. All Rights Reserved.
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

public class IntervalColumn extends BaseColumn
        implements IIntervalColumn {
    static final long serialVersionUID = 1;
    private final IDoubleColumn start;
    private final IDoubleColumn end;

    // At this point we only allow creation of interval columns from
    // two existing double columns of the same size.
    public IntervalColumn(ColumnDescription description, IDoubleColumn start, IDoubleColumn end) {
        super(description);
        if (description.kind != ContentsKind.Interval)
            throw new RuntimeException("Expected an Interval kind, not " + description.kind);
        this.start = start;
        this.end = end;
        if (start.sizeInRows() != end.sizeInRows())
            throw new RuntimeException("Incompatible column sizes: " +
                    this.start.sizeInRows() + " and " + this.end.sizeInRows());
    }

    @Override
    public ColumnDescription getDescription() {
        return this.description;
    }

    @Override
    public boolean isLoaded() {
        return this.start.isLoaded() && this.end.isLoaded();
    }

    @Override
    public double getDouble(final int rowIndex) {
        return this.start.getDouble(rowIndex);
    }

    @Override
    public int sizeInRows() {
        return this.start.sizeInRows();
    }

    @Override
    public double asDouble(int rowIndex) {
        return this.start.asDouble(rowIndex);
    }

    @Override
    public boolean isMissing(int rowIndex) {
        return this.start.isMissing(rowIndex) || this.end.isMissing(rowIndex);
    }

    @Nullable
    @Override
    public String asString(int rowIndex) {
        return Interval.toString(this.start.asDouble(rowIndex), this.end.asDouble(rowIndex));
    }

    @Override
    public IColumn rename(String newName) {
        return new IntervalColumn(description.rename(newName), this.start, this.end);
    }

    @Override
    public Double getValue(int rowIndex, boolean start) {
        if (start)
            return this.start.asDouble(rowIndex);
        else
            return this.end.asDouble(rowIndex);
    }

    @Override
    public Interval getInterval(int rowIndex) {
        return new Interval(this.getValue(rowIndex, true), this.getValue(rowIndex, false));
    }
}
