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
import org.hillview.table.api.IDurationColumn;
import org.hillview.table.api.IMutableColumn;

import javax.annotation.Nullable;
import java.time.Duration;

/*
 * Column of durations, implemented as an array of Durations and a BitSet of missing values
 */

public final class DurationArrayColumn extends BaseArrayColumn
        implements IDurationColumn, IMutableColumn {
    private final Duration[] data;

    public DurationArrayColumn(final ColumnDescription description, final int size) {
        super(description, size);
        this.checkKind(ContentsKind.Duration);
        this.data = new Duration[size];
    }

    public DurationArrayColumn(final ColumnDescription description,
                               final Duration[] data) {
        super(description, data.length);
        this.checkKind(ContentsKind.Duration);
        this.data = data;
    }

    @Override
    public int sizeInRows() {
        return this.data.length;
    }

    @Nullable
    @Override
    public Duration getDuration(final int rowIndex) {
        return this.data[rowIndex];
    }

    public void set(final int rowIndex, @Nullable final Duration value) {
        this.data[rowIndex] = value;
    }

    public void set(final int rowIndex, @Nullable final Object value) {
        this.set(rowIndex, (Duration)value);
    }

    @Override
    public boolean isMissing(final int rowIndex) {return this.getDuration(rowIndex) == null; }

    @Override
    public void setMissing(final int rowIndex) { this.set(rowIndex, (Duration)null);}
}
