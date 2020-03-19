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

import net.openhft.hashing.LongHashFunction;
import org.hillview.table.ColumnDescription;
import org.hillview.table.api.*;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.time.Duration;

/*
 * Column of durations with known size.
 * Durations are actually stored as doubles.
 */
@SuppressWarnings("EmptyMethod")
public final class DurationArrayColumn extends DoubleArrayColumn
        implements IDurationColumn {
    static final long serialVersionUID = 1;

    public DurationArrayColumn(final ColumnDescription description, final int size) {
        super(description, size);
        this.checkKind(ContentsKind.Duration);
    }

    public DurationArrayColumn(final ColumnDescription description,
                               final Duration[] data) {
        super(description, data.length);
        this.checkKind(ContentsKind.Duration);
    }

    @Override
    public Duration getDuration(final int rowIndex) {
        return Converters.toDuration(this.getDouble(rowIndex));
    }

    @Override
    public void set(int rowIndex, @Nullable Object value) {
        this.set(rowIndex, (Duration)value);
    }

    public void set(final int rowIndex, @Nullable final Duration value) {
        if (value == null)
            this.setMissing(rowIndex);
        else
            this.set(rowIndex, Converters.toDouble(value));
    }

    @Override
    public double asDouble(int rowIndex) {
        return super.getDouble(rowIndex);
    }

    @Override
    @Nullable
    public String asString(int rowIndex) {
        if (this.isMissing(rowIndex))
            return null;
        //noinspection ConstantConditions
        return this.getDuration(rowIndex).toString();
    }

    @Override
    public IndexComparator getComparator() {
        return super.getComparator();
    }

    @Override
    public long hashCode64(int rowIndex, LongHashFunction hash) {
        return super.hashCode64(rowIndex, hash);
    }

    @Override
    public IColumn convertKind(
            ContentsKind kind, String newColName, IMembershipSet set) {
        return IDurationColumn.super.convertKind(kind, newColName, set);
    }
}
