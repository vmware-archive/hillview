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

package org.hillview.table.columns;

import net.openhft.hashing.LongHashFunction;
import org.hillview.table.ColumnDescription;
import org.hillview.table.api.*;
import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.time.Duration;

/**
 * A column of time durations that can grow in size.
 */
@SuppressWarnings("EmptyMethod")
class DurationListColumn extends DoubleListColumn implements IDurationColumn {
    public DurationListColumn(final ColumnDescription desc) {
        super(desc);
        this.checkKind(ContentsKind.Duration);
    }

    @Nullable
    @Override
    public Duration getDuration(final int rowIndex) {
        double d = this.getDouble(rowIndex);
        return Converters.toDuration(d);
    }

    @Override
    @SuppressWarnings("Duplicates")
    public void append(@Nullable final Duration value) {
        if (value == null) {
            this.appendMissing();
        } else {
            double d = Converters.toDouble(value);
            this.append(d);
        }
    }

   @Override
    public void parseAndAppendString(@Nullable String s) {
        if ((s == null) || s.isEmpty())
            this.parseEmptyOrNull();
        else
            this.append(Duration.parse(s));
    }

    @Override
    public double asDouble(int rowIndex, @Nullable IStringConverter unused) {
        return this.getDouble(rowIndex);
    }

    @Nullable
    @Override
    public String asString(int rowIndex) {
        Duration dt = this.getDuration(rowIndex);
        if (dt == null)
            return null;
        return dt.toString();
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
    public IColumn convertKind(ContentsKind kind, String newColName, IMembershipSet set) {
        return IDurationColumn.super.convertKind(kind, newColName, set);
    }
}