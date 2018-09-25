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
import org.hillview.utils.DateParsing;

import javax.annotation.Nullable;
import java.time.Instant;

/**
 * A column of Dates that can grow in size.
 */
@SuppressWarnings("EmptyMethod")
public class DateListColumn
        extends DoubleListColumn
        implements IDateColumn {
    @Nullable
    private
    DateParsing dateParser;

    public DateListColumn(final ColumnDescription desc) {
        super(desc);
        this.checkKind(ContentsKind.Date);
        this.dateParser = null;
    }

    @Nullable
    @Override
    public Instant getDate(final int rowIndex) {
        double d = this.getDouble(rowIndex);
        return Converters.toDate(d);
    }

    @Override
    @SuppressWarnings("Duplicates")
    public void append(@Nullable final Instant value) {
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
        else {
            if (this.dateParser == null) {
                this.dateParser = new DateParsing(s);
            }
            try {
                Instant dt = this.dateParser.parse(s);
                this.append(dt);
            } catch (Exception e) {
                this.parsingExceptionCount++;
                this.parseEmptyOrNull();
            }
        }
    }

    @Override
    public double asDouble(int rowIndex) {
        return this.getDouble(rowIndex);
    }

    @Nullable
    @Override
    public String asString(int rowIndex) {
        Instant dt = this.getDate(rowIndex);
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
    public IColumn convertKind(
            ContentsKind kind, String newColName, IMembershipSet set) {
        return IDateColumn.super.convertKind(kind, newColName, set);
    }
}

