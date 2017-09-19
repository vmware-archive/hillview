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
import org.hillview.table.api.IDateColumn;
import org.hillview.utils.DateParsing;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.util.ArrayList;

/**
 * A column of Dates that can grow in size.
 */
public class DateListColumn
        extends BaseListColumn
        implements IDateColumn {
    private final ArrayList<LocalDateTime[]> segments;

    @Nullable
    DateParsing dateParser;

    public DateListColumn(final ColumnDescription desc) {
        super(desc);
        this.checkKind(ContentsKind.Date);
        this.segments = new ArrayList<LocalDateTime[]>();
        this.dateParser = null;
    }

    @Nullable
    @Override
    public LocalDateTime getDate(final int rowIndex) {
        final int segmentId = rowIndex >> this.LogSegmentSize;
        final int localIndex = rowIndex & this.SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    @Override
    void grow() {
        this.segments.add(new LocalDateTime[this.SegmentSize]);
        this.growMissing();
    }

    @Override
    @SuppressWarnings("Duplicates")
    public void append(@Nullable final LocalDateTime value) {
        final int segmentId = this.size >> this.LogSegmentSize;
        final int localIndex = this.size & this.SegmentMask;
        if (this.segments.size() <= segmentId)
            this.grow();
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        return this.getDate(rowIndex) == null;
    }

    @Override
    public void appendMissing() {
        this.append((LocalDateTime)null);
    }

    @Override
    public void parseAndAppendString(@Nullable String s) {
        if ((s == null) || s.isEmpty())
            this.parseEmptyOrNull();
        else {
            if (this.dateParser == null) {
                this.dateParser = new DateParsing(s);
            }
            LocalDateTime dt = this.dateParser.parse(s);
            this.append(dt);
        }
    }
}

