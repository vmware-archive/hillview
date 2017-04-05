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
import org.hiero.table.api.IDateColumn;

import javax.annotation.Nullable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;

/**
 * A column of Dates that can grow in size.
 */
public class DateListColumn
        extends BaseListColumn
        implements IDateColumn {
    private final ArrayList<LocalDateTime[]> segments;
    /**
     * Used when parsing; this is set the first time when parsing a date
     * and used subsequently.
     */
    @Nullable
    private DateTimeFormatter parserFormatter;
    /**
     * Used in conjunction with the parseFormatter.  If true
     * parse the strings as LocalDate. Java is really stupid in this respect,
     * and the spec is unclear about this.
     * @see <a href="http://stackoverflow.com/questions/27454025/unable-to-obtain-localdatetime-from-temporalaccessor-when-parsing-localdatetime">Parsing LocalDateTime</a>
     */
    boolean parseAsDate;

    public DateListColumn(final ColumnDescription desc) {
        super(desc);
        if (desc.kind != ContentsKind.Date)
            throw new IllegalArgumentException("Unexpected column kind " + desc.kind);
        this.segments = new ArrayList<LocalDateTime[]>();
        this.parserFormatter = null;
    }

    @Nullable
    @Override
    public LocalDateTime getDate(final int rowIndex) {
        final int segmentId = rowIndex >> this.LogSegmentSize;
        final int localIndex = rowIndex & this.SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    private void append(@Nullable final LocalDateTime value) {
        final int segmentId = this.size >> this.LogSegmentSize;
        final int localIndex = this.size & this.SegmentMask;
        if (this.segments.size() <= segmentId) {
            this.segments.add(new LocalDateTime[this.SegmentSize]);
            this.growMissing();
        }
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        return this.getDate(rowIndex) == null;
    }

    @Override
    public void appendMissing() {
        this.append(null);
    }

    static final DateTimeFormatter[] toTry = {
            DateTimeFormatter.BASIC_ISO_DATE,
            DateTimeFormatter.ISO_LOCAL_DATE,
            DateTimeFormatter.ISO_OFFSET_DATE,

            DateTimeFormatter.ISO_DATE,
            DateTimeFormatter.ISO_LOCAL_TIME,
            DateTimeFormatter.ISO_OFFSET_TIME,
            DateTimeFormatter.ISO_TIME,
            DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            DateTimeFormatter.ISO_OFFSET_DATE_TIME,
            DateTimeFormatter.ISO_ZONED_DATE_TIME,
            DateTimeFormatter.ISO_DATE_TIME,
            DateTimeFormatter.ISO_ORDINAL_DATE,
            DateTimeFormatter.ISO_INSTANT,
            DateTimeFormatter.RFC_1123_DATE_TIME
    };

    @SuppressWarnings("UnnecessaryContinue")
    protected void guessParseFormat(String s) {
        boolean[] asDate = {false, true};

        for (boolean b : asDate) {
            this.parseAsDate = b;
            for (DateTimeFormatter d : toTry) {
                try {
                    if (b)
                        LocalDate.parse(s, d);
                    else
                        LocalDateTime.parse(s, d);
                    this.parserFormatter = d;
                    return;
                } catch (DateTimeParseException ex) {
                    continue;
                }
            }
        }
        throw new RuntimeException("Could not guess parsing format for date " + s);
    }

    @Override
    public void parseAndAppendString(String s) {
        if (s.isEmpty())
            this.parseEmptyOrNull();
        else {
            if (this.parserFormatter == null)
                this.guessParseFormat(s);
            LocalDateTime dt;
            if (this.parseAsDate) {
                LocalDate date = LocalDate.parse(s, this.parserFormatter);
                dt = date.atStartOfDay();
            } else {
                dt = LocalDateTime.parse(s, this.parserFormatter);
            }
            this.append(dt);
        }
    }
}

