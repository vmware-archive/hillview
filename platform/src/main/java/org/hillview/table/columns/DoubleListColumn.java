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
import org.hillview.table.api.*;
import org.hillview.utils.Converters;
import org.hillview.utils.DateParsing;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.BitSet;

/**
 * A column of doubles that can grow in size.
 */
public class DoubleListColumn
        extends BaseListColumn implements IDoubleColumn {
    static final long serialVersionUID = 1;

    private final ArrayList<double[]> segments;
    // Only used for Date kinds
    @Nullable
    private DateParsing dateParser;

    public DoubleListColumn(final ColumnDescription desc) {
        super(desc);
        this.segments = new ArrayList<double []>();
    }

    private DoubleListColumn(final ColumnDescription desc, ArrayList<double[]> segments,
                             @Nullable ArrayList<BitSet> missing, int size) {
        super(desc);
        this.segments = segments;
        this.missing = missing;
        this.size = size;
    }

    @Override
    public IColumn seal() {
        this.checkMissingSize(this.segments.size());
        this.segments.trimToSize();
        return this;
    }

    @Override
    public IColumn rename(String newName) {
        return new DoubleListColumn(
                this.description.rename(newName), this.segments, this.missing, this.size);
    }

    @Override
    public double getDouble(final int rowIndex) {
        final int segmentId = rowIndex >> LogSegmentSize;
        final int localIndex = rowIndex & SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    @Override
    void grow() {
        this.segments.add(new double[SegmentSize]);
        this.growMissing();
    }

    public void append(final double value) {
        final int segmentId = this.size >> LogSegmentSize;
        final int localIndex = this.size & SegmentMask;
        int segmentCount = this.segments.size();
        if (segmentCount == segmentId)
            this.grow();
        else if (segmentCount != segmentId + 1)
            throw new RuntimeException("Not appending in last segment: " + segmentId + "/" + segmentCount);
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }

    @Override
    public void parseAndAppendString(@Nullable String s) {
        if ((s == null) || s.isEmpty())
            this.parseEmptyOrNull();
        else {
            try {
                switch (this.getKind()) {
                    case Double:
                        this.append(Double.parseDouble(s));
                        break;
                    case Date:
                        if (this.dateParser == null)
                            this.dateParser = new DateParsing(s);
                        Instant dt = this.dateParser.parseDate(s);
                        this.append(Converters.toDouble(dt));
                        break;
                    case LocalDate:
                        if (this.dateParser == null)
                            this.dateParser = new DateParsing(s);
                        LocalDateTime ldt = this.dateParser.parseLocalDate(s);
                        this.append(Converters.toDouble(ldt));
                        break;
                    case Duration:
                        this.append(Converters.toDouble(Duration.parse(s)));
                        break;
                    case Time:
                        this.append(Converters.toDouble(LocalTime.parse(s)));
                        break;
                    default:
                        throw new RuntimeException("Unexpected kind " + this.getKind());
                }
            } catch (Exception ex) {
                this.parsingExceptionCount++;
                this.parseEmptyOrNull();
            }
        }
    }
}
