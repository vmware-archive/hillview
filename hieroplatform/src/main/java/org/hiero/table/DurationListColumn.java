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
import org.hiero.table.api.IDurationColumn;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;

/**
 * A column of time durations that can grow in size.
 */
class DurationListColumn extends BaseListColumn implements IDurationColumn {
    private final ArrayList<Duration[]> segments;

    public DurationListColumn(final ColumnDescription desc) {
        super(desc);
        this.checkKind(ContentsKind.Duration);
        this.segments = new ArrayList<Duration []>();
    }

    @Nullable
    @Override
    public Duration getDuration(final int rowIndex) {
        final int segmentId = rowIndex >> this.LogSegmentSize;
        final int localIndex = rowIndex & this.SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    @Override
    void grow() {
        this.segments.add(new Duration[this.SegmentSize]);
        this.growMissing();
    }

    @SuppressWarnings("Duplicates")
    private void append(@Nullable final Duration value) {
        final int segmentId = this.size >> this.LogSegmentSize;
        final int localIndex = this.size & this.SegmentMask;
        if (this.segments.size() <= segmentId)
            this.grow();
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        return this.getDuration(rowIndex) == null;
    }

    @Override
    public void appendMissing() {
        this.append(null);
    }

    @Override
    public void parseAndAppendString(@Nullable String s) {
        if ((s == null) || s.isEmpty())
            this.parseEmptyOrNull();
        else
            this.append(Duration.parse(s));
    }
}