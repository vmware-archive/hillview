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
import org.hiero.table.api.IColumn;
import org.hiero.table.api.IIntColumn;
import org.hiero.table.api.IStringConverter;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.util.ArrayList;

/**
 * A column of integers that can grow in size.
 */
public final class IntListColumn
        extends BaseListColumn
        implements IIntColumn {
    private final ArrayList<int[]> segments;

    private IntListColumn(IntListColumn other, @Nullable IStringConverter converter) {
        super(other, converter);
        this.checkKind(ContentsKind.Double);
        this.segments = other.segments;
    }

    public IntListColumn(final ColumnDescription desc) {
        super(desc);
        if (this.description.kind != ContentsKind.Integer)
            throw new InvalidParameterException("Kind should be Integer " + this.description.kind);
        this.segments = new ArrayList<int []>();
    }

    public IColumn setDefaultConverter(@Nullable final IStringConverter converter) {
        return new IntListColumn(this, converter);
    }

    @Override
    public int getInt(final int rowIndex) {
        final int segmentId = rowIndex >> this.LogSegmentSize;
        final int localIndex = rowIndex & this.SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    @SuppressWarnings("Duplicates")
    public void append(final int value) {
        final int segmentId = this.size >> this.LogSegmentSize;
        final int localIndex = this.size & this.SegmentMask;
        if (this.segments.size() <= segmentId) {
            this.segments.add(new int[this.SegmentSize]);
            this.growMissing();
        }
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }

    @Override
    public void parseAndAppendString(String s) {
        if (s.isEmpty())
            this.parseEmptyOrNull();
        else
            this.append(Integer.parseInt(s));
    }
}
