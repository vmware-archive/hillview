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
import org.hillview.table.api.ContentsKind;
import org.hillview.table.api.IColumn;
import org.hillview.table.api.IIntColumn;

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

   public IntListColumn(final ColumnDescription desc) {
        super(desc);
        if (this.description.kind != ContentsKind.Integer)
            throw new InvalidParameterException("Kind should be Integer " + this.description.kind);
        this.segments = new ArrayList<int []>();
    }

    @Override
    public IColumn seal() {
        this.checkMissingSize(this.segments.size());
        return this;
    }

    @Override
    public int getInt(final int rowIndex) {
        final int segmentId = rowIndex >> LogSegmentSize;
        final int localIndex = rowIndex & SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    @Override
    void grow() {
        this.segments.add(new int[SegmentSize]);
        this.growMissing();
    }

    @SuppressWarnings("Duplicates")
    public void append(final int value) {
        final int segmentId = this.size >> LogSegmentSize;
        final int localIndex = this.size & SegmentMask;
        if (this.segments.size() <= segmentId)
            this.grow();
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }

    @Override
    public void parseAndAppendString(@Nullable String s) {
        if ((s == null) || s.isEmpty())
            this.parseEmptyOrNull();
        else
            this.append(Integer.parseInt(s));
    }
}
