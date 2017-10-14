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

import javax.annotation.Nullable;
import java.util.ArrayList;

/**
 * A column of Strings that can grow in size.
 */
public class StringListColumn extends BaseListColumn implements IStringColumn {
    private final ArrayList<String[]> segments;

    public StringListColumn(final ColumnDescription desc) {
        super(desc);
        if ((desc.kind != ContentsKind.String) &&
                (desc.kind != ContentsKind.Json) &&
                (desc.kind != ContentsKind.Category))
            throw new IllegalArgumentException("Unexpected column kind " + desc.kind);
        this.segments = new ArrayList<String []>();
    }

    @Override
    public IColumn seal() { return this; }

    @Nullable
    @Override
    public String getString(final int rowIndex) {
        final int segmentId = rowIndex >> LogSegmentSize;
        final int localIndex = rowIndex & SegmentMask;
        String[] segment = this.segments.get(segmentId);
        return segment[localIndex];
    }

    @Override
    void grow() {
        this.segments.add(new String[SegmentSize]);
        this.growMissing();
    }

    @Override
    public void append(@Nullable String value) {
        if (value != null)
            value = value.intern();

        final int segmentId = this.size >> LogSegmentSize;
        final int localIndex = this.size & SegmentMask;
        if (this.segments.size() <= segmentId)
            this.grow();
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        return this.getString(rowIndex) == null;
    }

    @Override
    public void appendMissing() {
        this.append((String)null);
    }

    @Override
    public void parseAndAppendString(@Nullable String s) {
        this.append(s);
    }
}
