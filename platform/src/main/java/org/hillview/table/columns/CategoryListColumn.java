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
import org.hillview.table.api.ICategoryColumn;
import org.hillview.table.api.IColumn;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.function.Consumer;

/**
 * A column of Categorical values that can grow in size.
 */
public class CategoryListColumn extends BaseListColumn implements ICategoryColumn {
    private CategoryEncoding encoding;
    private final ArrayList<int[]> segments;

    public CategoryListColumn(final ColumnDescription desc) {
        super(desc);
        if (desc.kind != ContentsKind.Category)
            throw new IllegalArgumentException("Unexpected column kind " + desc.kind);
        this.segments = new ArrayList<int[]>();
        this.encoding = new CategoryEncoding();
    }

    @Nullable
    @Override
    public String getString(final int rowIndex) {
        final int segmentId = rowIndex >> this.LogSegmentSize;
        final int localIndex = rowIndex & this.SegmentMask;
        int[] segment = this.segments.get(segmentId);
        int index = segment[localIndex];
        return this.encoding.decode(index);
    }

    @Override
    public IColumn seal() { return this; }

    @Override
    void grow() {
        this.segments.add(new int[this.SegmentSize]);
        this.growMissing();
    }

    @Override
    public void append(@Nullable String value) {
        final int segmentId = this.size >> this.LogSegmentSize;
        final int localIndex = this.size & this.SegmentMask;
        if (this.segments.size() <= segmentId)
            this.grow();
        if (value == null)
            return;
        this.segments.get(segmentId)[localIndex] = this.encoding.encode(value);
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

    @Override
    public void allDistinctStrings(Consumer<String> action) {
        this.encoding.allDistinctStrings(action);
    }
}
