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

import org.hillview.utils.Converters;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;

/**
 * Base class for a column that can grow in size.
 */
public abstract class BaseListColumn extends BaseColumn {
    // These should not be public, but they are made public for simplifying testing.
    final int LogSegmentSize = 10;
    public final int SegmentSize = 1 << this.LogSegmentSize;
    final int SegmentMask = this.SegmentSize - 1;

    @Nullable
    private ArrayList<BitSet> missing = null;
    int size;

    BaseListColumn(final ColumnDescription desc) {
        super(desc);
        if (desc.allowMissing && !desc.kind.isObject())
            this.missing = new ArrayList<BitSet>();
        this.size = 0;
    }

    @Override
    public int sizeInRows() {
        return this.size;
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        if (this.description.allowMissing) {
            if (this.missing == null)
                return false;
            final int segmentId = rowIndex >> this.LogSegmentSize;
            final int localIndex = rowIndex & this.SegmentMask;
            return this.missing.get(segmentId).get(localIndex);
        } else
            return false;
    }

    public void setMissing(final int rowIndex) {
        if (this.description.allowMissing) {
            Converters.checkNull(this.missing);
            final int segmentId = rowIndex >> this.LogSegmentSize;
            final int localIndex = rowIndex & this.SegmentMask;
            this.missing.get(segmentId).set(localIndex);
        }
    }

    public void appendMissing() {
        Converters.checkNull(this.missing);
        final int segmentId = this.size >> this.LogSegmentSize;
        final int localIndex = this.size & this.SegmentMask;
        if (this.missing.size() <= segmentId) {
            this.grow();
        }
        this.missing.get(segmentId).set(localIndex);
        this.size++;
    }

    public abstract void parseAndAppendString(@Nullable String s);

    void parseEmptyOrNull() {
        if (!this.description.allowMissing)
            throw new RuntimeException("Appending missing data to column " + this.toString());
        this.appendMissing();
    }

    abstract void grow();

    void growMissing() {
        if (this.missing != null)
            this.missing.add(new BitSet(this.SegmentSize));
    }

    public static BaseListColumn create(ColumnDescription desc) {
        switch (desc.kind) {
            case Category:
                return new CategoryListColumn(desc);
            case String:
            case Json:
                return new StringListColumn(desc);
            case Date:
                return new DateListColumn(desc);
            case Integer:
                return new IntListColumn(desc);
            case Double:
                return new DoubleListColumn(desc);
            case Duration:
                return new DurationListColumn(desc);
            default:
                throw new RuntimeException("Unexpected description " + desc.toString());
        }
    }

    @Override
    public String toString() {
        return this.getDescription().toString();
    }
}
