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
import org.hillview.table.api.IAppendableColumn;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;

/**
 * Base class for a column that can grow in size.
 */
public abstract class BaseListColumn extends BaseColumn implements IAppendableColumn {
    static final long serialVersionUID = 1;

    // These should not be public, but they are made public for simplifying testing.
    static final int LogSegmentSize = 11;
    public static final int SegmentSize = 1 << LogSegmentSize;
    static final int SegmentMask = SegmentSize - 1;

    @Nullable
    ArrayList<BitSet> missing = null;
    int size;

    BaseListColumn(final ColumnDescription desc) {
        super(desc);
        if (!desc.kind.isObject())
            this.missing = new ArrayList<BitSet>();
        this.size = 0;
    }

    void checkMissingSize(int size) {
        if (this.missing != null && this.missing.size() != size)
            throw new RuntimeException("Missing size does not match column data: " +
                    this.missing.size() + " vs. " + size);
    }

    @Override
    public boolean isLoaded() { return true; }

    @Override
    public int sizeInRows() {
        return this.size;
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        if (this.missing == null)
            return false;
        final int segmentId = rowIndex >> LogSegmentSize;
        final int localIndex = rowIndex & SegmentMask;
        return this.missing.get(segmentId).get(localIndex);
    }

    @Override
    public void appendMissing() {
        assert this.missing != null;
        final int segmentId = this.size >> LogSegmentSize;
        final int localIndex = this.size & SegmentMask;
        if (this.missing.size() <= segmentId) {
            this.grow();
        }
        this.missing.get(segmentId).set(localIndex);
        this.size++;
    }

    void parseEmptyOrNull() {
        this.appendMissing();
    }

    abstract void grow();

    void growMissing() {
        if (this.missing != null)
            this.missing.add(new BitSet(SegmentSize));
    }

    public static IAppendableColumn create(ColumnDescription desc) {
        switch (desc.kind) {
            case String:
            case Json:
                return new StringListColumn(desc);
            case None:
                return new EmptyColumn(desc, 0);
            case Integer:
                return new IntListColumn(desc);
            case Date:
            case Double:
            case Duration:
            case LocalDate:
            case Time:
                return new DoubleListColumn(desc);
            case Interval:
            default:
                throw new RuntimeException("Unexpected description " + desc.toString());
        }
    }
}
