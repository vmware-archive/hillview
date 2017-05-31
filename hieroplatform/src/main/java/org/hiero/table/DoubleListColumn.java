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
import org.hiero.table.api.IDoubleColumn;

import javax.annotation.Nullable;
import java.util.ArrayList;

/**
 * A column of doubles that can grow in size.
 */
public class DoubleListColumn
        extends BaseListColumn implements IDoubleColumn {
    private final ArrayList<double[]> segments;

    public DoubleListColumn(final ColumnDescription desc) {
        super(desc);
        this.checkKind(ContentsKind.Double);
        this.segments = new ArrayList<double []>();
    }

    @Override
    public double getDouble(final int rowIndex) {
        final int segmentId = rowIndex >> this.LogSegmentSize;
        final int localIndex = rowIndex & this.SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    void grow() {
        this.segments.add(new double[this.SegmentSize]);
        this.growMissing();
    }

    @SuppressWarnings("Duplicates")
    public void append(final double value) {
        final int segmentId = this.size >> this.LogSegmentSize;
        final int localIndex = this.size & this.SegmentMask;
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
            this.append(Double.parseDouble(s));
    }
}
