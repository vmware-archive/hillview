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
import org.hillview.utils.HillviewLogger;

import javax.annotation.Nullable;
import java.util.ArrayList;

/**
 * A column of Categorical values that can grow in size.
 */
public class CategoryListColumn extends BaseListColumn implements ICategoryColumn {
    private final CategoryEncoding encoding;
    // All these arrays hold indexes into the encoding data structure.
    // We use byte-indexes until we run out of them.
    // Then we use short indexes, and hopefully we never run out of them.
    // But if we do we switch to using int indexes.
    private final ArrayList<int[]> intSegments;
    private final ArrayList<short[]> shortSegments;
    private final ArrayList<byte[]> byteSegments;
    // first segment that uses shorts
    private int firstShortSegment;
    // first segment that uses integers
    private int firstIntSegment;

    public CategoryListColumn(final ColumnDescription desc) {
        super(desc);
        if (desc.kind != ContentsKind.Category)
            throw new IllegalArgumentException("Unexpected column kind " + desc.kind);
        this.byteSegments = new ArrayList<byte[]>();
        this.intSegments = new ArrayList<int[]>();
        this.shortSegments = new ArrayList<short[]>();
        this.encoding = new CategoryEncoding();
        this.firstIntSegment = Integer.MAX_VALUE;
        this.firstShortSegment = Integer.MAX_VALUE;
    }

    private CategoryListColumn(
            ColumnDescription desc,
            final CategoryEncoding encoding,
            final ArrayList<int[]> intSegments,
            final ArrayList<short[]> shortSegments,
            final ArrayList<byte[]> byteSegments,
            int firstShortSegment,
            int firstIntSegment,
            int size) {
        super(desc);
        this.encoding = encoding;
        this.intSegments = intSegments;
        this.shortSegments = shortSegments;
        this.byteSegments = byteSegments;
        this.firstIntSegment = firstIntSegment;
        this.firstShortSegment = firstShortSegment;
        this.size = size;
    }

    @Nullable
    @Override
    public String getString(final int rowIndex) {
        if (rowIndex > this.size)
            throw new ArrayIndexOutOfBoundsException(
                    "Index " + rowIndex + " larger than " + this.size);
        int segmentId = rowIndex >> LogSegmentSize;
        final int localIndex = rowIndex & SegmentMask;
        if (segmentId < this.firstShortSegment) {
            // use the byte segments
            byte[] segment = this.byteSegments.get(segmentId);
            byte index = segment[localIndex];
            return this.encoding.decode(Byte.toUnsignedInt(index));
        } else if (segmentId < this.firstIntSegment) {
            segmentId = segmentId - this.firstShortSegment;
            short[] segment = this.shortSegments.get(segmentId);
            short index = segment[localIndex];
            return this.encoding.decode(Short.toUnsignedInt(index));
        } else {
            segmentId = segmentId - this.firstIntSegment;
            int[] segment = this.intSegments.get(segmentId);
            int index = segment[localIndex];
            return this.encoding.decode(index);
        }
    }

    @Override
    public IColumn seal() { return this; }

    @Override
    void grow() {
        if (this.firstIntSegment == Integer.MAX_VALUE) {
            if (this.firstShortSegment == Integer.MAX_VALUE)
                this.byteSegments.add(new byte[SegmentSize]);
            else
                this.shortSegments.add(new short[SegmentSize]);
        } else {
            this.intSegments.add(new int[SegmentSize]);
        }
    }

    @Override
    public void append(@Nullable String value) {
        int segmentId = this.size >> LogSegmentSize;
        int localIndex = this.size & SegmentMask;
        int encoding = this.encoding.encode(value);

        if (this.firstIntSegment != Integer.MAX_VALUE) {
            // use an integer encoding
            segmentId -= this.firstIntSegment;
            if (this.intSegments.size() <= segmentId)
                this.grow();
            this.intSegments.get(segmentId)[localIndex] = encoding;
        } else if (this.firstShortSegment != Integer.MAX_VALUE) {
            if (encoding > 65535) {
                // switch to an int encoding.  This should not really happen,
                // but we guard here for wrong schemas.
                HillviewLogger.instance.warn(
                        "Categorical column has too many values", "{0}", this);
                this.firstIntSegment = segmentId;
                int[] segment = new int[SegmentSize];
                this.intSegments.add(segment);
                if (localIndex > 0) {
                    segmentId -= this.firstShortSegment;
                    short[] shortSegment = this.shortSegments.get(segmentId);
                    for (int i = 0; i < localIndex; i++)
                        segment[i] = Short.toUnsignedInt(shortSegment[i]);
                    this.shortSegments.set(segmentId, null);
                }
                segment[localIndex] = encoding;
            } else {
                segmentId -= this.firstShortSegment;
                if (this.shortSegments.size() <= segmentId)
                    this.grow();
                this.shortSegments.get(segmentId)[localIndex] = (short)encoding;
            }
        } else {
            if (encoding > 255) {
                // switch to a short encoding
                this.firstShortSegment = segmentId;
                short[] segment = new short[SegmentSize];
                this.shortSegments.add(segment);
                if (localIndex > 0) {
                    byte[] byteSegment = this.byteSegments.get(segmentId);
                    for (int i = 0; i < localIndex; i++)
                        segment[i] = (short)Byte.toUnsignedInt(byteSegment[i]);
                    this.byteSegments.set(segmentId, null);
                }
                segment[localIndex] = (short)encoding;
            } else {
                if (this.byteSegments.size() <= segmentId)
                    this.grow();
                this.byteSegments.get(segmentId)[localIndex] = (byte)encoding;
            }
        }
        this.size++;
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        return this.getString(rowIndex) == null;
    }

    @Override
    public IColumn rename(String newName) {
        return new CategoryListColumn(
                this.description.rename(newName),
                this.encoding,
                this.intSegments,
                this.shortSegments,
                this.byteSegments,
                this.firstShortSegment,
                this.firstIntSegment,
                this.size);
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
