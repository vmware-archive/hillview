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
 * A column of String values that can grow in size.
 */
public class StringListColumn extends BaseListColumn implements IStringColumn {
    static final long serialVersionUID = 1;
    /*
     * We use one of two representations for string columns:
     * - for columns that have relatively few distinct values we use a dictionary encoding
     * - for columns with many distinct values we use just a list of values
     * We start optimistically with the first representation, but if we discover
     * too many distinct values then we switch to the second representation.
     */

    private final CategoryEncoding encoding;
    /**
     *All these arrays hold indexes into the encoding data structure.
     * We use byte-indexes until we run out of them.
     * Then we use short indexes.  If we run out of these we switch to a dense representation
     */
    private final ArrayList<short[]> shortSegments;
    private final ArrayList<byte[]> byteSegments;
    /**
     * first segment that uses shorts
     */
    private int firstShortSegment;
    /**
     * If non-null we are using a dense representation.
     */
    @Nullable
    private ArrayList<String[]> segments;

    public StringListColumn(final ColumnDescription desc) {
        super(desc);
        if (!desc.kind.isString())
            throw new IllegalArgumentException("Unexpected column kind " + desc.kind);
        this.byteSegments = new ArrayList<byte[]>();
        this.shortSegments = new ArrayList<short[]>();
        this.encoding = new CategoryEncoding();
        this.firstShortSegment = Integer.MAX_VALUE;
        this.segments = null;
    }

    private StringListColumn(
            ColumnDescription desc,
            final CategoryEncoding encoding,
            final ArrayList<short[]> shortSegments,
            final ArrayList<byte[]> byteSegments,
            @Nullable
            final ArrayList<String[]> segments,
            int firstShortSegment,
            int size) {
        super(desc);
        this.encoding = encoding;
        this.shortSegments = shortSegments;
        this.byteSegments = byteSegments;
        this.firstShortSegment = firstShortSegment;
        this.segments = segments;
        this.size = size;
    }

    /**
     * Returns true if we use the sparse representation with a dictionary encoding.
     */
    private boolean isSparse() {
        return this.segments == null;
    }

    @Nullable
    @Override
    public String getString(final int rowIndex) {
        if (rowIndex > this.size)
            throw new ArrayIndexOutOfBoundsException(
                    "Index " + rowIndex + " larger than " + this.size);
        if (this.isSparse()) {
            int segmentId = rowIndex >> LogSegmentSize;
            final int localIndex = rowIndex & SegmentMask;
            if (segmentId < this.firstShortSegment) {
                // use the byte segments
                byte[] segment = this.byteSegments.get(segmentId);
                byte index = segment[localIndex];
                return this.encoding.decode(Byte.toUnsignedInt(index));
            } else {
                segmentId = segmentId - this.firstShortSegment;
                short[] segment = this.shortSegments.get(segmentId);
                short index = segment[localIndex];
                return this.encoding.decode(Short.toUnsignedInt(index));
            }
        } else {
            final int segmentId = rowIndex >> LogSegmentSize;
            final int localIndex = rowIndex & SegmentMask;
            String[] segment = this.segments.get(segmentId);
            return segment[localIndex];
        }
    }

    @Override
    public IColumn seal() { return this; }

    @Override
    void grow() {
        if (this.isSparse()) {
            if (this.firstShortSegment == Integer.MAX_VALUE)
                this.byteSegments.add(new byte[SegmentSize]);
            else
                this.shortSegments.add(new short[SegmentSize]);
        } else {
            this.segments.add(new String[SegmentSize]);
        }
    }

    @Override
    public void append(@Nullable String value) {
        int segmentId = this.size >> LogSegmentSize;
        final int localIndex = this.size & SegmentMask;

        if (this.isSparse()) {
            int encoding = this.encoding.encode(value);

            if (this.firstShortSegment != Integer.MAX_VALUE) {
                if (encoding > 65535) {
                    HillviewLogger.instance.info(
                            "Switching column to dense encoding", "{0}", this);
                    ArrayList<String[]> segments = new ArrayList<String[]>(segmentId);
                    for (int i = 0; i <= segmentId; i++) {
                        String[] newSegment = new String[SegmentSize];
                        segments.add(newSegment);
                        int limit = (i == segmentId) ? localIndex : SegmentSize;
                        for (int j = 0; j < limit; j++) {
                            String s;
                            if (i < this.firstShortSegment) {
                                // use the byte segments
                                byte[] segment = this.byteSegments.get(i);
                                byte index = segment[j];
                                s = this.encoding.decode(Byte.toUnsignedInt(index));
                            } else {
                                short[] segment = this.shortSegments.get(i - this.firstShortSegment);
                                short index = segment[j];
                                s = this.encoding.decode(Short.toUnsignedInt(index));
                            }
                            newSegment[j] = s;
                        }
                    }
                    // Append the new value
                    segments.get(segments.size() - 1)[localIndex] = value;
                    this.segments = segments;
                    this.shortSegments.clear();
                    this.byteSegments.clear();
                    this.encoding.clear();
                } else {
                    segmentId -= this.firstShortSegment;
                    if (this.shortSegments.size() <= segmentId)
                        this.grow();
                    this.shortSegments.get(segmentId)[localIndex] = (short) encoding;
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
                            segment[i] = (short) Byte.toUnsignedInt(byteSegment[i]);
                        this.byteSegments.set(segmentId, null);
                    }
                    segment[localIndex] = (short) encoding;
                } else {
                    if (this.byteSegments.size() <= segmentId)
                        this.grow();
                    this.byteSegments.get(segmentId)[localIndex] = (byte) encoding;
                }
            }
        } else {
            int segmentCount = this.segments.size();
            if (segmentCount == segmentId)
                this.grow();
            else if (segmentCount != segmentId + 1)
                throw new RuntimeException("Not appending in last segment: " + segmentId + "/" + segmentCount);

            String[] segment = this.segments.get(segmentId);
            segment[localIndex] = value;
        }
        this.size++;
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        return this.getString(rowIndex) == null;
    }

    @Override
    public IColumn rename(String newName) {
        return new StringListColumn(
                this.description.rename(newName),
                this.encoding,
                this.shortSegments,
                this.byteSegments,
                this.segments,
                this.firstShortSegment,
                this.size);
    }

    @Override
    public void appendMissing() {
        this.append(null);
    }

    @Override
    public void parseAndAppendString(@Nullable String s) {
        this.append(s);
    }
}
