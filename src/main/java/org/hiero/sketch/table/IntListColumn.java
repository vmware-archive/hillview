package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;

import java.security.InvalidParameterException;
import java.util.ArrayList;

/**
 * A column of integers that can grow in size.
 */
public final class IntListColumn extends BaseListColumn {
    private ArrayList<int[]> segments;

    public IntListColumn(ColumnDescription desc) {
        super(desc);
        if (this.description.kind != ContentsKind.Int)
            throw new InvalidParameterException("Kind should be Int " + description.kind);
        this.segments = new ArrayList<int []>();
    }

    @Override
    public int getInt(int rowIndex) {
        int segmentId = rowIndex >> LogSegmentSize;
        int localIndex = rowIndex & SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    public double asDouble(int rowIndex, IStringConverter unused) {
        return this.getInt(rowIndex);
    }

    public void append(int value) {
        int segmentId = this.size >> LogSegmentSize;
        int localIndex = this.size & SegmentMask;
        if (this.segments.size() <= segmentId) {
            this.segments.add(new int[SegmentSize]);
            this.growPresent();
        }
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }
}
