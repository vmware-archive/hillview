package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IIntColumn;

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
        if (this.description.kind != ContentsKind.Int)
            throw new InvalidParameterException("Kind should be Int " + this.description.kind);
        this.segments = new ArrayList<int []>();
    }

    @Override
    public int getInt(final int rowIndex) {
        final int segmentId = rowIndex >> this.LogSegmentSize;
        final int localIndex = rowIndex & this.SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

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
}
