package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.RowComparator;

import java.security.InvalidParameterException;
import java.util.ArrayList;

/**
 * A column of integers that can grow in size.
 */
public final class IntListColumn extends BaseListColumn {
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

    public double asDouble(final int rowIndex, final IStringConverter unused) {
        return this.getInt(rowIndex);
    }

    @Override
    public RowComparator getComparator() {
        return new RowComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
                return Integer.compare(IntListColumn.this.getInt(i), IntListColumn.this.getInt(j));
            }
        };
    }

    public void append(final int value) {
        final int segmentId = this.size >> this.LogSegmentSize;
        final int localIndex = this.size & this.SegmentMask;
        if (this.segments.size() <= segmentId) {
            this.segments.add(new int[this.SegmentSize]);
            this.growPresent();
        }
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }
}
