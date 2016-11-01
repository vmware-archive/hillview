package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.RowComparator;

import java.util.ArrayList;

/**
 * A column of doubles that can grow in size.
 */
public class DoubleListColumn extends BaseListColumn {
    private final ArrayList<double[]> segments;

    public DoubleListColumn(final ColumnDescription desc) {
        super(desc);
        if (desc.kind != ContentsKind.Double)
            throw new IllegalArgumentException("Unexpected column kind " + desc.kind);
        this.segments = new ArrayList<double []>();
    }

    @Override
    public double getDouble(final int rowIndex) {
        final int segmentId = rowIndex >> this.LogSegmentSize;
        final int localIndex = rowIndex & this.SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    @Override
    public double asDouble(final int rowIndex, final IStringConverter unused) {
        return this.getDouble(rowIndex);
    }

    public void append(final double value) {
        final int segmentId = this.size >> this.LogSegmentSize;
        final int localIndex = this.size & this.SegmentMask;
        if (this.segments.size() <= segmentId) {
            this.segments.add(new double[this.SegmentSize]);
            this.growPresent();
        }
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }

    public RowComparator getComparator() {
        return new RowComparator() {
            @Override
            public int compare(Integer i, Integer j) {
                return Double.compare(getDouble(i), getDouble(j));
            }
        };
    }
}
