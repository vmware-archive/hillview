package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.RowComparator;

import java.util.ArrayList;

/**
 * A column of doubles that can grow in size.
 */
public class DoubleListColumn extends BaseListColumn {
    private ArrayList<double[]> segments;

    public DoubleListColumn(ColumnDescription desc) {
        super(desc);
        if (desc.kind != ContentsKind.Double)
            throw new IllegalArgumentException("Unexpected column kind " + desc.kind);
        this.segments = new ArrayList<double []>();
    }

    @Override
    public double getDouble(int rowIndex) {
        int segmentId = rowIndex >> LogSegmentSize;
        int localIndex = rowIndex & SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    @Override
    public double asDouble(int rowIndex, IStringConverter unused) {
        return this.getDouble(rowIndex);
    }

    public void append(double value) {
        int segmentId = this.size >> LogSegmentSize;
        int localIndex = this.size & SegmentMask;
        if (this.segments.size() <= segmentId) {
            this.segments.add(new double[SegmentSize]);
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
