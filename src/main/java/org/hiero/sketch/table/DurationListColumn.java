package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.RowComparator;

import java.time.Duration;
import java.util.ArrayList;

/**
 * A column of time durations that can grow in size.
 */
public class DurationListColumn extends BaseListColumn {
    private final ArrayList<Duration[]> segments;

    public DurationListColumn(final ColumnDescription desc) {
        super(desc);
        if (desc.kind != ContentsKind.TimeDuration)
            throw new IllegalArgumentException("Unexpected column kind " + desc.kind);
        this.segments = new ArrayList<Duration []>();
    }

    @Override
    public Duration getDuration(final int rowIndex) {
        final int segmentId = rowIndex >> this.LogSegmentSize;
        final int localIndex = rowIndex & this.SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    @Override
    public double asDouble(final int rowIndex, final IStringConverter unused) {
        final Duration s = this.getDuration(rowIndex);
        return Converters.toDouble(s);
    }

    public void append(final Duration value) {
        final int segmentId = this.size >> this.LogSegmentSize;
        final int localIndex = this.size & this.SegmentMask;
        if (this.segments.size() <= segmentId) {
            this.segments.add(new Duration[this.SegmentSize]);
            this.growPresent();
        }
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }

    public RowComparator getComparator() {
        return new RowComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
                return DurationListColumn.this.getDuration(i).compareTo(DurationListColumn.this.getDuration(j));
            }
        };
    }
}
