package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;

import java.time.Duration;
import java.util.ArrayList;

/**
 * A column of time durations that can grow in size.
 */
public class DurationListColumn extends BaseListColumn {
    private ArrayList<Duration[]> segments;

    public DurationListColumn(ColumnDescription desc) {
        super(desc);
        if (desc.kind != ContentsKind.TimeDuration)
            throw new IllegalArgumentException("Unexpected column kind " + desc.kind);
        this.segments = new ArrayList<Duration []>();
    }

    @Override
    public Duration getDuration(int rowIndex) {
        int segmentId = rowIndex >> LogSegmentSize;
        int localIndex = rowIndex & SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    @Override
    public double asDouble(int rowIndex, IStringConverter unused) {
        Duration s = this.getDuration(rowIndex);
        return Converters.toDouble(s);
    }

    public void append(Duration value) {
        int segmentId = this.size >> LogSegmentSize;
        int localIndex = this.size & SegmentMask;
        if (this.segments.size() <= segmentId) {
            this.segments.add(new Duration[SegmentSize]);
            this.growPresent();
        }
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }
}
