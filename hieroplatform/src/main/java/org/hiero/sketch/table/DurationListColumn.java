package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IDurationColumn;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;

/**
 * A column of time durations that can grow in size.
 */
class DurationListColumn extends BaseListColumn implements IDurationColumn {
    private final ArrayList<Duration[]> segments;

    public DurationListColumn(final ColumnDescription desc) {
        super(desc);
        if (desc.kind != ContentsKind.Duration)
            throw new IllegalArgumentException("Unexpected column kind " + desc.kind);
        this.segments = new ArrayList<Duration []>();
    }

    @Override
    public Duration getDuration(final int rowIndex) {
        final int segmentId = rowIndex >> this.LogSegmentSize;
        final int localIndex = rowIndex & this.SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    private void append(@Nullable final Duration value) {
        final int segmentId = this.size >> this.LogSegmentSize;
        final int localIndex = this.size & this.SegmentMask;
        if (this.segments.size() <= segmentId) {
            this.segments.add(new Duration[this.SegmentSize]);
            this.growMissing();
        }
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        return this.getDuration(rowIndex) == null;
    }

    @Override
    public void appendMissing() {
        this.append(null);
    }
}