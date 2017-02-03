package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IDateColumn;

import java.time.LocalDateTime;
import java.util.ArrayList;

/**
 * A column of Dates that can grow in size.
 */
public class DateListColumn
        extends BaseListColumn
        implements IDateColumn {
    @NonNull
    private final ArrayList<LocalDateTime[]> segments;

    public DateListColumn(final ColumnDescription desc) {
        super(desc);
        if (desc.kind != ContentsKind.Date)
            throw new IllegalArgumentException("Unexpected column kind " + desc.kind);
        this.segments = new ArrayList<LocalDateTime[]>();
    }

    @Override
    public LocalDateTime getDate(final int rowIndex) {
        final int segmentId = rowIndex >> this.LogSegmentSize;
        final int localIndex = rowIndex & this.SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    private void append(final LocalDateTime value) {
        final int segmentId = this.size >> this.LogSegmentSize;
        final int localIndex = this.size & this.SegmentMask;
        if (this.segments.size() <= segmentId) {
            this.segments.add(new LocalDateTime[this.SegmentSize]);
            this.growMissing();
        }
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        return this.getDate(rowIndex) == null;
    }

    @Override
    public void appendMissing() {
        this.append(null);
    }
}

