package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.RowComparator;

import java.util.ArrayList;
import java.util.Date;

/**
 * A column of Dates that can grow in size.
 */
public class DateListColumn extends BaseListColumn {
    private final ArrayList<Date[]> segments;

    public DateListColumn(final ColumnDescription desc) {
        super(desc);
        if (desc.kind != ContentsKind.Date)
            throw new IllegalArgumentException("Unexpected column kind " + desc.kind);
        this.segments = new ArrayList<Date []>();
    }

    @Override
    public Date getDate(final int rowIndex) {
        final int segmentId = rowIndex >> this.LogSegmentSize;
        final int localIndex = rowIndex & this.SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    @Override
    public double asDouble(final int rowIndex, final IStringConverter unused) {
        final Date s = this.getDate(rowIndex);
        return Converters.toDouble(s);
    }

    public void append(final Date value) {
        final int segmentId = this.size >> this.LogSegmentSize;
        final int localIndex = this.size & this.SegmentMask;
        if (this.segments.size() <= segmentId) {
            this.segments.add(new Date[this.SegmentSize]);
            this.growPresent();
        }
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }

    public RowComparator getComparator() {
        return new RowComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
                return DateListColumn.this.getDate(i).compareTo(DateListColumn.this.getDate(j));
            }
        };
    }
}

