package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;

import java.util.ArrayList;
import java.util.Date;

/**
 * A column of Dates that can grow in size.
 */
public class DateListColumn extends BaseListColumn {
    private ArrayList<Date[]> segments;

    public DateListColumn(ColumnDescription desc) {
        super(desc);
        if (desc.kind != ContentsKind.Date)
            throw new IllegalArgumentException("Unexpected column kind " + desc.kind);
        this.segments = new ArrayList<Date []>();
    }

    @Override
    public Date getDate(int rowIndex) {
        int segmentId = rowIndex >> LogSegmentSize;
        int localIndex = rowIndex & SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    @Override
    public double asDouble(int rowIndex, IStringConverter unused) {
        Date s = this.getDate(rowIndex);
        return Converters.toDouble(s);
    }

    public void append(Date value) {
        int segmentId = this.size >> LogSegmentSize;
        int localIndex = this.size & SegmentMask;
        if (this.segments.size() <= segmentId) {
            this.segments.add(new Date[SegmentSize]);
            this.growPresent();
        }
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }
}

