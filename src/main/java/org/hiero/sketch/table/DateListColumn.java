package org.hiero.sketch.table.api;

import org.hiero.sketch.table.BaseListColumn;
import org.hiero.sketch.table.ColumnDescription;

import java.util.ArrayList;

/**
 * A column of Dates that can grow in size.
 */
public class DateListColumn extends BaseListColumn {
    private ArrayList<Date[]> segments;

    public DateListColumn(ColumnDescription desc) {
        super(desc);
        this.segments = new ArrayList<Date []>();
    }

    @Override
    public Date getDate(int rowIndex) {
        int segmentId = rowIndex >> LogSegmentSize;
        int localIndex = rowIndex & SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    @Override
    public double asDouble(int rowIndex, IDateConverter converter) {
        Date s = this.getDate(rowIndex);
        return converter.asDouble(s);
    }

    public void append(Date value) {
        int segmentId = this.size >> LogSegmentSize;
        int localIndex = this.size & SegmentMask;
        if (this.segments.size() < segmentId) {
            this.segments.add(new Date[SegmentSize]);
            this.growPresent();
        }
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }
}

