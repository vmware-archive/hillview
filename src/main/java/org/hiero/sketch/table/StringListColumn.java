package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;
import java.util.ArrayList;

/**
 * A column of Strings that can grow in size.
 */
public class StringListColumn extends BaseListColumn {
    private ArrayList<String[]> segments;

    public StringListColumn(ColumnDescription desc) {
        super(desc);
        if (desc.kind != ContentsKind.String && desc.kind != ContentsKind.Json)
            throw new IllegalArgumentException("Unexpected column kind " + desc.kind);
        this.segments = new ArrayList<String []>();
    }

    @Override
    public String getString(int rowIndex) {
        int segmentId = rowIndex >> LogSegmentSize;
        int localIndex = rowIndex & SegmentMask;
        return this.segments.get(segmentId)[localIndex];
    }

    @Override
    public double asDouble(int rowIndex, IStringConverter converter) {
        String s = this.getString(rowIndex);
        return converter.asDouble(s);
    }

    public void append(String value) {
        int segmentId = this.size >> LogSegmentSize;
        int localIndex = this.size & SegmentMask;
        if (this.segments.size() <= segmentId) {
            this.segments.add(new String[SegmentSize]);
            this.growPresent();
        }
        this.segments.get(segmentId)[localIndex] = value;
        this.size++;
    }
}
