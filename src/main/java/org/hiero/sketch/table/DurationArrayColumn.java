package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.RowComparator;

import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.BitSet;

/*
 * Column of durations, implemented as an array of Durations and a BitSet of missing values
 */

public final class DurationArrayColumn extends BaseArrayColumn {
    private final Duration[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.TimeDuration)
            throw new InvalidParameterException("Kind should be Time Duration" + this.description.kind);
    }

    public DurationArrayColumn(final ColumnDescription description, final int size) {
        super(description, size);
        this.validate();
        this.data = new Duration[size];
    }

    public DurationArrayColumn(final ColumnDescription description, final Duration[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    public DurationArrayColumn(final ColumnDescription description, final Duration[] data, final BitSet missing) {
        super(description, missing);
        this.validate();
        this.data = data;
    }

    @Override
    public int sizeInRows() { return this.data.length; }

    @Override
    public Duration getDuration(final int rowIndex) { return this.data[rowIndex]; }

    @Override
    public double asDouble(final int rowIndex, final IStringConverter unused) {
        final Duration tmp = this.data[rowIndex];
        return Converters.toDouble(tmp);
    }

    public void set(final int rowIndex, final Duration value) {
        this.data[rowIndex] = value;
    }

    public RowComparator getComparator() {
        return new RowComparator() {
            @Override
            public int compare(Integer i, Integer j) {
                return Double.compare(asDouble(i, null), asDouble(j, null));
            }
        };
    }
}
