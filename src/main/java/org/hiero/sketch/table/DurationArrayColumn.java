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
    private Duration[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.TimeDuration)
            throw new InvalidParameterException("Kind should be Time Duration" + description.kind);
    }

    /* Will set data array. If missing values are allowed initalize missing Bitset to an array of
     False */
    public DurationArrayColumn(ColumnDescription description, int size) {
        super(description, size);
        this.validate();
        this.data = new Duration[size];
    }

    /* Will set description, data array, and missing Bitset to an array of False of length equal
    to data */
    public DurationArrayColumn(ColumnDescription description, Duration[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    /* Will initialize data Array and missing Bitset by input*/
    public DurationArrayColumn(ColumnDescription description, Duration[] data, BitSet missing) {
        super(description, missing);
        this.validate();
        this.data = data;
    }

    @Override
    public int sizeInRows() { return data.length; }

    @Override
    public Duration getDuration(int rowIndex) { return this.data[rowIndex]; }

    @Override
    public double asDouble(int rowIndex, IStringConverter unused) {
        Duration tmp = this.data[rowIndex];
        return Converters.toDouble(tmp);
    }

    public void set(int rowIndex, Duration value) {
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
