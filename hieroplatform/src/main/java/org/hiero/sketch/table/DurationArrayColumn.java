package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IDurationColumn;

import javax.annotation.Nullable;
import java.security.InvalidParameterException;
import java.time.Duration;

/*
 * Column of durations, implemented as an array of Durations and a BitSet of missing values
 */

public final class DurationArrayColumn extends BaseArrayColumn implements IDurationColumn {

    private final Duration[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Duration)
            throw new InvalidParameterException("Kind should be Time Duration"
                    + this.description.kind);
    }

    public DurationArrayColumn(final ColumnDescription description, final int size) {
        super(description, size);
        this.validate();
        this.data = new Duration[size];
    }

    public DurationArrayColumn(final ColumnDescription description,
                               final Duration[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }


    @Override
    public int sizeInRows() {
        return this.data.length;
    }

    @Override
    public Duration getDuration(final int rowIndex) {
        return this.data[rowIndex];
    }

    private void set(final int rowIndex, @Nullable final Duration value) {
        this.data[rowIndex] = value;
    }

    @Override
    public boolean isMissing(final int rowIndex){return this.getDuration(rowIndex) == null;}

    @Override
    public void setMissing(final int rowIndex) { this.set(rowIndex, null);}
}
