package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IDurationColumn;

import java.security.InvalidParameterException;
import java.time.Duration;

/*
 * Column of durations, implemented as an array of Durations and a BitSet of missing values
 */

public final class DurationArrayColumn extends BaseArrayColumn implements IDurationColumn {
    @NonNull
    private final Duration[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Duration)
            throw new InvalidParameterException("Kind should be Time Duration"
                    + this.description.kind);
    }

    public DurationArrayColumn(@NonNull final ColumnDescription description, final int size) {
        super(description, size);
        this.validate();
        this.data = new Duration[size];
    }

    public DurationArrayColumn(@NonNull final ColumnDescription description,
                               @NonNull final Duration[] data) {
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

    private void set(final int rowIndex, final Duration value) {
        this.data[rowIndex] = value;
    }

    @Override
    public boolean isMissing(final int rowIndex){return this.getDuration(rowIndex) == null;}

    @Override
    public void setMissing(final int rowIndex) { this.set(rowIndex, null);}
}
