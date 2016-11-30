package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.ContentsKind;

import java.security.InvalidParameterException;
import java.util.Date;

/*
 * Column of dates, implemented as an array of dates and a BitSet of missing values
 */
public final class DateArrayColumn
        extends BaseArrayColumn
        implements IDateColumn {
    @NonNull
    private final Date[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Date)
            throw new InvalidParameterException("Kind should be Date" + this.description.kind);
    }

    public DateArrayColumn(@NonNull final ColumnDescription description, final int size) {
        super(description, size);
        this.validate();
        this.data = new Date[size];
    }

    public DateArrayColumn(@NonNull final ColumnDescription description,
                           @NonNull final Date[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    @Override
    public int sizeInRows() {
        return this.data.length;
    }

    @Override
    public Date getDate(final int rowIndex) {
        return this.data[rowIndex];
    }

    public void set(final int rowIndex, final Date value) {
        this.data[rowIndex] = value;
    }

    @Override
    public boolean isMissing(final int rowIndex) { return this.getDate(rowIndex) == null; }

    @Override
    public void setMissing(final int rowIndex) { this.set(rowIndex, null);}
}
