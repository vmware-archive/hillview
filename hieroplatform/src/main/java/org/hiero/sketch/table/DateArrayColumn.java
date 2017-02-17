package org.hiero.sketch.table;

import javax.annotation.Nonnull;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IDateColumn;

import java.security.InvalidParameterException;
import java.time.LocalDateTime;

/*
 * Column of dates, implemented as an array of dates and a BitSet of missing values
 */
public final class DateArrayColumn
        extends BaseArrayColumn
        implements IDateColumn {
    @Nonnull
    private final LocalDateTime[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Date)
            throw new InvalidParameterException("Kind should be Date" + this.description.kind);
    }

    public DateArrayColumn(@Nonnull final ColumnDescription description, final int size) {
        super(description, size);
        this.validate();
        this.data = new LocalDateTime[size];
    }

    public DateArrayColumn(@Nonnull final ColumnDescription description,
                           @Nonnull final LocalDateTime[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    @Override
    public int sizeInRows() {
        return this.data.length;
    }

    @Override
    public LocalDateTime getDate(final int rowIndex) {
        return this.data[rowIndex];
    }

    private void set(final int rowIndex, final LocalDateTime value) {
        this.data[rowIndex] = value;
    }

    @Override
    public boolean isMissing(final int rowIndex) { return this.getDate(rowIndex) == null; }

    @Override
    public void setMissing(final int rowIndex) { this.set(rowIndex, null);}
}
