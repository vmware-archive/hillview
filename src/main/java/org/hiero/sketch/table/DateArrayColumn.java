package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.RowComparator;

import java.security.InvalidParameterException;
import java.util.BitSet;
import java.util.Date;

/*
 * Column of dates, implemented as an array of dates and a BisSet of missing values
 */
public final class DateArrayColumn extends BaseArrayColumn {
    private final Date[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Date)
            throw new InvalidParameterException("Kind should be Date" + this.description.kind);
    }

    public DateArrayColumn(final ColumnDescription description, final int size) {
        super(description, size);
        this.validate();
        this.data = new Date[size];
    }

    public DateArrayColumn(final ColumnDescription description, final Date[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    public DateArrayColumn(final ColumnDescription description, final Date[] data,
                           final BitSet missing) {
        super(description, missing);
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

    @Override
    public double asDouble(final int rowIndex, final IStringConverter unused) {
        final Date tmp = this.data[rowIndex];
        return Converters.toDouble(tmp);
    }

    public void set(final int rowIndex, final Date value) {
        this.data[rowIndex] = value;
    }

    public RowComparator getComparator() {
        return new RowComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
                return DateArrayColumn.this.getDate(i).compareTo(DateArrayColumn.this.getDate(j));
            }
        };
    }
}
