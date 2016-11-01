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
    private Date[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Date)
            throw new InvalidParameterException("Kind should be Date" + description.kind);
    }

    /* Will set data array. If missing values are allowed initalize missing Bitset to an array of
     False */
    public DateArrayColumn(ColumnDescription description, int size) {
        super(description, size);
        this.validate();
        this.data = new Date[size];
    }

    /* Will set description, data array, and missing Bitset to an array of False of length equal
    to data */
    public DateArrayColumn(ColumnDescription description, Date[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    /* Will initialize data Array and missing Bitset by input*/
    public DateArrayColumn(ColumnDescription description, Date[] data, BitSet missing) {
        super(description, missing);
        this.validate();
        this.data = data;
    }

    @Override
    public int sizeInRows() {
        return data.length;
    }

    @Override
    public Date getDate(int rowIndex) {
        return this.data[rowIndex];
    }

    @Override
    public double asDouble(int rowIndex, IStringConverter unused) {
        Date tmp = this.data[rowIndex];
        return Converters.toDouble(tmp);
    }

    public void set(int rowIndex, Date value) {
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
