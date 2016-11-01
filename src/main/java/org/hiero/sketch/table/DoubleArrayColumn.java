package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.RowComparator;

import java.security.InvalidParameterException;
import java.util.BitSet;

/**
 * Column of doubles, implemented as an array of doubles and a BitSet of missing values.
 */
public final class DoubleArrayColumn extends BaseArrayColumn {
    private final double[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Double)
            throw new InvalidParameterException("Kind should be Double " + this.description.kind);
    }

    public DoubleArrayColumn(final ColumnDescription description, final int size) {
        super(description, size);
        this.validate();
        this.data = new double[size];
    }

    public DoubleArrayColumn(final ColumnDescription description, final double[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    public DoubleArrayColumn(final ColumnDescription description, final double[] data,
                             final BitSet missing) {
        super(description, missing);
        this.validate();
        this.data = data;
    }

    @Override
    public int sizeInRows() { return this.data.length;}

    @Override
    public double getDouble(final int rowIndex) { return this.data[rowIndex];}

    @Override
    public double asDouble(final int rowIndex, final IStringConverter unused) {return this.data[rowIndex];}

    public void set(final int rowIndex, final double value) {this.data[rowIndex] = value;}

    public RowComparator getComparator() {
        return new RowComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
                return Double.compare(DoubleArrayColumn.this.data[i], DoubleArrayColumn.this.data[j]);
            }
        };
    }
}
