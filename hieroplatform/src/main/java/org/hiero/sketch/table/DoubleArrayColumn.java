package org.hiero.sketch.table;

import javax.annotation.Nonnull;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IDoubleColumn;

import java.security.InvalidParameterException;

/**
 * Column of doubles, implemented as an array of doubles and a BitSet of missing values.
 */
public final class DoubleArrayColumn
        extends BaseArrayColumn
        implements IDoubleColumn {

    private final double[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Double)
            throw new InvalidParameterException("Kind should be Double " + this.description.kind);
    }

    public DoubleArrayColumn( final ColumnDescription description, final int size) {
        super(description, size);
        this.validate();
        this.data = new double[size];
    }

    public DoubleArrayColumn( final ColumnDescription description,
                              final double[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    @Override
    public int sizeInRows() { return this.data.length;}

    @Override
    public double getDouble(final int rowIndex) { return this.data[rowIndex];}

    public void set(final int rowIndex, final double value) {this.data[rowIndex] = value;}
}
