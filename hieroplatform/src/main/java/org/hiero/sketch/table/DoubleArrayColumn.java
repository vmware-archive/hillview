package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IDoubleColumn;

import java.security.InvalidParameterException;

/**
 * Column of doubles, implemented as an array of doubles and a BitSet of missing values.
 */
public final class DoubleArrayColumn
        extends BaseArrayColumn
        implements IDoubleColumn {
    @NonNull
    private final double[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Double)
            throw new InvalidParameterException("Kind should be Double " + this.description.kind);
    }

    public DoubleArrayColumn(@NonNull final ColumnDescription description, final int size) {
        super(description, size);
        this.validate();
        this.data = new double[size];
    }

    public DoubleArrayColumn(@NonNull final ColumnDescription description,
                             @NonNull final double[] data) {
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
