package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.RowComparator;

import java.security.InvalidParameterException;
import java.util.BitSet;

/**
 * Column of integers, implemented as an array of integers and a BitSet of missing values.
 */
public final class IntArrayColumn extends BaseArrayColumn {
    private final int[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Int)
            throw new InvalidParameterException("Kind should be Int " + this.description.kind);
    }

    public IntArrayColumn(final ColumnDescription description, final int size) {
        super(description, size);
        this.validate();
        this.data = new int[size];
    }

    public IntArrayColumn(final ColumnDescription description, final int[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    public IntArrayColumn(final ColumnDescription description,
                          final int[] data, final BitSet missing) {
        super(description, missing);
        this.validate();
        this.data = data;
    }

    @Override
    public int sizeInRows() {
        return this.data.length;
    }

    @Override
    public int getInt(final int rowIndex) {
        return this.data[rowIndex];
    }

    @Override
    public double asDouble(final int rowIndex, final IStringConverter unused) {
        return this.data[rowIndex];
    }

    public void set(final int rowIndex, final int value) {
        this.data[rowIndex] = value;
    }

    @Override
    public RowComparator getComparator() {
        return new RowComparator() {
            @Override
            public int compare(Integer i, Integer j) {
                return Integer.compare(data[i], data[j]);
            }
        };
    }
}
