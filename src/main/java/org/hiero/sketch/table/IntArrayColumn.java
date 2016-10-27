package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;

import java.security.InvalidParameterException;

/**
 * A column that stores data in an array.
 */
public final class IntArrayColumn extends BaseColumn {
    private final int[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Int)
            throw new InvalidParameterException("Kind should be Int " + this.description.kind);
    }

    public IntArrayColumn(final ColumnDescription description, final int size) {
        super(description);
        if (size <= 0)
            throw new InvalidParameterException("Size must be positive: " + size);
        this.validate();
        this.data = new int[size];
    }

    public IntArrayColumn(final ColumnDescription description, final int [] data) {
        super(description);
        this.validate();
        this.data = data;
    }

    @Override
    public boolean isMissing(final int rowIndex) { return false; }

    @Override
    public int sizeInRows() { return this.data.length; }

    @Override
    public int getInt(final int rowIndex) { return this.data[rowIndex]; }

    @Override
    public double asDouble(final int rowIndex, final IStringConverter unused) { return this.data[rowIndex]; }

    public void set(final int rowIndex, final int value) { this.data[rowIndex] = value; }
}
