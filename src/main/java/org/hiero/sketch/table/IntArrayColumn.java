package org.hiero.sketch.table;

import java.security.InvalidParameterException;

/**
 * A column that stores data in an array.
 */
public final class IntArrayColumn extends BaseColumn {
    private int[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Int)
            throw new InvalidParameterException("Kind should be Int " + description.kind);
        if (this.description.allowMissing)
            throw new InvalidParameterException("Column cannot have nulls");
    }

    public IntArrayColumn(ColumnDescription description, int size) {
        super(description);
        if (size <= 0)
            throw new InvalidParameterException("Size must be positive: " + size);
        this.validate();
        data = new int[size];
    }

    public IntArrayColumn(ColumnDescription description, int [] data) {
        super(description);
        this.validate();
        this.data = data;
    }

    @Override
    public boolean isMissing(int rowIndex) { return false; }

    @Override
    public int sizeInRows() { return data.length; }

    @Override
    public int getInt(int rowIndex) { return this.data[rowIndex]; }

    @Override
    public double asDouble(int rowIndex, IStringConverter unused) { return this.data[rowIndex]; }

    public void set(int rowIndex, int value) { this.data[rowIndex] = value; }
}
