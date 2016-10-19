package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;

import java.security.InvalidParameterException;
import java.util.BitSet;

/**
 * Column of integers, implemented as an array of integers and a BitSet of missing values.
 */
public final class IntArrayColumn extends BaseArrayColumn {
    private int[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Int)
            throw new InvalidParameterException("Kind should be Int " + description.kind);
    }

    /* Will set data array and missing Bitset to an array of False of length equal to size */
    public IntArrayColumn(ColumnDescription description, int size) {
        super(description, size);
        this.validate();
        this.data = new int[size];
    }

    /* Will set description, data array, and missing Bitset to an array of False of length equal
    to data */
    public IntArrayColumn(ColumnDescription description, int[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    /* Will initialize data Array and missing Bitset by input*/
    public IntArrayColumn(ColumnDescription description, int[] data, BitSet missing) {
        super(description, missing);
        this.validate();
        this.data = data;
    }

    @Override
    public int sizeInRows() {
        return data.length;
    }

    @Override
    public int getInt(int rowIndex) {
        return this.data[rowIndex];
    }

    @Override
    public double asDouble(int rowIndex, IStringConverter unused) {
        return this.data[rowIndex];
    }

    public void set(int rowIndex, int value) {
        this.data[rowIndex] = value;
    }
}
