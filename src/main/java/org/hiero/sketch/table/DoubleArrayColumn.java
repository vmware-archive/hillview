package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;

import java.security.InvalidParameterException;
import java.util.BitSet;

/**
 * Column of doubles, implemented as an array of doubles and a BitSet of missing values.
 */
public final class DoubleArrayColumn extends BaseArrayColumn {
    private double[] data;

    private void validate() {
        if (this.description.kind != ContentsKind.Double)
            throw new InvalidParameterException("Kind should be Double " + description.kind);
    }

    /* Will set data array. If missing values are allowed, initalize missing Bitset to an array of
     False */
    public DoubleArrayColumn(ColumnDescription description, int size) {
        super(description, size);
        this.validate();
        data = new double[size];
    }

    /* Will set description, data array, and missing Bitset to an array of False of length equal
    to data */
    public DoubleArrayColumn(ColumnDescription description, double[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    /* Will initialize data Array and missing Bitset by input*/
    public DoubleArrayColumn(ColumnDescription description, double[] data, BitSet missing) {
        super(description, missing);
        this.validate();
        this.data = data;
    }

    @Override
    public int sizeInRows() { return data.length;}

    @Override
    public double getDouble(int rowIndex) { return this.data[rowIndex];}

    @Override
    public double asDouble(int rowIndex, IStringConverter unused) {return this.data[rowIndex];}

    public void set(int rowIndex, double value) {this.data[rowIndex] = value;}
}
