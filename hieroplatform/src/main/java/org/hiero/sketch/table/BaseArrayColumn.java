package org.hiero.sketch.table;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.BitSet;

/**
 * Adds a missing bit vector to BaseColumn for integers and doubles (if missing values are allowed)
 */
abstract class BaseArrayColumn extends BaseColumn implements Serializable {
    private BitSet missing = null;

    BaseArrayColumn(final ColumnDescription description, final int size) {
        super(description);
        if (size < 0)
            throw new InvalidParameterException("Size must be positive: " + size);
        if (this.description.allowMissing && !this.description.kind.isObject())
            this.missing = new BitSet(size);
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        return this.description.allowMissing && this.missing.get(rowIndex);
    }

    public void setMissing(final int rowIndex) {
        this.missing.set(rowIndex);
    }

    /**
     * Create an empty column with the specified description.
     * @param description Column description.
     */
    public static BaseArrayColumn create(ColumnDescription description) {
        switch (description.kind) {
            case Json:
            case String:
                return new StringArrayColumn(description, 0);
            case Date:
                return new DateArrayColumn(description, 0);
            case Int:
                return new IntArrayColumn(description, 0);
            case Double:
                return new DoubleArrayColumn(description, 0);
            case Duration:
                return new DurationArrayColumn(description, 0);
            default:
                throw new RuntimeException(description.toString());
        }
    }
}
