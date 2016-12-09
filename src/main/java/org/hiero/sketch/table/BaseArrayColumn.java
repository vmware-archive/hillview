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
        if (size <= 0)
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
}
