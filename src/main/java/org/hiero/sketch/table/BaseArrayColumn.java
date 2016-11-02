package org.hiero.sketch.table;

import java.security.InvalidParameterException;
import java.util.BitSet;

/**
 * Adds a BitSet missing to BaseColumn for integers and doubles (if missing values are allowed)
 * to evaluate the IsMissing method in the IColumn interface.
 */
abstract class BaseArrayColumn extends BaseColumn {
    private BitSet missing;

    BaseArrayColumn(final ColumnDescription description, final int size) {
        super(description);
        if (size <= 0)
            throw new InvalidParameterException("Size must be positive: " + size);
        if (this.description.allowMissing && !this.description.kind.isObject())
            this.missing = new BitSet(size);
    }

    @Override
    public boolean isMissing(final int rowIndex) {
        return this.missing.get(rowIndex);
    }

    public void setMissing(final int rowIndex) {
        this.missing.set(rowIndex);
    }

    public void setMissing(final int fromIndex, final int toIndex) {
        this.missing.set(fromIndex, toIndex);
    }
}
