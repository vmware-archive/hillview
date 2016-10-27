package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;

/**
 * A IMembershipSet which contains all rows.
 */
public class FullMembership implements IMembershipSet {
    private final int rowCount;

    public FullMembership(final int rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public boolean isMember(final int rowIndex) {
        return rowIndex < this.rowCount;
    }

    @Override
    public int getSize() {
        return this.rowCount;
    }

    @Override
    public IRowIterator getIterator() {
        return null;
    }
}
