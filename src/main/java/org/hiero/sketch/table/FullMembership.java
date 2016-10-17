package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IMembershipMap;
import org.hiero.sketch.table.api.IRowIterator;

import java.util.function.Function;

/**
 * A IMembershipMap which contains all rows.
 */
public class FullMembership implements IMembershipMap {
    private final int rowCount;

    public FullMembership(int rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public boolean isMember(int rowIndex) {
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

    @Override
    public IMembershipMap subset(Function<Integer, Boolean> isMember) {
        return null;
    }

    @Override
    public IMembershipMap sample(double percentage) {
        return null;
    }
}
