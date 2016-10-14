package org.hiero.sketch.table;

/**
 * Represents a IMembershipMap which represents all rows.
 */
public class FullMembership implements IMembershipMap {
    private final int rowCount;

    public FullMembership(int rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public boolean isMember(int rowIndex) {
        return rowIndex > 0 && rowIndex < this.rowCount;
    }
}
