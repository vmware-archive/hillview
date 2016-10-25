package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;

/**
 * A IMembershipSet which contains all rows.
 */
public class FullMembership implements IMembershipSet {
    private final int rowCount;

    public FullMembership(int rowCount) throws NegativeArraySizeException {
            if (rowCount > 0)
                this.rowCount = rowCount;
            else
                throw (new NegativeArraySizeException("Can't initialize FullMembership with " +
                        "negative rowCount"));
    }

    @Override
    public boolean isMember(int rowIndex) {
        return rowIndex < this.rowCount && rowIndex >= 0;
    }

    @Override
    public int getSize() {
        return this.rowCount;
    }

    @Override
    public IRowIterator getIterator() {
        return new FullMemebershipIterator(this.rowCount);
    }

    private static class FullMemebershipIterator implements IRowIterator {
        private int cursor = 0;
        private final int range;

        public  FullMemebershipIterator(int range) {
            this.range = range;
        }

        @Override
        public int getNextRow() {
            if (cursor < range) {
                cursor++;
                return cursor-1;
            }
            else return -1;
        }
    }
}
