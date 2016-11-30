package org.hiero.sketch.table.api;

import java.util.Comparator;

/**
 * A comparator which compares two rows given by their integer indexes.
 */
public abstract class RowComparator implements Comparator<Integer> {
    /**
     * The reverse comparator.
     */
    public RowComparator rev() {
        return new RowComparator() {
            @Override
            public int compare(final Integer o1, final Integer o2) {
                return RowComparator.this.compare(o2, o1);
            }
        };
    }
}
