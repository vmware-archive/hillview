package org.hiero.sketch.table.api;

import java.util.Comparator;

/**
 * A comparator which compares two values given by their integer indexes in an array/column/table.
 */
public abstract class IndexComparator implements Comparator<Integer> {
    /**
     * The reverse comparator.
     */
    public IndexComparator rev() {
        return new IndexComparator() {
            @Override
            public int compare(final Integer o1, final Integer o2) {
                return IndexComparator.this.compare(o2, o1);
            }
        };
    }
}
