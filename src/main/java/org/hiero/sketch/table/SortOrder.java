package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IRowOrder;
import org.hiero.sketch.table.api.RowComparator;

import java.util.Arrays;

/**
 * Given a (table and a) RowComparator, gives an iterator for the rows of the table in sorted order
 */
public class SortOrder implements IRowOrder {
    private Integer[] order;

    /**
     * @param size Number of rows
     * @param rowComparator Defines the ordering
     */
    public SortOrder(final int size, final RowComparator rowComparator) {
        for (int i = 0; i < size; i++)
            this.order[i] = i;
        Arrays.sort(this.order, rowComparator);
    }

    @Override
    public int getSize() {
        return this.order.length;
    }

    @Override
    public IRowIterator getIterator() {
        return new IRowIterator() {
            private int current = 0;

            @Override
            public int getNextRow() {
                if (this.current < SortOrder.this.order.length) {
                    final int i = SortOrder.this.order[this.current];
                    this.current++;
                    return i;
                } else {
                    return -1;
                }
            }
        };
    }
}
