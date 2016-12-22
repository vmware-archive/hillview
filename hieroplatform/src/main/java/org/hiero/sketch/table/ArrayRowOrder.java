package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IRowOrder;

import java.util.ArrayList;

public class ArrayRowOrder implements IRowOrder {

    private final ArrayList<Integer> sortedRows = new ArrayList<>();
    private final int size;

    public ArrayRowOrder(final int[] order) {
        this.size = order.length;
        for (int i = 0; i < this.size; i++) {
            this.sortedRows.add(order[i]);
        }
    }

    @Override
    public int getSize() {
        return this.size;
    }

    @Override
    public IRowIterator getIterator() {
        return new IRowIterator() {
            private int current = 0;

            @Override
            public int getNextRow() {
                if (this.current < ArrayRowOrder.this.size) {
                    this.current++;
                    return ArrayRowOrder.this.sortedRows.get(this.current - 1);
                } else {
                    return -1;
                }
            }
        };
    }
}
