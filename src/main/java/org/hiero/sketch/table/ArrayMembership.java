package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;

import java.util.ArrayList;

public class ArrayMembership implements IMembershipSet {

    private ArrayList<Integer> sortedRows = new ArrayList<>();
    private final int size;

    public ArrayMembership(final int[] order) {
        this.size = order.length;
        for (int i =0; i < this.size; i++) {
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
                if (this.current < ArrayMembership.this.size) {
                    this.current++;
                    return ArrayMembership.this.sortedRows.get(this.current -1);
                } else {
                    return -1;
                }
            }
        };
    }

    @Override
    public boolean isMember(final int rowIndex) {
        return this.sortedRows.contains(rowIndex);
    }

    @Override
    public int getSize(final boolean exact) {
        return this.size;
    }

    @Override
    public IMembershipSet sample(final int k) {
        return null;
    }

    @Override
    public IMembershipSet sample(final int k, final long seed) {
        return null;
    }
}
