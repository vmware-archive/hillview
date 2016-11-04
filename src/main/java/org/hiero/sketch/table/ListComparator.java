package org.hiero.sketch.table;

import org.hiero.sketch.table.api.RowComparator;

import java.util.List;

public class ListComparator extends RowComparator {
    private final List<RowComparator> comparatorList;

    public ListComparator(final List<RowComparator> comparatorList) {
        this.comparatorList = comparatorList;
    }

    @Override
    public int compare(final Integer o1, final Integer o2) {
        for (final RowComparator aComparator : this.comparatorList) {
            final int val = aComparator.compare(o1, o2);
            if (val != 0) { return val; }
        }
        return 0;
    }
}