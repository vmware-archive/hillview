package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IndexComparator;

import javax.annotation.Nonnull;
import java.util.List;

public class ListComparator extends IndexComparator {
    @Nonnull
    private final List<IndexComparator> comparatorList;

    public ListComparator(@Nonnull final List<IndexComparator> comparatorList) {
        this.comparatorList = comparatorList;
    }

    @Override
    public int compare(final Integer o1, final Integer o2) {
        for (final IndexComparator aComparator : this.comparatorList) {
            final int val = aComparator.compare(o1, o2);
            if (val != 0) { return val; }
        }
        return 0;
    }
}