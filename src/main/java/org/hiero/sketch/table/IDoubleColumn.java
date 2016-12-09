package org.hiero.sketch.table;

import org.hiero.sketch.table.api.*;

public interface IDoubleColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, final IStringConverter unused) {
        if (isMissing(rowIndex))
            throw new MissingException(this, rowIndex);
        return this.getDouble(rowIndex);
    }

    @Override
    default String asString(final int rowIndex) {
        if (this.isMissing(rowIndex))
            return null;
        return Double.toString(this.getDouble(rowIndex));
    }

    @Override
    default IndexComparator getComparator() {
        return new IndexComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
                final boolean iMissing = IDoubleColumn.this.isMissing(i);
                final boolean jMissing = IDoubleColumn.this.isMissing(j);
                if (iMissing && jMissing) {
                    return 0;
                } else if (iMissing) {
                    return 1;
                } else if (jMissing) {
                    return -1;
                } else {
                    return Double.compare(IDoubleColumn.this.getDouble(i), IDoubleColumn.this.getDouble(j));
                }
            }
        };
    }
}
