package org.hiero.sketch.table.api;

import javax.annotation.Nullable;

public interface IIntColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, @Nullable final IStringConverter unused) {
        if (isMissing(rowIndex))
            throw new MissingException(this, rowIndex);
        return this.getInt(rowIndex);
    }

    @Override
    default String asString(final int rowIndex) {
        if (this.isMissing(rowIndex))
            return null;
        return Integer.toString(this.getInt(rowIndex));
    }

    default IndexComparator getComparator() {
        return new IndexComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
                final boolean iMissing = IIntColumn.this.isMissing(i);
                final boolean jMissing = IIntColumn.this.isMissing(j);
                if (iMissing && jMissing) {
                    return 0;
                } else if (iMissing) {
                    return 1;
                } else if (jMissing) {
                    return -1;
                } else {
                    return Integer.compare(IIntColumn.this.getInt(i), IIntColumn.this.getInt(j));
                }
            }
        };
    }
}
