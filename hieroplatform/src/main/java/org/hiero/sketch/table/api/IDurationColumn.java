package org.hiero.sketch.table.api;

import java.time.Duration;

public interface IDurationColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, final IStringConverter unused) {
        if (isMissing(rowIndex))
            throw new MissingException(this, rowIndex);
        final Duration tmp = this.getDuration(rowIndex);
        return Converters.toDouble(tmp);
    }

    @Override
    default String asString(final int rowIndex) {
        if (this.isMissing(rowIndex))
            return null;
        return this.getDuration(rowIndex).toString();
    }

    @Override
    default IndexComparator getComparator() {
        return new IndexComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
                final boolean iMissing = IDurationColumn.this.isMissing(i);
                final boolean jMissing = IDurationColumn.this.isMissing(j);
                if (iMissing && jMissing) {
                    return 0;
                } else if (iMissing) {
                    return 1;
                } else if (jMissing) {
                    return -1;
                } else {
                    return IDurationColumn.this.getDuration(i).
                            compareTo(IDurationColumn.this.getDuration(j));
                }
            }
        };
    }
}
