package org.hiero.sketch.table.api;

import javax.annotation.Nullable;
import java.time.LocalDateTime;

public interface IDateColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, @Nullable final IStringConverter unused) {
        if (isMissing(rowIndex))
            throw new MissingException(this, rowIndex);
        final LocalDateTime tmp = this.getDate(rowIndex);
        return Converters.toDouble(tmp);
    }

    @Override
    default String asString(final int rowIndex) {
        if (this.isMissing(rowIndex))
            return null;
        return this.getDate(rowIndex).toString();
    }

    @Override
    default IndexComparator getComparator() {
        return new IndexComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
                final boolean iMissing = IDateColumn.this.isMissing(i);
                final boolean jMissing = IDateColumn.this.isMissing(j);
                if (iMissing && jMissing) {
                    return 0;
                } else if (iMissing) {
                    return 1;
                } else if (jMissing) {
                    return -1;
                } else {
                    return IDateColumn.this.getDate(i).compareTo(IDateColumn.this.getDate(j));
                }
            }
        };
    }
}
