package org.hiero.sketch.table.api;

import javax.annotation.Nullable;

public interface IStringColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, @Nullable final IStringConverter conv) {
        if (isMissing(rowIndex))
            throw new MissingException(this, rowIndex);
        final String tmp = this.getString(rowIndex);
        if (conv == null)
            throw new NullPointerException();
        return conv.asDouble(tmp);
    }

    @Override
    default String asString(final int rowIndex) {
        return this.getString(rowIndex);
    }

    @Override
    default IndexComparator getComparator() {
        return new IndexComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
                final boolean iMissing = IStringColumn.this.isMissing(i);
                final boolean jMissing = IStringColumn.this.isMissing(j);
                if (iMissing && jMissing) {
                    return 0;
                } else if (iMissing) {
                    return 1;
                } else if (jMissing) {
                    return -1;
                } else {
                    return IStringColumn.this.getString(i).compareTo(
                            IStringColumn.this.getString(j));
                }
            }
        };
    }
}