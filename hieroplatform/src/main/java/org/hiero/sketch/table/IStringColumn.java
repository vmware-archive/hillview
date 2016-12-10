package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.*;

public interface IStringColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, @NonNull final IStringConverter conv) {
        if (isMissing(rowIndex))
            throw new MissingException(this, rowIndex);
        final String tmp = this.getString(rowIndex);
        return conv.asDouble(tmp);
    }

    @Override
    default String asString(final int rowIndex) {
        return this.getString(rowIndex);
    }

    @Override
    default RowComparator getComparator() {
        return new RowComparator() {
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

    @Override
    default IColumn compress(@NonNull final IMembershipSet set) {
        final int size = set.getSize();
        final IRowIterator rowIt = set.getIterator();
        final StringArrayColumn result = new StringArrayColumn(this.getDescription(), size);
        int row = 0;
        while (true) {
            final int i = rowIt.getNextRow();
            if (i == -1) {
                break;
            }
            if (this.isMissing(i)) {
                result.setMissing(row);
            } else {
                result.set(row, this.getString(i));
            }
            row++;
        }
        return result;
    }
}