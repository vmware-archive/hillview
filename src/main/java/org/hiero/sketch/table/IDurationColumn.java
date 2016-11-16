package org.hiero.sketch.table;

import org.hiero.sketch.table.api.*;

import java.time.Duration;

public interface IDurationColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, final IStringConverter unused) {
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
    default RowComparator getComparator() {
        return new RowComparator() {
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
                    return IDurationColumn.this.getDate(i).
                            compareTo(IDurationColumn.this.getDate(j));
                }
            }
        };
    }


    @Override
    default IColumn compress(final IMembershipSet set) {
        final int size = set.getSize();
        final IRowIterator rowIt = set.getIterator();
        final DurationArrayColumn result = new DurationArrayColumn(this.getDescription(), size);
        int row = 0;
        while (true) {
            final int i = rowIt.getNextRow();
            if (i == -1) {
                break;
            }
            if (this.isMissing(i)) {
                result.setMissing(row);
            } else {
                result.set(row, this.getDuration(i));
            }
            row++;
        }
        return result;
    }

}
