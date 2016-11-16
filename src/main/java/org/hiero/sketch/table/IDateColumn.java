package org.hiero.sketch.table;

import org.hiero.sketch.table.api.*;

import java.util.Date;

public interface IDateColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, final IStringConverter unused) {
        final Date tmp = this.getDate(rowIndex);
        return Converters.toDouble(tmp);
    }

    @Override
    default String asString(final int rowIndex) {
        if (this.isMissing(rowIndex))
            return null;
        return this.getDate(rowIndex).toString();
    }

    @Override
    default RowComparator getComparator() {
        return new RowComparator() {
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

    @Override
    default IColumn compress(IMembershipSet set) {
        int size = set.getSize();
        IRowIterator rowIt = set.getIterator();
        DateArrayColumn result = new DateArrayColumn(this.getDescription(), size);
        int row = 0;
        while (true) {
            int i = rowIt.getNextRow();
            if (i == -1) {
                break;
            }
            if (this.isMissing(i)) {
                result.setMissing(row);
            } else {
                result.set(row, this.getDate(i));
            }
            row++;
        }
        return result;
    }
}
