package org.hiero.sketch.table;

import org.hiero.sketch.table.api.*;

public interface IDoubleColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, final IStringConverter unused) {
        return this.getDouble(rowIndex);
    }

    @Override
    default String asString(final int rowIndex) {
        if (this.isMissing(rowIndex))
            return null;
        return Double.toString(this.getDouble(rowIndex));
    }

    @Override
    default RowComparator getComparator() {
        return new RowComparator() {
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

    @Override
    default IColumn compress(final IMembershipSet set) {
        final int size = set.getSize();
        final IRowIterator rowIt = set.getIterator();
        final DoubleArrayColumn result = new DoubleArrayColumn(this.getDescription(), size);
        int row = 0;
        while (true) {
            final int i = rowIt.getNextRow();
            if (i == -1) {
                break;
            }
            if (this.isMissing(i)) {
                result.setMissing(row);
            } else {
                result.set(row, this.getDouble(i));
            }
            row++;
        }
        return result;
    }
}
