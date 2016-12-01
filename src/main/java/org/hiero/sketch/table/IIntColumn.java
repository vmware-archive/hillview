package org.hiero.sketch.table;

import org.hiero.sketch.table.api.*;

public interface IIntColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, final IStringConverter unused) {
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

    default RowComparator getComparator() {
        return new RowComparator() {
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

    @Override
    default IColumn compress(final IRowOrder membershipSet) {
        final int size = membershipSet.getSize();
        final IRowIterator rowIt = membershipSet.getIterator();
        final IntArrayColumn result = new IntArrayColumn(this.getDescription(), size);
        int row = 0;
        while (true) {
            final int i = rowIt.getNextRow();
            if (i == -1) {
                break;
            }
            if (this.isMissing(i)) {
                result.setMissing(row);
            } else {
                result.set(row, this.getInt(i));
            }
            row++;
        }
        return result;
    }
}
