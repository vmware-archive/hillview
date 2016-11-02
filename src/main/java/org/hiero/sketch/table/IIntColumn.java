package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.RowComparator;

public interface IIntColumn extends IColumn {

    @Override
    default double asDouble(final int rowIndex, final IStringConverter unused) {
        return this.getInt(rowIndex);
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
}
