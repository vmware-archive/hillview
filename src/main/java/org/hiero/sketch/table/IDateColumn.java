package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.RowComparator;

import java.util.Date;

public interface IDateColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, final IStringConverter unused) {
        final Date tmp = this.getDate(rowIndex);
        return Converters.toDouble(tmp);
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
}
