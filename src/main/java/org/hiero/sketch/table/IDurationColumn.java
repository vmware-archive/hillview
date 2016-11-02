package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.RowComparator;
import java.time.Duration;

public interface IDurationColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, final IStringConverter unused) {
        final Duration tmp = this.getDuration(rowIndex);
        return Converters.toDouble(tmp);
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
}
