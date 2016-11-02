package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.RowComparator;

public interface IStringColumn extends IColumn {
    @Override
    default double asDouble(final int rowIndex, final IStringConverter conv) {
        final String tmp = this.getString(rowIndex);
        return conv.asDouble(tmp);
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
}
