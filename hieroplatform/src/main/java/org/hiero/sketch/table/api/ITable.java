package org.hiero.sketch.table.api;

import org.hiero.sketch.table.Schema;
import org.hiero.sketch.table.SmallTable;

import javax.annotation.Nonnull;

public interface ITable {
    @Nonnull
    Schema getSchema();

    @Nonnull IRowIterator getRowIterator();

    /**
     * Describes the set of rows that are really present in the table.
     */
    @Nonnull IMembershipSet getMembershipSet();

    int getNumOfRows();

    IColumn getColumn(String colName);

    SmallTable compress(@Nonnull ISubSchema subSchema,
                        @Nonnull IRowOrder rowOrder);

    SmallTable compress(IRowOrder rowOrder);
}
