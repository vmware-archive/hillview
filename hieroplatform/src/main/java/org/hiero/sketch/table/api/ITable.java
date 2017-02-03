package org.hiero.sketch.table.api;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.Schema;
import org.hiero.sketch.table.SmallTable;
import org.hiero.sketch.table.api.*;

public interface ITable {
    @NonNull Schema getSchema();

    @NonNull IRowIterator getRowIterator();

    /**
     * Describes the set of rows that are really present in the table.
     */
    @NonNull IMembershipSet getMembershipSet();

    int getNumOfRows();

    IColumn getColumn(String colName);

    SmallTable compress(@NonNull ISubSchema subSchema,
                        @NonNull IRowOrder rowOrder);

    SmallTable compress(IRowOrder rowOrder);
}
