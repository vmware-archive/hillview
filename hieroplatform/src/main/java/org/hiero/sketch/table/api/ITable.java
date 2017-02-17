package org.hiero.sketch.table.api;

import org.hiero.sketch.table.Schema;
import org.hiero.sketch.table.SmallTable;

public interface ITable {

    Schema getSchema();

    IRowIterator getRowIterator();

    /**
     * Describes the set of rows that are really present in the table.
     */
    IMembershipSet getMembershipSet();

    int getNumOfRows();

    IColumn getColumn(String colName);

    SmallTable compress(ISubSchema subSchema,
                        IRowOrder rowOrder);

    SmallTable compress(IRowOrder rowOrder);
}
