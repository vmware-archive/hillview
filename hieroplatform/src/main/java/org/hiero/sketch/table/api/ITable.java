package org.hiero.sketch.table.api;

import org.hiero.sketch.table.Schema;
import org.hiero.sketch.table.SmallTable;

/**
 * An ITable object has a schema, a set of columns, and a MembershipSet.
 * All columns have the same size.
 */
public interface ITable {
    Schema getSchema();

    IRowIterator getRowIterator();

    /**
     * Describes the set of rows that are really present in the table.
     */
    IMembershipSet getMembershipSet();

    int getNumOfRows();

    IColumn getColumn(String colName);

    /**
     * Creates a small table by keeping only the rows in the IRowOrder and
     * the columns in the subSchema.
     * @param subSchema Indicates columns to keep.
     * @param rowOrder  Indicates rows to keep.
     */
    SmallTable compress(ISubSchema subSchema, IRowOrder rowOrder);

    SmallTable compress(IRowOrder rowOrder);
}
