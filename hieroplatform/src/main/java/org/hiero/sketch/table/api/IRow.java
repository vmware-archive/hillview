package org.hiero.sketch.table.api;

import org.hiero.sketch.table.Schema;

public interface IRow {
    /**
     * @return  The number of fields in the row
     */
    int rowSize();

    Schema getSchema();

    Object get(final String colName);

    default ContentsKind getKind(final String colName) {
        return this.getSchema().getKind(colName);
    }
}
