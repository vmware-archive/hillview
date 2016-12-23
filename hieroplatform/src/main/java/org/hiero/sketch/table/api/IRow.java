package org.hiero.sketch.table.api;

public interface IRow {
    /**
     * @return  The number of fields in the row
     */
    int rowSize();

    ISchema getSchema();

    Object get(final String colName);

    default ContentsKind getKind(final String colName) {
        return this.getSchema().getKind(colName);
    }
}
