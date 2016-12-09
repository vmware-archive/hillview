package org.hiero.sketch.table.api;

public interface IRow {
    /**
     * @return  The number of fields in the row
     */
    int rowSize();

    ISchema getSchema();

    Object get(int colIndex);

    default Object get(final String colName) {
        return this.get(this.getSchema().getColumnIndex(colName));
    }

    default ContentsKind getKind(final String colName) {
        return this.getSchema().getDescription(this.getSchema().getColumnIndex(colName)).kind;
    }
}
