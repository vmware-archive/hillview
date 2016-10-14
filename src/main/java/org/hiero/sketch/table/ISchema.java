package org.hiero.sketch.table;

/**
 * Represents the schema of a table.
 */
public interface ISchema {
    ColumnDescription getDescription(int index);
    int getColumnCount();
    int getColumnIndex(String columnName);
}
