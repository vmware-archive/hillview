package org.hiero.sketch.table.api;

import org.hiero.sketch.table.ColumnDescription;

import java.util.Set;

/**
 * Represents the schema of a table.
 */
public interface ISchema {
    int getColumnCount();
    ColumnDescription getDescription(String columnName);
    ISchema project(ISubSchema subSchema);
    Set<String> getColumnNames();

    default ContentsKind getKind(final String colName){
        return this.getDescription(colName).kind;
    }
}
