package org.hiero.sketch.table;

/**
 * Describes the contents of a column in a local table.
 */
public class ColumnDescription {
    String       name;
    ContentsKind kind;
    /**
     * If true the column can have missing values (called NULL in databases).
     */
    boolean allowMissing;

    public ColumnDescription(String name, ContentsKind kind, boolean allowMissing) {
        this.name = name;
        this.kind = kind;
        this.allowMissing = allowMissing;
    }

    @Override public String toString() {
        return this.name + "(" + this.kind.toString() + ")";
    }
}
