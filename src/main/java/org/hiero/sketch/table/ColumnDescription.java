package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;

/**
 * Describes the contents of a column in a local table.
 */
public class ColumnDescription {
    public final String name;
    public final ContentsKind kind;
    /**
     * If true the column can have missing values (called NULL in databases).
     */
    public final boolean allowMissing;

    public ColumnDescription(final String name, final ContentsKind kind, final boolean allowMissing) {
        this.name = name;
        this.kind = kind;
        this.allowMissing = allowMissing;
    }

    @Override public String toString() {
        return this.name + "(" + this.kind.toString() + ")";
    }
}
