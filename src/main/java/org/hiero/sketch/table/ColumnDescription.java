package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.ContentsKind;

/**
 * Describes the contents of a column in a local table.
 */
public class ColumnDescription {
    @NonNull
    public final String name;
    @NonNull
    public final ContentsKind kind;
    /**
     * If true the column can have missing values (called NULL in databases).
     */
    public final boolean allowMissing;

    public ColumnDescription(final String name, final ContentsKind kind,
                             final boolean allowMissing) {
        this.name = name;
        this.kind = kind;
        this.allowMissing = allowMissing;
    }

    @Override public String toString() {
        return this.name + "(" + this.kind.toString() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnDescription that = (ColumnDescription) o;

        if (this.allowMissing != that.allowMissing) return false;
        if (!this.name.equals(that.name)) return false;
        return this.kind == that.kind;
    }

    @Override
    public int hashCode() {
        int result = this.name.hashCode();
        result = 31 * result + this.kind.hashCode();
        result = 31 * result + (this.allowMissing ? 1 : 0);
        return result;
    }
}
