package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.ContentsKind;

import java.security.InvalidParameterException;

/**
 * Column of Strings, implemented as an array of strings and a bit vector of missing values.
 * Allows ContentsKind String or Json
 */
public final class StringArrayColumn
        extends BaseArrayColumn implements IStringColumn {
    @NonNull
    private final String[] data;

    private void validate() {
        if ((this.description.kind != ContentsKind.String) &&
                (this.description.kind != ContentsKind.Json))
            throw new InvalidParameterException("Kind should be String or Json "
                    + this.description.kind);
    }

    public StringArrayColumn(@NonNull final ColumnDescription description, final int size) {
        super(description, size);
        this.validate();
        this.data = new String[size];
    }

    public StringArrayColumn(@NonNull final ColumnDescription description,
                             @NonNull final String[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    @Override
    public int sizeInRows() {
        return this.data.length;
    }

    @Override
    public String getString(final int rowIndex) {
        return this.data[rowIndex];
    }

    public void set(final int rowIndex, final String value) {
        this.data[rowIndex] = value;
    }

    @Override
    public boolean isMissing(final int rowIndex){return this.getString(rowIndex) == null;}

    @Override
    public void setMissing(final int rowIndex) { this.set(rowIndex, null);}
}
