package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.IStringConverter;
import org.hiero.sketch.table.api.RowComparator;

import java.security.InvalidParameterException;
import java.util.BitSet;

/**
 * Column of Strings, implemented as an array of strings and a bit vector of missing values.
 * Allows ContentsKind String or Json
 */
public final class StringArrayColumn extends BaseArrayColumn {
    private final String[] data;

    private void validate() {
        if ((this.description.kind != ContentsKind.String) && (this.description.kind != ContentsKind
                .Json))
            throw new InvalidParameterException("Kind should be String or Json " + this.description
                    .kind);
    }

    public StringArrayColumn(final ColumnDescription description, final int size) {
        super(description, size);
        this.validate();
        this.data = new String[size];
    }

    public StringArrayColumn(final ColumnDescription description, final String[] data) {
        super(description, data.length);
        this.validate();
        this.data = data;
    }

    public StringArrayColumn(final ColumnDescription description, final String[] data, final BitSet missing) {
        super(description, missing);
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

    @Override
    public double asDouble(final int rowIndex, final IStringConverter conv) {
        final String tmp = this.data[rowIndex];
        return conv.asDouble(tmp);
    }

    public void set(final int rowIndex, final String value) {
        this.data[rowIndex] = value;
    }

    public RowComparator getComparator() {
        return new RowComparator() {
            @Override
            public int compare(final Integer i, final Integer j) {
                return StringArrayColumn.this.data[i].compareTo(StringArrayColumn.this.data[j]);
            }
        };
    }
}
