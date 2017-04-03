package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IRow;
import org.hiero.utils.Converters;

/**
 * An abstract class that implements IRow, which is an interface for accessing rows in a table.
 * Concrete classes that extend it are RowSnapshot and VirtualRowSnapshot. The main methods this
 * class provides is equality testing. This for instance allows easy comparison between the
 * classes mentioned above.
 */
public abstract class BaseRowSnapshot implements IRow {
    protected final Schema schema;

    protected BaseRowSnapshot(Schema schema) {
        this.schema = schema;
    }

    @Override
    public int rowSize() {
        return this.schema.getColumnCount();
    }

    @Override
    public Schema getSchema() {
        return this.schema;
    }

    /**
     * A Boolean function that compares a BaseRowSnapshot to another object.
     * @param o
     * @return true if o equals this object (in terms of the contents of each field), and false
     * otherwise.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o == null) || !(o instanceof IRow)) return false;
        IRow that = (IRow) o;
        if (this.schema != that.getSchema())
            return false;
        for (String cn: this.schema.getColumnNames()) {
            if (this.isMissing(cn) && that.isMissing(cn)) {
                return true;
            } else if (this.isMissing(cn) || that.isMissing(cn)) {
                return false;
            } else {
                switch (this.schema.getKind(cn)) {
                    case String:
                    case Category:
                    case Json:
                        if (Converters.checkNull(this.getString(cn)).equals(
                                Converters.checkNull(that.getString(cn))))
                            return false;
                        break;
                    case Date:
                        if (Converters.checkNull(this.getDate(cn)).equals(
                                Converters.checkNull(that.getDate(cn))))
                            return false;
                        break;
                    case Integer:
                        if (Converters.checkNull(this.getInt(cn)) ==
                                Converters.checkNull(that.getInt(cn)))
                            return false;
                        break;
                    case Double:
                        if (Converters.checkNull(this.getDouble(cn)) ==
                                Converters.checkNull(that.getDouble(cn)))
                            return false;
                        break;
                    case Duration:
                        if (Converters.checkNull(this.getDuration(cn)).equals(
                                Converters.checkNull(that.getDuration(cn))))
                        return false;
                    break;
                }
            }
        }
        return true;
    }

    protected Object[] getData() {
        Object[] data = new Object[this.schema.getColumnCount()];
        int i = 0;
        for (final String nextCol: this.schema.getColumnNames())
            data[i++] = this.getObject(nextCol);
        return data;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (Object o : this.getData()) {
            if (!first)
                builder.append(", ");
            builder.append(o.toString());
            first = false;
        }
        return builder.toString();
    }
}