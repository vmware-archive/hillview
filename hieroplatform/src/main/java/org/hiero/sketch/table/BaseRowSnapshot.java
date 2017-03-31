package org.hiero.sketch.table;

import org.hiero.sketch.table.api.IRow;
import org.hiero.utils.Converters;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o == null) || !(o instanceof IRow)) return false;
        IRow that = (IRow) o;
        if(this.schema != that.getSchema())
            return false;
        for (String cn: this.schema.getColumnNames()) {
            if (this.isMissing(cn) && that.isMissing(cn)) {
                return true;
            } else if (this.isMissing(cn) || that.isMissing(cn)) {
                return false;
            } else {
                switch (this.schema.getKind(cn)) {
                    case String:
                    case Json:
                        if (Converters.checkNull(this.getString(cn)).compareTo(
                                Converters.checkNull(that.getString(cn))) != 0)
                            return false;
                        break;
                    case Date:
                        if (Converters.checkNull(this.getDate(cn)).compareTo(
                                Converters.checkNull(that.getDate(cn))) != 0)
                            return false;
                        break;
                    case Integer:
                        if (Converters.checkNull(this.getInt(cn)).compareTo(
                                Converters.checkNull(that.getInt(cn))) != 0)
                            return false;
                        break;
                    case Double:
                        if (Converters.checkNull(this.getDouble(cn)).compareTo(
                                Converters.checkNull(that.getDouble(cn))) != 0)
                            return false;
                        break;
                    case Duration:
                        if (Converters.checkNull(this.getDuration(cn)).compareTo(
                                Converters.checkNull(that.getDuration(cn))) != 0)
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