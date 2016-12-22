package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.IRow;
import org.hiero.sketch.table.api.ISchema;

import java.util.HashMap;

/**
 * The copy of the data in a row of the table.
 * This is quite inefficient, it should be used rarely.
 */
public class RowSnapshot implements IRow {
    @NonNull
    private final ISchema schema;
    @NonNull
    private final HashMap<String, Object> field = new HashMap<String, Object>();

    public RowSnapshot(@NonNull final Table data, final int rowIndex) {
        this.schema = data.schema;
        for(final String colName : this.schema.getColumnNames()) {
            this.field.put(colName, data.getColumn(colName).getObject(rowIndex));
        }
    }

    @Override
    public int rowSize() {
        return this.field.size();
    }

    @Override
    public ISchema getSchema() {
        return this.schema;
    }

    @Override
    public Object get(@NonNull String colName) {
        return this.field.get(colName);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (final String nextCol: this.schema.getColumnNames()) {
            if (!first)
                builder.append(", ");
            builder.append(this.field.get(nextCol).toString());
            first = false;
        }
        return builder.toString();
    }
}
