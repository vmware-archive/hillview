package org.hiero.sketch.table;

import com.google.gson.JsonElement;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.api.IJson;
import org.hiero.sketch.table.api.IRow;
import org.hiero.sketch.table.api.ITable;

import java.io.Serializable;
import java.util.HashMap;

/**
 * The copy of the data in a row of the table.
 * This is quite inefficient, it should be used rarely.
 */
public class RowSnapshot implements IRow, Serializable, IJson {
    @NonNull
    protected final Schema schema;

    /**
     * Maps a column name to a value.
     */
    @NonNull
    private final HashMap<String, Object> field = new HashMap<String, Object>();

    public RowSnapshot(@NonNull final ITable data, final int rowIndex) {
        this.schema = data.getSchema();
        for (final String colName : this.schema.getColumnNames())
            this.field.put(colName, data.getColumn(colName).getObject(rowIndex));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;

        RowSnapshot that = (RowSnapshot) o;
        return this.schema.equals(that.schema) && this.field.equals(that.field);
    }

    @Override
    public int hashCode() {
        int result = this.schema.hashCode();
        result = (31 * result) + this.field.hashCode();
        return result;
    }

    @Override
    public int rowSize() {
        return this.field.size();
    }

    @Override
    public Schema getSchema() {
        return this.schema;
    }

    @Override
    public Object get(@NonNull String colName) {
        return this.field.get(colName);
    }

    private Object[] getData() {
        Object[] data = new Object[this.schema.getColumnCount()];
        int i = 0;
        for (final String nextCol: this.schema.getSortedColumnNames())
            data[i++] = this.get(nextCol);
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

    @Override
    public JsonElement toJsonTree() {
        Object[] data = this.getData();
        return gsonInstance.toJsonTree(data);
    }
}
