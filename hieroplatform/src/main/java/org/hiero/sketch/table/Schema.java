package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.ISchema;
import org.hiero.sketch.table.api.ISubSchema;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Set;

/**
 * A schema is just a map from a column name
 * to a column description.  Column names are case-sensitive.
 */
public final class Schema implements ISchema {
    /* Map a column name into an integer index */
    @NonNull
    private final HashMap<String, ColumnDescription> columns;

    public Schema() {
        this.columns = new HashMap<String, ColumnDescription>();
    }

    public void append(@NonNull final ColumnDescription desc) {
        if (this.columns.containsKey(desc.name))
            throw new InvalidParameterException("Column with name " + desc.name + " already exists");
        this.columns.put(desc.name, desc);
    }

    @Override
    public ColumnDescription getDescription(@NonNull final String columnName) {
        return this.columns.get(columnName);
    }

    @Override
    public int getColumnCount() {
        return this.columns.size();
    }

    @Override
    public Set<String> getColumnNames() {
        return this.columns.keySet();
    }

    /**
     * Generates a new Schema that contains only the subset of columns contained in the subSchema.
     */
    @Override
    public ISchema project(@NonNull final ISubSchema subSchema) {
        final Schema projection = new Schema();
        for (String colName : this.getColumnNames()) {
            if (subSchema.isColumnPresent(colName)) {
                projection.append(this.getDescription(colName));
            }
        }
        return projection;
    }

    @Override
    public String toString() {
        String result = "";
        String separator = "";
        for (final ColumnDescription c : this.columns.values()) {
            result += separator + c.toString();
            separator = ", ";
        }
        return result;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if ((o == null) || (getClass() != o.getClass())) return false;

        final Schema schema = (Schema) o;
        return this.columns.equals(schema.columns);
    }

    @Override
    public int hashCode() {
        return this.columns.hashCode();
    }
}
