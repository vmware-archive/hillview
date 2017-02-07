package org.hiero.sketch.table;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.api.IJson;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.ISubSchema;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.*;

/**
 * A schema is just a map from a column name
 * to a column description.  Column names are case-sensitive.
 */
public final class Schema
        implements Serializable, IJson {
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

    public ColumnDescription getDescription(@NonNull final String columnName) {
        return this.columns.get(columnName);
    }

    public int getColumnCount() {
        return this.columns.size();
    }

    @NotNull
    @NonNull
    public Set<String> getColumnNames() {
        return this.columns.keySet();
    }

    @NonNull
    public List<String> getSortedColumnNames() {
        ArrayList<String> keys = new ArrayList<String>(this.getColumnNames());
        Collections.sort(keys);
        return keys;
    }

    /**
     * Generates a new Schema that contains only the subset of columns contained in the subSchema.
     */
    @NonNull
    public Schema project(@NonNull final ISubSchema subSchema) {
        final Schema projection = new Schema();
        for (String colName : this.getColumnNames()) {
            if (subSchema.isColumnPresent(colName)) {
                projection.append(this.getDescription(colName));
            }
        }
        return projection;
    }

    @Override
    @NonNull
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

    @NonNull
    public ContentsKind getKind(final String colName){
        return this.getDescription(colName).kind;
    }

    // The columns will always be sorted alphabetically
    @NonNull
    private ColumnDescription[] toArray() {
        ColumnDescription[] all = new ColumnDescription[this.columns.size()];
        int i = 0;
        for (String name: this.getSortedColumnNames()) {
            ColumnDescription cd = this.getDescription(name);
            all[i++] = cd;
        }
        return all;
    }

    @Override
    @NonNull
    public JsonElement toJsonTree() {
        ColumnDescription[] all = this.toArray();
        JsonArray result = new JsonArray();
        for (ColumnDescription cd : all)
            result.add(cd.toJsonTree());
        return result;
    }
}
