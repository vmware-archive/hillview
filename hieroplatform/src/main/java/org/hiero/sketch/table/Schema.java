package org.hiero.sketch.table;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.hiero.sketch.dataset.api.IJson;
import org.hiero.sketch.table.api.ContentsKind;
import org.hiero.sketch.table.api.ISubSchema;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.*;

/**
 * A schema is an ordering of the columns, plus a map from a column name to a column description.
 * Column names are case-sensitive.
 */
public final class Schema
        implements Serializable, IJson {
    private final HashMap<String, ColumnDescription> columns;
    private final List<String> colOrder;

    public Schema() {
        this.columns = new HashMap<String, ColumnDescription>();
        this.colOrder = new ArrayList<String>();
    }

    public void append(final ColumnDescription desc) {
        if (this.columns.containsKey(desc.name))
            throw new InvalidParameterException("Column with name " + desc.name + " already exists");
        this.columns.put(desc.name, desc);
        this.colOrder.add(desc.name);
    }

    public ColumnDescription getDescription(final String columnName) {
        return this.columns.get(columnName);
    }

    public int getColumnCount() {
        return this.columns.size();
    }

    public List<String> getColumnNames() {
        return this.colOrder;
    }

    /**
     * Generates a new Schema that contains only the subset of columns contained in the subSchema.
     */
    public Schema project(final ISubSchema subSchema) {
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

    public ContentsKind getKind(final String colName){
        return this.getDescription(colName).kind;
    }

    // The columns will be ordered as in colOrder
    private ColumnDescription[] toArray() {
        ColumnDescription[] all = new ColumnDescription[this.columns.size()];
        int i = 0;
        for (String name: this.getColumnNames()) {
            ColumnDescription cd = this.getDescription(name);
            all[i++] = cd;
        }
        return all;
    }

    @Override
    public JsonElement toJsonTree() {
        ColumnDescription[] all = this.toArray();
        JsonArray result = new JsonArray();
        for (ColumnDescription cd : all)
            result.add(cd.toJsonTree());
        return result;
    }
}
