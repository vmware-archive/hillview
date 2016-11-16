package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ISchema;
import org.hiero.sketch.table.api.ISubSchema;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;

public final class Schema implements ISchema {
    /* Map a column name into an integer index */
    private final HashMap<String, Integer> index;
    private final ArrayList<ColumnDescription> columns;

    public Schema() {
        this.columns = new ArrayList<ColumnDescription>();
        this.index = new HashMap<String, Integer>();
    }

    public void append(final ColumnDescription desc) {
        if (this.index.containsKey(desc.name))
            throw new InvalidParameterException("Column with name " + desc.name + " already exists");
        this.index.put(desc.name, this.columns.size());
        this.columns.add(desc);
    }

    @Override
    public ColumnDescription getDescription(final int index) {
        return this.columns.get(index);
    }

    @Override
    public int getColumnCount() {
        return this.columns.size();
    }

    /**
     * Return the index of a column given its name.
     * @param columnName: Name of column to search.
     * @return The column index, or -1 if the column is not present.
     */
    @Override
    public int getColumnIndex(final String columnName) {
        return this.index.getOrDefault(columnName, -1);
    }

    @Override
    public ISchema project(final ISubSchema subSchema) {
        final Schema projection = new Schema();
        for (int i =0; i < this.getColumnCount(); i++) {
            final ColumnDescription colDesc = this.getDescription(i);
            if (subSchema.isColumnPresent(colDesc.name)) {
                projection.append(colDesc);
            }
        }
        return projection;
    }

    @Override
    public String toString() {
        String result = "";
        String separator = "";
        for (final ColumnDescription c : this.columns) {
            result += separator + c.toString();
            separator = ", ";
        }
        return result;
    }
}
