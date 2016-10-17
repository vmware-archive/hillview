package org.hiero.sketch.table;

import org.hiero.sketch.table.api.ISchema;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.HashMap;


public final class Schema implements ISchema {
    /* Map a column name into an integer index */
    private HashMap<String, Integer> index;
    private ArrayList<ColumnDescription> columns;

    public Schema() {
        this.columns = new ArrayList<ColumnDescription>();
        this.index = new HashMap<String, Integer>();
    }

    public void append(ColumnDescription desc) {
        if (this.index.containsKey(desc.name))
            throw new InvalidParameterException("Column with name " + desc.name + " already exists");
        this.columns.add(desc);
        this.index.put(desc.name, this.columns.size());
    }

    @Override
    public ColumnDescription getDescription(int index) {
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
    public int getColumnIndex(String columnName) {
        return this.index.getOrDefault(columnName, -1);
    }

    @Override
    public String toString() {
        String result = "";
        String separator = "";
        for (ColumnDescription c : this.columns) {
            result += separator + c.toString();
            separator = ", ";
        }
        return result;
    }
}
