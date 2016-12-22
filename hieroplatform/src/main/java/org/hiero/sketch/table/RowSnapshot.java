package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IRow;
import org.hiero.sketch.table.api.ISchema;

import java.util.ArrayList;
import java.util.List;

public class RowSnapshot implements IRow {

    @NonNull
    private final ISchema schema;
    @NonNull
    private final List<Object> field = new ArrayList<Object>();

    public RowSnapshot(final Table data, final int rowIndex) {
        this.schema = data.schema;
        for(final IColumn col: data.columns) {
            final int i = this.schema.getColumnIndex(col.getName());
            this.field.add(i, col.getObject(rowIndex));
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
    public Object get(final int colIndex) {
        return this.field.get(colIndex);
    }
}
