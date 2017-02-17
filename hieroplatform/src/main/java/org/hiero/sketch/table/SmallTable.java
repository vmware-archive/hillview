package org.hiero.sketch.table;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.hiero.sketch.dataset.api.IJson;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IMembershipSet;
import org.hiero.sketch.table.api.IRowIterator;

import java.io.Serializable;

/**
 * A SmallTable is similar to a Table, but it is intended to be shipped over the network.
 * We expect all columns to be serializable.
 */
public class SmallTable
        extends BaseTable
        implements Serializable, IJson {

    protected final Schema schema;
    protected final int rowCount;

    @Override
    public Schema getSchema() {
        return this.schema;
    }

    public SmallTable( final Iterable<IColumn> cols) {
        super(cols);
        this.rowCount = BaseTable.columnSize(this.columns.values());
        final Schema s = new Schema();
        for (final IColumn c : cols) {
            s.append(c.getDescription());
            if (!(c instanceof Serializable))
                throw new RuntimeException("Column for SmallTable is not serializable");
        }
        this.schema = s;
    }

    public SmallTable( final Schema schema) {
        super(schema);
        this.schema = schema;
        this.rowCount = 0;
    }

    @Override
    public IRowIterator getRowIterator() {
        return new FullMembership.FullMembershipIterator(this.rowCount);
    }

    @Override
    public  IMembershipSet getMembershipSet() {
        return new FullMembership(this.rowCount);
    }

    @Override
    public int getNumOfRows() {
        return this.rowCount;
    }

    private RowSnapshot[] getRows() {
        RowSnapshot[] rows = new RowSnapshot[this.getNumOfRows()];
        for (int i = 0; i < this.getNumOfRows(); i++)
            rows[i] = new RowSnapshot(this, i);
        return rows;
    }

    @Override
    public JsonElement toJsonTree() {
        JsonObject result = new JsonObject();
        result.add("schema", this.schema.toJsonTree());
        result.addProperty("rowCount", this.rowCount);
        RowSnapshot[] rows = this.getRows();
        JsonArray jrows = new JsonArray();
        for (RowSnapshot rs : rows)
            jrows.add(rs.toJsonTree());
        result.add("rows", jrows);
        return result;
    }
}
