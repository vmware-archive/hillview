package org.hiero.sketch.spreadsheet;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.hiero.sketch.table.RowSnapshot;
import org.hiero.sketch.table.Schema;
import org.hiero.sketch.table.SmallTable;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.dataset.api.IJson;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The data structure used to store the next K rows in a Table from a given starting point (topRow)
 * according to a RecordSortOrder.
 */
public class NextKList implements Serializable, IJson {
    public final SmallTable table;
    /**
     * The number of times each row in the above table occurs in the original DataSet.
     */
    public final List<Integer> count;
    /**
     * The row number of the starting point (topRow)
     */
    public final long startPosition;
    /**
     * Total rows in the original table over which this is computed.
     */
    public final long totalRows;

    public NextKList(SmallTable table, List<Integer> count, long position, long totalRows) {
        this.table = table;
        this.count = count;
        this.startPosition = position;
        this.totalRows = totalRows;
        /* If the table is empty, discard the counts. Else check we have counts for each row.*/
        if((table.getNumOfRows() !=0) && (count.size() != table.getNumOfRows()))
            throw new IllegalArgumentException("Mismatched table and count length");
    }

    /**
     * A NextK list containing an empty table with the specified schema.
     */
    public NextKList(Schema schema) {
        this.table = new SmallTable(schema);
        this.count = new ArrayList<Integer>(0);
        this.startPosition = 0;
        this.totalRows = 0;
    }


    public String toLongString(int rowsToDisplay) {
        final StringBuilder builder = new StringBuilder();
        builder.append(this.toString());
        builder.append(System.getProperty("line.separator"));
        final IRowIterator rowIt = this.table.getRowIterator();
        int nextRow = rowIt.getNextRow();
        int i = 0;
        while ((nextRow != -1) && (i < rowsToDisplay)) {
            RowSnapshot rs = new RowSnapshot(this.table, nextRow);
            builder.append(rs.toString()).append(": ").append(this.count.get(i));
            builder.append(System.getProperty("line.separator"));
            nextRow = rowIt.getNextRow();
            i++;
        }
        return builder.toString();
    }


    @Override
    public JsonElement toJsonTree() {
        JsonObject result = new JsonObject();
        result.add("schema", this.table.getSchema().toJsonTree());
        result.addProperty("rowCount", this.totalRows);
        result.addProperty("startPosition", this.startPosition);
        JsonArray rows = new JsonArray();
        result.add("rows", rows);
        for (int i = 0; i < this.count.size(); i++) {
            JsonObject row = new JsonObject();
            row.addProperty("count", this.count.get(i));
            row.add("values", new RowSnapshot(this.table, i).toJsonTree());
            rows.add(row);
        }
        return result;
    }
}
