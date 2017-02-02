package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.table.RowSnapshot;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.ISchema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The data structure used to store the next K rows in a Table from a given starting point (topRow)
 * according to a RecordSortOrder.
 */
public class NextKList implements Serializable {
    public final Table table;
    /**
     * The number of times each row in the above table occurs in the original DataSet.
     */
    public final List<Integer> count;
    /**
     * The row number of the starting point (topRow)
     */
    private final long position;

    public NextKList(Table table, List<Integer> count, int position, RowSnapshot topRow) {
        this.table = table;
        this.count = count;
        this.position = position;
    }
    /**
     * A NextK list containing an empty table with the specified schema.
     */
    public NextKList(ISchema schema) {
        this.table = new Table(schema);
        this.count = new ArrayList<Integer>(0);
        this.position = 0;
    }

    public String toLongString(int rowsToDisplay) {
        final StringBuilder builder = new StringBuilder();
        builder.append(this.toString());
        builder.append(System.getProperty("line.separator"));
        final IRowIterator rowIt = this.table.members.getIterator();
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
}
