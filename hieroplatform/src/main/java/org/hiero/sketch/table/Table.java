package org.hiero.sketch.table;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.table.api.*;

import java.util.HashMap;

/**
 * This is a simple table held entirely in RAM.
 */
public class Table {
    @NonNull
    public final ISchema schema;

    /**
     * Maps columns name to an IColumn.
     */
    @NonNull
    private final HashMap<String, IColumn> columns;

    @NonNull
    public final IMembershipSet members;

    /**
     * Create an empty table with the specified schema.
     * @param schema schema of the empty table
     */
    public Table(@NonNull final ISchema schema) {
        this.schema = schema;
        this.columns = new HashMap<String, IColumn>();
        this.members = new FullMembership(0);
        for (final String c : schema.getColumnNames()) {
            ColumnDescription cd = schema.getDescription(c);
            this.columns.put(c, BaseArrayColumn.create(cd));
        }
    }

    public Table(@NonNull final ISchema schema,
                 @NonNull final IColumn[] columns,
                 @NonNull final IMembershipSet members) {
        this.schema = schema;
        this.columns = new HashMap<String, IColumn>();
        this.members = members;
        for (final IColumn c : columns) {
            this.columns.put(c.getName(), c);
        }
    }

    public Table(@NonNull final IColumn[] columns, @NonNull final IMembershipSet members) {
        final Schema s = new Schema();
        for (final IColumn c : columns)
            s.append(c.getDescription());
        this.schema = s;
        this.columns = new HashMap<>();
        this.members = members;
        for (final IColumn c : columns) {
            this.columns.put(c.getName(), c);
        }
    }

    /**
     * Compress generates a table that contains only the columns referred to by subSchema,
     * and only the rows contained in IMembership Set with consecutive numbering.
     * The order among the columns is preserved.
     */
    public Table compress(@NonNull final ISubSchema subSchema,
                          @NonNull final IRowOrder rowOrder) {
        final ISchema newSchema = this.schema.project(subSchema);
        final int width = newSchema.getColumnCount();
        final IColumn[] compressedCols = new IColumn[width];
        int i = 0;
        for (String colName: newSchema.getColumnNames()) {
            compressedCols[i] = this.columns.get(colName).compress(rowOrder);
            i++;
        }
        final IMembershipSet full = new FullMembership(rowOrder.getSize());
        return new Table(newSchema, compressedCols, full);
    }

    /** Version of Compress that defaults subSchema to the entire Schema.
     * @param rowOrder Ordered set of rows to include in the compressed table.
     * @return A compressed table containing only the rows contained in rowOrder.
     */
    public Table compress(final IRowOrder rowOrder) {
        final ISubSchema subSchema = new FullSubSchema();
        return compress(subSchema, rowOrder);
    }

    /**
     * Generates a table that contains all the columns, and only
     * the rows contained in IMembership Set members with consecutive numbering.
     */
    public Table compress() {
        final ISubSchema subSchema = new FullSubSchema();
        return compress(subSchema, this.members);
    }

    @Override
    public String toString() {
        return "Table, " + this.schema.getColumnCount() + " columns, " +
                this.members.getSize() + " rows" +
                System.getProperty("line.separator");
    }

    public String toLongString(int startRow, int rowsToDisplay) {
        final StringBuilder builder = new StringBuilder();
        builder.append(this.toString());
        builder.append(System.getProperty("line.separator"));
        final IRowIterator rowIt = this.members.getIterator();
        int nextRow = rowIt.getNextRow();
        while ((nextRow != startRow) && (nextRow != -1))
            nextRow = rowIt.getNextRow();
        if(nextRow == -1) {
            builder.append("Start row not found!%n");
            return builder.toString();
        }
        int count = 0;
        while ((nextRow != -1) && (count < rowsToDisplay)) {
            RowSnapshot rs = new RowSnapshot(this, nextRow);
            builder.append(rs.toString());
            builder.append(System.getProperty("line.separator"));
            nextRow = rowIt.getNextRow();
            count++;
        }
        return builder.toString();
    }

    public String toLongString(int rowsToDisplay) {
        final StringBuilder builder = new StringBuilder();
        builder.append(this.toString());
        builder.append(System.getProperty("line.separator"));
        final IRowIterator rowIt = this.members.getIterator();
        int nextRow = rowIt.getNextRow();
        int count = 0;
        while ((nextRow != -1) && (count < rowsToDisplay)) {
            RowSnapshot rs = new RowSnapshot(this, nextRow);
            builder.append(rs.toString());
            builder.append(System.getProperty("line.separator"));
            nextRow = rowIt.getNextRow();
            count++;
        }
        return builder.toString();
    }

    public IColumn getColumn(final String colName) {
        return this.columns.get(colName);
    }

    public int getNumOfRows() {
        return this.members.getSize();
    }
}
