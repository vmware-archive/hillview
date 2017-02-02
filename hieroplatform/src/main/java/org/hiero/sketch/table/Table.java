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

    public Table(@NonNull final IColumn[] columns, @NonNull final IMembershipSet members) {
        columnSize(columns);  // validate column sizes
        final Schema s = new Schema();
        for (final IColumn c : columns)
            s.append(c.getDescription());
        this.schema = s;
        this.columns = new HashMap<String, IColumn>();
        this.members = members;
        for (final IColumn c : columns) {
            this.columns.put(c.getName(), c);
        }
    }

    public Table(@NonNull final IColumn[] columns) {
        this(columns, new FullMembership(columnSize(columns)));
    }

    /**
     * Compute the size common to all these columns.
     * @param columns A set of columns.
     * @return The common size, or 0 if the set is empty.
     * Throws if the columns do not all have the same size.
     */
    private static int columnSize(IColumn[] columns) {
        if (columns.length == 0)
            return 0;
        int size = -1;
        for (IColumn c : columns) {
            if (size < 0)
                size = c.sizeInRows();
            else if (size != c.sizeInRows())
                throw new IllegalArgumentException("Columns do not have the same size");
        }
        return size;
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
        return new Table(compressedCols, full);
    }

    /**
     * Version of Compress that defaults subSchema to the entire Schema.
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
                this.members.getSize() + " rows";
    }

    public String toLongString(int startRow, int rowsToDisplay) {
        final StringBuilder builder = new StringBuilder();
        builder.append(this.toString());
        builder.append(System.getProperty("line.separator"));
        final IRowIterator rowIt = this.members.getIterator();
        int nextRow = rowIt.getNextRow();
        while ((nextRow < startRow) && (nextRow != -1))
            nextRow = rowIt.getNextRow();
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
        return this.toLongString(0, rowsToDisplay);
    }

    public IColumn getColumn(final String colName) {
        return this.columns.get(colName);
    }

    public int getNumOfRows() {
        return this.members.getSize();
    }

    /**
     * Can be used for testing.
     * @return A small table with some interesting contents.
     */
    public static Table testTable() {
        ColumnDescription c0 = new ColumnDescription("Name", ContentsKind.String, false);
        ColumnDescription c1 = new ColumnDescription("Age", ContentsKind.Int, false);
        StringArrayColumn sac = new StringArrayColumn(c0, new String[] { "Mike", "John", "Tom"});
        IntArrayColumn iac = new IntArrayColumn(c1, new int[] { 20, 30, 10 });

        Table table = new Table(new IColumn[] { sac, iac });
        return table;
    }
}
