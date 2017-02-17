package org.hiero.sketch.table;

import javax.annotation.Nonnull;
import org.hiero.sketch.table.api.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Base class for in-memory tables.
 */
public abstract class BaseTable implements ITable {
    /**
     * Maps columns name to an IColumn.
     */
    @Nonnull
    protected final HashMap<String, IColumn> columns;

    /**
     * @return An iterator over the rows in the table.
     */
    @Override@Nonnull public IRowIterator getRowIterator() {
        return this.getMembershipSet().getIterator();
    }

    protected BaseTable(@Nonnull Iterable<IColumn> columns) {
        BaseTable.columnSize(columns);  // validate column sizes
        this.columns = new HashMap<String, IColumn>();
        for (final IColumn c : columns)
            this.columns.put(c.getName(), c);
    }

    protected BaseTable(@Nonnull Schema schema) {
        this.columns = new HashMap<String, IColumn>();
        for (final String c : schema.getColumnNames()) {
            ColumnDescription cd = schema.getDescription(c);
            this.columns.put(c, BaseArrayColumn.create(cd));
        }
    }

    @Override
    public String toString() {
        return "Table, " + this.getSchema().getColumnCount() + " columns, " +
                this.getNumOfRows() + " rows";
    }

    /**
     * Compute the size common to all these columns.
     * @param columns A set of columns.
     * @return The common size, or 0 if the set is empty.
     * Throws if the columns do not all have the same size.
     */
    protected static int columnSize(Iterable<IColumn> columns) {
        int size = -1;
        for (IColumn c : columns) {
            if (size < 0)
                size = c.sizeInRows();
            else if (size != c.sizeInRows())
                throw new IllegalArgumentException("Columns do not have the same size");
        }
        if (size < 0)
            size = 0;
        return size;
    }

    @Override public IColumn getColumn(final String colName) {
        return this.columns.get(colName);
    }

    /**
     * Compress generates a table that contains only the columns referred to by subSchema,
     * and only the rows contained in IMembership Set with consecutive numbering.
     * The order among the columns is preserved.
     */
    @Override public SmallTable compress(@Nonnull final ISubSchema subSchema,
                                         @Nonnull final IRowOrder rowOrder) {
        Schema newSchema = this.getSchema().project(subSchema);
        int width = newSchema.getColumnCount();
        List<IColumn> compressedCols = new ArrayList<IColumn>();
        for (String colName: newSchema.getColumnNames())
            compressedCols.add(this.columns.get(colName).compress(rowOrder));
        return new SmallTable(compressedCols);
    }

    /**
     * Version of Compress that defaults subSchema to the entire Schema.
     * @param rowOrder Ordered set of rows to include in the compressed table.
     * @return A compressed table containing only the rows contained in rowOrder.
     */
    @Override public SmallTable compress(final IRowOrder rowOrder) {
        final ISubSchema subSchema = new FullSubSchema();
        return this.compress(subSchema, rowOrder);
    }

    public String toLongString(int startRow, int rowsToDisplay) {
        final StringBuilder builder = new StringBuilder();
        builder.append(this.toString());
        builder.append(System.getProperty("line.separator"));
        final IRowIterator rowIt = this.getRowIterator();
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
}
