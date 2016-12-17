package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.table.RowSnapshot;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.ISchema;

import java.io.Serializable;
import java.security.InvalidParameterException;

public class QuantileList implements Serializable {
    public final Table quantile;
    private final ApproxRank[] approxRank;
    private final int dataSize;

    public QuantileList(final Table quantile, final ApproxRank[] approxRank, final int dataSize) {
        this.approxRank = approxRank;
        if (quantile.getNumOfRows() != approxRank.length)
            throw new InvalidParameterException("Two arguments have different lengths");
        this.quantile = quantile;
        this.dataSize = dataSize;
    }

    /**
     * @return The number of elements in the list of quanitles
     */
    public int getQuantileSize() { return this.quantile.getNumOfRows(); }

    /**
     * @return The number of input rows over which these quantiles have been computed.
     */
    public int getDataSize() { return this.dataSize; }

    private int getColumnIndex(final String colName) {
        return this.quantile.schema.getColumnIndex(colName);
    }

    public IColumn getColumn(final int index) {
        return this.quantile.getColumn(index);
    }

    public IColumn getColumn(final String colName) {
        return this.quantile.getColumn(this.getColumnIndex(colName));
    }

    public ISchema getSchema() {
        return this.quantile.schema;
    }

    public RowSnapshot getRow(final int rowIndex) {
        return new RowSnapshot(this.quantile, rowIndex);
    }

    //public ApproxRank getRank(final int rowIndex) { return this.approxRank[rowIndex]; }

    public int getLowerRank(final int rowIndex) { return this.approxRank[rowIndex].lower; }

    public int getUpperRank(final int rowIndex) { return this.approxRank[rowIndex].upper; }

    public String toString() {
        final StringBuilder builder = new StringBuilder();
        final IRowIterator rowIt = this.quantile.members.getIterator();
        int nextRow = rowIt.getNextRow();
        while (nextRow != -1) {
            for (final IColumn col: this.quantile.columns) {
                builder.append(col.asString(nextRow));
                builder.append(", ");
            }
            builder.append("Rank: (").append(this.getLowerRank(nextRow));
            builder.append(", ").append(this.getUpperRank(nextRow)).append(")");
            builder.append(System.lineSeparator());
            nextRow = rowIt.getNextRow();
        }
        return builder.toString();
    }
}

