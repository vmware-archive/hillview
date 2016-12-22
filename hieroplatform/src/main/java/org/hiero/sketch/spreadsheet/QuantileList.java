package org.hiero.sketch.spreadsheet;

import org.hiero.sketch.table.ArrayRowOrder;
import org.hiero.sketch.table.RowSnapshot;
import org.hiero.sketch.table.Table;
import org.hiero.sketch.table.api.IColumn;
import org.hiero.sketch.table.api.IRowIterator;
import org.hiero.sketch.table.api.IRowOrder;
import org.hiero.sketch.table.api.ISchema;

import java.io.Serializable;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

public class QuantileList implements Serializable {
    public final Table quantile;
    private final ApproxRank[] approxRank;
    private final int dataSize;

    /**
     * An empty quantile list for a table with the specified schema.
     */
    public QuantileList(ISchema schema) {
        this.approxRank = new ApproxRank[0];
        this.dataSize = 0;
        this.quantile = new Table(schema);
    }

    public QuantileList(final Table quantile, final ApproxRank[] approxRank, final int dataSize) {
        this.approxRank = approxRank;
        if (quantile.getNumOfRows() != approxRank.length)
            throw new InvalidParameterException("Two arguments have different lengths");
        this.quantile = quantile;
        this.dataSize = dataSize;
    }



    /**
     * @return The number of elements in the list of quantiles
     */
    public int getQuantileSize() { return this.quantile.getNumOfRows(); }

    /**
     * @return The number of input rows over which these quantiles have been computed.
     */
    public int getDataSize() { return this.dataSize; }

    public IColumn getColumn(final String colName) {
        return this.quantile.getColumn(colName);
    }

    public ISchema getSchema() {
        return this.quantile.schema;
    }

    public RowSnapshot getRow(final int rowIndex) {
        return new RowSnapshot(this.quantile, rowIndex);
    }

    //public ApproxRank getRank(final int rowIndex) { return this.approxRank[rowIndex]; }

    public int getWins(final int rowIndex) { return this.approxRank[rowIndex].wins; }

    public int getLosses(final int rowIndex) { return this.approxRank[rowIndex].losses; }

    private double getApproxRank(final int rowIndex) {
        return (((double) this.getWins(rowIndex) + this.getDataSize() - this.getLosses(rowIndex)) / 2);
    }

    /**
     * Given a RowOrder as input, compressApprox the QuantileList down to a new one containing only the
     * sequence of rows specified by the input RowOrder.
     * @param rowOrder The subset of Rows (and their ordering)
     * @return A new QuantileList
     */
    private QuantileList compress(IRowOrder rowOrder) {
        ApproxRank[] newRank = new ApproxRank[rowOrder.getSize()];
        final IRowIterator rowIt = rowOrder.getIterator();
        int row = 0;
        while (true) {
            final int i = rowIt.getNextRow();
            if (i == -1) { break; }
            newRank[row] = new ApproxRank(this.getWins(i), this.getLosses(i));
            row++;
        }
        return new QuantileList(this.quantile.compress(rowOrder), newRank, this.dataSize);
    }

    /** Given a desired size parameter, compressApprox down to nearly the desired size.
     * More precisely, we define the average gap to be the datasize/ the desired size.
     * We greedily discard an entry if the gap between the previous and next
     * entry in the quantile is less than the average gap.
     */
    public QuantileList compressApprox(int newSize) {
        int oldSize = this.getQuantileSize();
        if (oldSize <= newSize) { return this; }
        //System.out.printf("Shrinking from size %d down to %d:%n", oldSize, newSize);
        double avgGap = ((double) this.getDataSize()) / (newSize -1);
        List<Integer> newSubset = new ArrayList<>();
        newSubset.add(0);
        double open = this.getApproxRank(0);
        double close;
        for (int i = 1; i < oldSize - 1; i++) {
            close = this.getApproxRank(i+1);
            if (close - open > avgGap) {
                newSubset.add(i);
                open = this.getApproxRank(i);
            }
        }
        newSubset.add(oldSize - 1);
        //System.out.printf("Desired size %d, actual size %d:%n", newSize, newSubset.size());
        IRowOrder rowOrder = new ArrayRowOrder(newSubset);
        return this.compress(rowOrder);
    }

    public QuantileList compressExact(int newSize) {
        int oldSize = this.getQuantileSize();
        if (oldSize <= newSize) { return this; }
        List<Integer> newSubset = new ArrayList<>();
        double stepSize = ((double) this.getDataSize()) / (newSize - 1);
        newSubset.add(0);
        int j = 0;
        for (int i = 1; i < newSize - 1; i++) {
            while (this.getApproxRank(j) <= i * stepSize) {
                if (j + 2 <= oldSize) { j++; }
            }
            if (this.getApproxRank(j) + this.getApproxRank(j - 1) <= 2* i * stepSize )
                newSubset.add(j);
            else
                newSubset.add(j - 1);
        }
        newSubset.add(oldSize - 1);
        IRowOrder rowOrder = new ArrayRowOrder(newSubset);
        return this.compress(rowOrder);
    }

    public String toString() {
        final StringBuilder builder = new StringBuilder();
        final IRowIterator rowIt = this.quantile.members.getIterator();
        int nextRow = rowIt.getNextRow();
        while (nextRow != -1) {
            for (final String colName: this.quantile.schema.getColumnNames()) {
                builder.append(this.getColumn(colName).asString(nextRow));
                builder.append(", ");
            }
            builder.append("Approx Rank: ").append(this.getApproxRank(nextRow));
            builder.append(System.lineSeparator());
            nextRow = rowIt.getNextRow();
        }
        return builder.toString();
    }
}