package org.hiero.sketches;

import org.hiero.table.ArrayRowOrder;
import org.hiero.table.RowSnapshot;
import org.hiero.table.SmallTable;

/**
 * A sample of rows from a large table, stored in a small table, sorted according to some order.
 */
public class SampleList {
    /**
     * The table containing the rows.
     */
    public SmallTable table;

    public SampleList(SmallTable table) {
        this.table = table;
    }

    /**
     * Returns the empirical p^th quantile
     * @param p
     * @return The p^th row in the sample
     */
    public RowSnapshot getRow(double p) {
        return new RowSnapshot(table, (int) (p*this.table.getNumOfRows()));
    }

    /** A method that can be sued in testing to estimate the quality of the quantiles sketch.
     * It returns  a specified number of equally spaced rows from the sample table.
     * @param resolution The number of rows.
     * @return
     */
    public SmallTable getQuantiles(int resolution) {
        if (this.table.getNumOfRows() < resolution + 1)
            return this.table;
        else {
            int[] order = new int[resolution];
            for (int i = 0; i < resolution; i++) {
                order[i] = Math.round((i + 1) * table.getNumOfRows() / (resolution + 1) - 1);
            }
            return this.table.compress(new ArrayRowOrder(order));
        }
    }


}
