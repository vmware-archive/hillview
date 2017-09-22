package org.hillview.sketches;

import org.hillview.table.ArrayRowOrder;
import org.hillview.table.RowSnapshot;
import org.hillview.table.SmallTable;

import java.io.Serializable;

/**
 * A sample of rows from a large table, stored in a small table. The expectation is that the rows
 * are sorted according to some order (this is needed for the getRow method to be meaningful).
 */
public class SampleList implements Serializable {
    /**
     * The table containing the rows.
     */
    public final SmallTable table;

    public SampleList(SmallTable table) {
        this.table = table;
    }

    /**
     * @param q in (0,1), which is the desired quantile.
     * @return Assuming the rows are sorted, this method returns the empirical p^th quantile as an
     * estimator for the p^th quantile in the large table.
     */
    public RowSnapshot getRow(double q) {
        return new RowSnapshot(this.table, (int) (q*this.table.getNumOfRows()));
    }

    /** A method that can be used in testing to estimate the quality of the quantiles test.
     * @param resolution The desired number of rows.
     * @return Equally spaced rows from the sample table.
     */
    public SmallTable getQuantiles(int resolution) {
        if (this.table.getNumOfRows() < (resolution + 1))
            return this.table;
        else {
            int[] order = new int[resolution];
            for (int i = 0; i < resolution; i++) {
                order[i] = Math.round((((i + 1) * this.table.getNumOfRows()) / (resolution + 1)) - 1);
            }
            return this.table.compress(new ArrayRowOrder(order));
        }
    }
}
